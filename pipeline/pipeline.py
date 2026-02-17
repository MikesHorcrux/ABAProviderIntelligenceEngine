from __future__ import annotations

from pathlib import Path
from typing import Iterable

import sqlite3

from pipeline.config import CrawlConfig, load_crawl_config
from pipeline.db import connect_db
from pipeline.observability import Metrics, build_logger, log_stage_end, log_stage_start
from pipeline.stages.discovery import DiscoverySeed, dedupe_seeds, load_seeds
from pipeline.stages.fetch import FetchResult, run_fetch
from pipeline.stages.parse import ParsedPage, dedupe_signals, parse_page
from pipeline.stages.resolve import ResolvedLocation, resolve_and_upsert_locations
from pipeline.stages.enrich import run_waterfall_enrichment
from pipeline.stages.score import run_score
from pipeline.stages.export import export_outreach, export_research_queue, export_merge_suggestions
from pipeline.quality import run_quality_report
from pipeline.utils import make_pk, normalize_domain, normalize_url, utcnow_iso


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/cannaradar_v1.db"
SCHEMA_PATH = ROOT / "db/schema.sql"
OUT_DIR = ROOT / "out"


def _normalise_seed_from_job(seed_name: str | None, seed_domain: str | None) -> DiscoverySeed:
    name = (seed_name or "").strip()
    website = normalize_url(seed_domain or "")
    domain = normalize_domain(seed_domain or "")
    return DiscoverySeed(name=name or domain, website=website, state="", market="")


class PipelineRunner:
    def __init__(self, seeds: str | None = None, max_pages: int | None = None, db_path: str | Path = DB_PATH):
        self.seeds_path = seeds or str(ROOT / "seeds.csv")
        self.max_pages = max_pages
        self.db_path = Path(db_path)
        self.config = load_crawl_config()
        self.job_id = utcnow_iso().replace(":", "").replace("-", "").replace("T", "-")
        self.logger = build_logger(self.job_id, "pipeline")
        self.metrics = Metrics(self.job_id)

    def _discovery_stage(self) -> list[DiscoverySeed]:
        batch = load_seeds(self.seeds_path)
        return dedupe_seeds(batch.seeds, limit=self.max_pages)

    def run_fetch(self, seeds: list[DiscoverySeed] | None = None) -> list[FetchResult]:
        seeds = seeds or self._discovery_stage()
        start = log_stage_start(self.logger, "fetch", self.job_id)
        con = connect_db(self.db_path, SCHEMA_PATH)
        fetched = run_fetch(con, seeds, self.config, self.logger, self.metrics, self.job_id)
        log_stage_end(self.logger, "fetch", self.job_id, start, self.metrics.snapshot())
        con.close()
        return fetched

    def _load_results_for_enrichment(self, since: str | None = None) -> list[FetchResult]:
        con = connect_db(self.db_path, SCHEMA_PATH)
        if since:
            rows = con.execute(
                """
                SELECT cj.seed_name, cj.seed_domain, cr.crawl_job_pk, cr.target_url, cr.status_code,
                       cr.content_hash, cr.content, cr.fetched_at
                FROM crawl_results cr
                INNER JOIN crawl_jobs cj ON cj.crawl_job_pk = cr.crawl_job_pk
                WHERE cr.status_code = 200
                  AND cr.fetched_at >= ?
                  AND cr.content <> ''
                ORDER BY cr.fetched_at ASC
                """,
                (since,),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT cj.seed_name, cj.seed_domain, cr.crawl_job_pk, cr.target_url, cr.status_code,
                       cr.content_hash, cr.content, cr.fetched_at
                FROM crawl_results cr
                INNER JOIN crawl_jobs cj ON cj.crawl_job_pk = cr.crawl_job_pk
                WHERE cr.status_code = 200
                  AND cr.content <> ''
                ORDER BY cr.fetched_at ASC
                """,
            ).fetchall()

        con.close()
        output: list[FetchResult] = []
        for row in rows:
            seed = _normalise_seed_from_job(row["seed_name"], row["seed_domain"])
            output.append(
                FetchResult(
                    job_pk=row["crawl_job_pk"],
                    seed_name=seed.name,
                    seed_state=seed.state,
                    seed_market=seed.market,
                    seed_website=seed.website,
                    target_url=row["target_url"],
                    normalized_url=row["target_url"],
                    status_code=row["status_code"],
                    content=row["content"],
                    content_hash=row["content_hash"],
                    fetched_at=row["fetched_at"],
                )
            )
        return output

    def run_enrich(self, fetched: list[FetchResult] | None = None, since: str | None = None) -> list[str]:
        start = log_stage_start(self.logger, "enrich", self.job_id)
        con = connect_db(self.db_path, SCHEMA_PATH)
        fetched_rows = list(fetched or self._load_results_for_enrichment(since=since))
        enriched_locations: list[str] = []

        for item in fetched_rows:
            parsed = parse_page(item.target_url, item.content)
            self.metrics.inc("parse_success")
            self.metrics.inc("contacts_found", len(parsed.contact_people))
            seed = DiscoverySeed(
                name=item.seed_name,
                website=item.seed_website,
                state=item.seed_state,
                market=item.seed_market,
            )

            resolved = resolve_and_upsert_locations(con, seed, [parsed])
            now = utcnow_iso()

            for sig in dedupe_signals(parsed.emails):
                con.execute(
                    """
                    INSERT OR REPLACE INTO contact_points
                    (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at, created_at, updated_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("cp", [resolved.location_pk, sig.field_name, sig.value]),
                        resolved.location_pk,
                        sig.field_name,
                        sig.value,
                        sig.confidence,
                        sig.url,
                        now,
                        now,
                        now,
                        now,
                    ),
                )
                con.execute(
                    """
                    INSERT OR REPLACE INTO evidence
                    (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("ev", [resolved.location_pk, sig.field_name, sig.value]),
                        "location",
                        resolved.location_pk,
                        sig.field_name,
                        sig.value,
                        sig.url,
                        sig.snippet,
                        now,
                    ),
                )

            for sig in dedupe_signals(parsed.phones):
                con.execute(
                    """
                    INSERT OR REPLACE INTO contact_points
                    (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at, created_at, updated_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("cp", [resolved.location_pk, sig.field_name, sig.value]),
                        resolved.location_pk,
                        sig.field_name,
                        sig.value,
                        sig.confidence,
                        sig.url,
                        now,
                        now,
                        now,
                        now,
                    ),
                )
                con.execute(
                    """
                    INSERT OR REPLACE INTO evidence
                    (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("ev", [resolved.location_pk, sig.field_name, sig.value]),
                        "location",
                        resolved.location_pk,
                        sig.field_name,
                        sig.value,
                        sig.url,
                        sig.snippet,
                        now,
                    ),
                )

            for person_name, person_role, snippet in parsed.contact_people:
                con.execute(
                    """
                    INSERT OR REPLACE INTO contacts
                    (contact_pk, location_pk, full_name, role, email, phone, source_kind, confidence, verification_status, created_at, updated_at, last_seen_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("c", [resolved.location_pk, person_name, person_role]),
                        resolved.location_pk,
                        person_name,
                        person_role,
                        "",
                        "",
                        "first_party_parse",
                        0.7,
                        "unverified",
                        now,
                        now,
                        now,
                    ),
                )
                con.execute(
                    """
                    INSERT OR REPLACE INTO evidence
                    (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("ev", [resolved.location_pk, "contact", person_name]),
                        "location",
                        resolved.location_pk,
                        "contact",
                        f"{person_name} ({person_role})",
                        item.target_url,
                        snippet,
                        now,
                    ),
                )

            if parsed.menu_providers:
                for provider in parsed.menu_providers:
                    con.execute(
                        """
                        INSERT OR REPLACE INTO evidence
                        (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                        VALUES (?,?,?,?,?,?,?,?,'')
                        """,
                        (
                            make_pk("ev", [resolved.location_pk, "menu_provider", provider]),
                            "location",
                            resolved.location_pk,
                            "menu_provider",
                            provider,
                            item.target_url,
                            "menu provider detected from page",
                        now,
                    ),
                )

            for social_url in parsed.social_urls[:3]:
                con.execute(
                    """
                    INSERT OR REPLACE INTO evidence
                    (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("ev", [resolved.location_pk, "social_url", social_url]),
                        "location",
                        resolved.location_pk,
                        "social_url",
                        social_url,
                        social_url,
                        "social signal",
                        now,
                    ),
                )

            if parsed.schema_local_business:
                con.execute(
                    """
                    INSERT OR REPLACE INTO evidence
                    (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,'')
                    """,
                    (
                        make_pk("ev", [resolved.location_pk, "schema_org"]),
                        "location",
                        resolved.location_pk,
                        "schema_org",
                        "present",
                        item.target_url,
                        "schema.org local business signal",
                        now,
                    ),
                )

            con.execute(
                """
                UPDATE locations
                SET website_domain=COALESCE(NULLIF(?, ''), website_domain),
                    canonical_name=COALESCE(NULLIF(?, ''), canonical_name),
                    state=COALESCE(NULLIF(?, ''), state),
                    last_crawled_at=?,
                    last_seen_at=?,
                    updated_at=?
                WHERE location_pk=?
                """,
                (
                    normalize_domain(item.seed_website),
                    seed.name,
                    seed.state,
                    now,
                    now,
                    now,
                    resolved.location_pk,
                ),
            )

            if resolved.domain:
                con.execute(
                    """
                    INSERT OR REPLACE INTO domains
                    (domain_pk, location_pk, domain, is_primary, confidence, source_url, last_seen_at, created_at, updated_at, deleted_at)
                    VALUES (?,?,?,?,?,?,?,?,?, '')
                    """,
                    (
                        make_pk("dom", [resolved.location_pk, resolved.domain]),
                        resolved.location_pk,
                        resolved.domain,
                        1,
                        0.9,
                        seed.website,
                        now,
                        now,
                        now,
                    ),
                )

            con.execute(
                """
                UPDATE crawl_jobs
                SET status='enriched', updated_at=?, last_status_code=?
                WHERE crawl_job_pk=?
                """,
                (now, item.status_code, item.job_pk),
            )

            if resolved.merge_suggestions:
                self.metrics.inc("dupes_merged", resolved.merge_suggestions)

            run_waterfall_enrichment(con, resolved.location_pk)
            self.metrics.inc("enrichment_success")
            self.metrics.inc("locations_enriched")
            enriched_locations.append(resolved.location_pk)

        if enriched_locations:
            con.execute(
                f"UPDATE locations SET updated_at=? WHERE location_pk IN ({','.join('?' for _ in enriched_locations)})",
                tuple([utcnow_iso(), *enriched_locations]),
            )

        con.commit()
        log_stage_end(self.logger, "enrich", self.job_id, start, self.metrics.snapshot())
        con.close()
        return enriched_locations

    def run_score(self) -> int:
        start = log_stage_start(self.logger, "score", self.job_id)
        con = connect_db(self.db_path, SCHEMA_PATH)
        run_score(con)
        con.close()
        log_stage_end(self.logger, "score", self.job_id, start, self.metrics.snapshot())
        return self.metrics.snapshot().get("scores_written", 0)

    def run_export(self, tier: str = "A", limit: int = 200, research_limit: int = 200) -> dict[str, object]:
        con = connect_db(self.db_path, SCHEMA_PATH)
        result = export_outreach(con, OUT_DIR, tier=tier, limit=limit, run_id=self.job_id)
        research_path = export_research_queue(con, OUT_DIR, limit=research_limit, run_id=self.job_id)
        merge_report = export_merge_suggestions(con, OUT_DIR, run_id=self.job_id)
        quality = run_quality_report(con, OUT_DIR)
        con.close()
        return {
            "outreach": result,
            "research": research_path,
            "merge_suggestions": merge_report,
            "quality": quality,
        }

    def run_quality(self) -> dict[str, object]:
        con = connect_db(self.db_path, SCHEMA_PATH)
        payload = run_quality_report(con, OUT_DIR)
        con.close()
        return payload

    def run_crawl(self, seed_limit: int | None = None) -> dict[str, object]:
        self.max_pages = seed_limit or self.max_pages
        fetched = self.run_fetch()
        self.run_enrich(fetched=fetched)
        self.run_score()
        return self.run_export(tier="A", limit=200, research_limit=200)
