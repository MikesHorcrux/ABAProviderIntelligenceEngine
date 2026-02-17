from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable
from datetime import datetime, timedelta

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
from pipeline.stages.export import (
    export_buyer_signal_queue,
    export_merge_suggestions,
    export_new_leads,
    export_outreach,
    export_research_queue,
)
from pipeline.quality import run_quality_report
from pipeline.utils import make_pk, normalize_domain, normalize_url, utcnow_iso


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data/cannaradar_v1.db"
SCHEMA_PATH = ROOT / "db/schema.sql"
OUT_DIR = ROOT / "out"
MANIFEST_PATH = ROOT / "data" / "state" / "last_run_manifest.json"


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

    def _resolve_seed_path(self, candidate: str | None) -> str | None:
        if not candidate:
            return None
        p = Path(candidate)
        if not p.is_absolute():
            p = (ROOT / p).resolve()
        if p.exists():
            return str(p)
        return None

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

    def _is_seed_in_backoff(self, con: sqlite3.Connection, seed: DiscoverySeed) -> bool:
        domain = normalize_domain(seed.website)
        if not domain:
            return False

        failure_limit = int(self.config.seed_failure_streak_limit)
        if failure_limit <= 0:
            return False
        cooldown_hours = max(0, int(self.config.seed_backoff_hours))
        if cooldown_hours <= 0:
            return False
        failure_limit = max(1, failure_limit)

        rows = con.execute(
            """
            SELECT cr.status_code, cr.fetched_at
            FROM crawl_jobs cj
            INNER JOIN crawl_results cr ON cr.crawl_job_pk = cj.crawl_job_pk
            WHERE cj.seed_domain = ?
            ORDER BY cr.fetched_at DESC
            LIMIT ?
            """,
            (domain, failure_limit),
        ).fetchall()
        if not rows:
            return False

        consecutive_failures = 0
        for row in rows:
            if int(row["status_code"] or 0) == 200:
                break
            consecutive_failures += 1

        if consecutive_failures < failure_limit:
            return False

        last_seen = self._parse_iso_datetime(rows[0]["fetched_at"])
        if not last_seen:
            return False

        now = datetime.utcnow()
        if now - last_seen < timedelta(hours=cooldown_hours):
            return True
        return False

    def _discovery_stage(self, seed_limit: int | None = None) -> list[DiscoverySeed]:
        sources: list[tuple[str, str, int]] = []
        main_path = self._resolve_seed_path(self.seeds_path)
        if main_path:
            sources.append((main_path, "seed_file", 100))
        discovery_path = self._resolve_seed_path(self.config.discovery_seed_file)
        if discovery_path:
            sources.append((discovery_path, "discovery_file", 60))

        con = connect_db(self.db_path, SCHEMA_PATH)
        items: list[DiscoverySeed] = []
        for path, source, priority in sources:
            batch = load_seeds(path, source=source, priority=priority)
            for seed in batch.seeds:
                if self._is_seed_in_backoff(con, seed):
                    self.logger.warning(
                        "Seed in cooldown; skipping for now",
                        extra={
                            "job_id": self.job_id,
                            "stage": "discovery",
                            "seed_domain": normalize_domain(seed.website),
                            "source": seed.source,
                        },
                    )
                    self.metrics.inc("seeds_skipped_backoff")
                    continue
                items.append(seed)
        con.close()
        return dedupe_seeds(items, limit=seed_limit or self.max_pages)

    def _monitoring_stage(self, stale_days: int | None, seed_limit: int | None = None) -> list[DiscoverySeed]:
        stale_days = max(0, int(stale_days or self.config.monitor_stale_days))
        modifier = f"-{stale_days} days"
        con = connect_db(self.db_path, SCHEMA_PATH)
        if stale_days <= 0:
            rows = con.execute(
                """
                SELECT canonical_name, website_domain, state, fit_score, updated_at
                FROM locations
                WHERE COALESCE(deleted_at,'')=''
                ORDER BY
                  CASE WHEN COALESCE(last_crawled_at, '') = '' THEN 0 ELSE 1 END,
                  last_crawled_at ASC,
                  fit_score DESC,
                  updated_at DESC
                """,
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT canonical_name, website_domain, state, fit_score, updated_at
                FROM locations
                WHERE COALESCE(deleted_at,'')=''
                  AND (
                    last_crawled_at IS NULL
                    OR last_crawled_at = ''
                    OR date(last_crawled_at) <= date('now', ?)
                  )
                ORDER BY
                  CASE WHEN COALESCE(last_crawled_at, '') = '' THEN 0 ELSE 1 END,
                  last_crawled_at ASC,
                  fit_score DESC,
                  updated_at DESC
                """,
                (modifier,),
            ).fetchall()

        items: list[DiscoverySeed] = []
        for row in rows:
            domain = (row["website_domain"] or "").strip().lower()
            if not domain:
                continue
            candidate = DiscoverySeed(
                name=(row["canonical_name"] or "").strip(),
                website=normalize_url(f"https://{domain}"),
                state=(row["state"] or "").strip(),
                market="",
                source="monitor_seed",
                priority=40 + min(50, int((row["fit_score"] or 0) / 2)),
            )
            if self._is_seed_in_backoff(con, candidate):
                self.logger.warning(
                    "Monitor seed in cooldown; skipping for now",
                    extra={
                        "job_id": self.job_id,
                        "stage": "monitor",
                        "seed_domain": candidate.website,
                    },
                )
                self.metrics.inc("seeds_skipped_backoff")
                continue
            items.append(candidate)
            if seed_limit and len(items) >= seed_limit:
                break
        con.close()
        return dedupe_seeds(items)

    def _seed_signature(self, seed: DiscoverySeed) -> tuple[str, str]:
        return (seed.website, seed.state.lower())

    def _build_seed_plan(
        self,
        crawl_mode: str = "full",
        discovery_limit: int | None = None,
        monitor_limit: int | None = None,
        stale_days: int | None = None,
    ) -> tuple[list[DiscoverySeed], list[DiscoverySeed]]:
        crawl_mode = (crawl_mode or "full").lower()
        discovery_seeds = [] if crawl_mode == "monitor" else self._discovery_stage(seed_limit=discovery_limit)
        monitoring_seeds = [] if crawl_mode == "growth" else self._monitoring_stage(stale_days=stale_days, seed_limit=monitor_limit)

        seen: set[tuple[str, str]] = set()
        merged: list[DiscoverySeed] = []
        combined_monitoring: list[DiscoverySeed] = []

        if crawl_mode in {"balanced", "full", "hybrid"}:
            for seed in sorted(discovery_seeds + monitoring_seeds, key=lambda x: x.priority, reverse=True):
                key = self._seed_signature(seed)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(seed)
            if discovery_seeds:
                discovery_seen = {self._seed_signature(seed): seed for seed in discovery_seeds}
                for seed in monitoring_seeds:
                    key = self._seed_signature(seed)
                    if key in discovery_seen:
                        continue
                    combined_monitoring.append(seed)
            return merged, combined_monitoring

        return discovery_seeds, monitoring_seeds

    def _previous_run_started_at(self) -> str | None:
        if not MANIFEST_PATH.exists():
            return None
        try:
            payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except Exception:
            return None
        started_at = payload.get("started_at_utc")
        if isinstance(started_at, str) and started_at.strip():
            return started_at.strip()
        return None

    def run_fetch(
        self,
        seeds: list[DiscoverySeed] | None = None,
        max_pages_per_domain: int | None = None,
        max_total_pages: int | None = None,
        max_depth: int | None = None,
    ) -> list[FetchResult]:
        seeds = seeds or self._discovery_stage()
        start = log_stage_start(self.logger, "fetch", self.job_id)
        con = connect_db(self.db_path, SCHEMA_PATH)
        fetched = run_fetch(
            con,
            seeds,
            self.config,
            self.logger,
            self.metrics,
            self.job_id,
            max_pages_per_domain=max_pages_per_domain,
            max_total_pages=max_total_pages,
            max_depth=max_depth,
        )
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

    def run_export(
        self,
        tier: str = "A",
        limit: int = 200,
        research_limit: int = 200,
        since: str | None = None,
        new_limit: int = 100,
        signal_limit: int = 200,
    ) -> dict[str, object]:
        con = connect_db(self.db_path, SCHEMA_PATH)
        result = export_outreach(con, OUT_DIR, tier=tier, limit=limit, run_id=self.job_id)
        research_path = export_research_queue(con, OUT_DIR, limit=research_limit, run_id=self.job_id)
        merge_report = export_merge_suggestions(con, OUT_DIR, run_id=self.job_id)
        quality = run_quality_report(con, OUT_DIR)
        new_leads = export_new_leads(
            con,
            OUT_DIR,
            since=since or (datetime.now().isoformat(timespec="seconds")),
            limit=new_limit,
            run_id=self.job_id,
        )
        signal_path = export_buyer_signal_queue(
            con,
            OUT_DIR,
            since=since or (datetime.now().isoformat(timespec="seconds")),
            limit=signal_limit,
            run_id=self.job_id,
        )
        con.close()
        return {
            "outreach": result,
            "research": research_path,
            "merge_suggestions": merge_report,
            "quality": quality,
            "new_leads": new_leads,
            "buying_signal_watchlist": signal_path,
        }

    def run_quality(self) -> dict[str, object]:
        con = connect_db(self.db_path, SCHEMA_PATH)
        payload = run_quality_report(con, OUT_DIR)
        con.close()
        return payload

    def run_crawl(
        self,
        seed_limit: int | None = None,
        crawl_mode: str = "full",
        discovery_limit: int | None = None,
        monitor_limit: int | None = None,
        stale_days: int | None = None,
        growth_max_pages: int | None = None,
        growth_max_total: int | None = None,
        growth_max_depth: int | None = None,
        monitor_max_pages: int | None = None,
        monitor_max_total: int | None = None,
        monitor_max_depth: int | None = None,
    ) -> dict[str, object]:
        self.max_pages = seed_limit or self.max_pages
        growth_limit = discovery_limit or self.max_pages
        discovery_seeds, monitoring_seeds = self._build_seed_plan(
            crawl_mode=crawl_mode,
            discovery_limit=growth_limit,
            monitor_limit=monitor_limit,
            stale_days=stale_days,
        )

        fetched: list[FetchResult] = []
        if discovery_seeds:
            fetched.extend(
                self.run_fetch(
                    seeds=discovery_seeds,
                    max_pages_per_domain=growth_max_pages or (self.config.growth_max_pages_per_domain or None),
                    max_total_pages=growth_max_total or (self.config.growth_max_total_pages or None),
                    max_depth=growth_max_depth or (self.config.growth_max_depth or None),
                )
            )
        if monitoring_seeds:
            fetched.extend(
                self.run_fetch(
                    seeds=monitoring_seeds,
                    max_pages_per_domain=monitor_max_pages or self.config.monitor_max_pages_per_domain,
                    max_total_pages=monitor_max_total or self.config.monitor_max_total_pages,
                    max_depth=monitor_max_depth or self.config.monitor_max_depth,
                )
            )

        if discovery_seeds or monitoring_seeds:
            self.run_enrich(fetched=fetched)
            self.run_score()
        if not discovery_seeds and not monitoring_seeds:
            self.metrics.inc("no_seeds")
            return {"outreach": "", "research": "", "merge_suggestions": "", "quality": {}, "new_leads": "", "buying_signal_watchlist": ""}

        previous_run = self._previous_run_started_at()
        if previous_run:
            # Keep output window focused on changes since last successful run.
            last_week_cutoff = previous_run
        else:
            last_week_cutoff = (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")

        report = self.run_export(tier="A", limit=200, research_limit=200, since=last_week_cutoff, new_limit=100, signal_limit=200)
        return report
