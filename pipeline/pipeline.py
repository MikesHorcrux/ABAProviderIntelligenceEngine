from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jobs.ingest_sources import load_reference_rules
from pipeline.config import load_crawl_config
from pipeline.db import connect_db
from pipeline.fetch_backends.common import FetchResult
from pipeline.observability import Metrics, build_logger
from pipeline.stages.discovery import DiscoverySeed, dedupe_seeds, load_seeds
from pipeline.stages.export import export_provider_intel
from pipeline.stages.extract import evidence_to_json, extract_records
from pipeline.stages.fetch import run_fetch
from pipeline.stages.qa import run_qa
from pipeline.stages.resolve import resolve_extracted_records
from pipeline.stages.score import run_score
from pipeline.utils import make_pk, normalize_url, utcnow_iso
from runtime_context import RuntimePaths, default_runtime_paths


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_PATHS = default_runtime_paths()
DB_PATH = DEFAULT_RUNTIME_PATHS.db_path
SCHEMA_PATH = ROOT / "db" / "schema.sql"
OUT_DIR = DEFAULT_RUNTIME_PATHS.out_root
MANIFEST_PATH = DEFAULT_RUNTIME_PATHS.manifest_path


class PipelineRunner:
    def __init__(
        self,
        seeds: str | None = None,
        max_pages: int | None = None,
        db_path: str | Path = DB_PATH,
        *,
        db_timeout_ms: int | None = None,
        config_overrides: dict[str, Any] | None = None,
        crawl_mode: str = "full",
        runtime_paths: RuntimePaths | None = None,
    ):
        self.runtime_paths = runtime_paths or DEFAULT_RUNTIME_PATHS
        self.db_path = Path(db_path)
        self.max_pages = max_pages
        self.db_timeout_ms = int(db_timeout_ms) if db_timeout_ms is not None else None
        self.config = load_crawl_config()
        self.crawl_mode = str(crawl_mode or "full").strip().lower()
        self._apply_config_overrides(config_overrides or {})
        self.seeds_path = str(seeds or self.config.seed_file or "seed_packs/nj/seed_pack.json")
        self.job_id = utcnow_iso().replace(":", "").replace("-", "").replace("T", "-")
        self.logger = build_logger(self.job_id, "provider_intel")
        self.metrics = Metrics(self.job_id)
        self._seed_lookup_cache: dict[str, DiscoverySeed] = {}
        self._metro_lookup_cache: dict[str, str] = {}

    def _apply_config_overrides(self, overrides: dict[str, Any]) -> None:
        for key, value in overrides.items():
            if value is None or not hasattr(self.config, key):
                continue
            setattr(self.config, key, value)

    def _fetch_mode_overrides(self) -> dict[str, int | None]:
        if self.crawl_mode != "refresh":
            return {
                "max_pages_per_domain": None,
                "max_total_pages": None,
                "max_depth": None,
            }

        max_pages_per_domain = max(
            1,
            int(self.config.monitor_max_pages_per_domain or self.config.max_pages_per_domain or 1),
        )
        max_total_pages = int(self.config.monitor_max_total_pages or max_pages_per_domain)
        if max_total_pages <= 0:
            max_total_pages = max_pages_per_domain
        max_depth = max(
            0,
            int(self.config.monitor_max_depth if self.config.monitor_max_depth is not None else self.config.max_depth),
        )
        return {
            "max_pages_per_domain": max_pages_per_domain,
            "max_total_pages": max_total_pages,
            "max_depth": max_depth,
        }

    def _seed_pack_path(self) -> Path:
        candidate = Path(self.seeds_path)
        if candidate.is_absolute():
            return candidate
        return (ROOT / candidate).resolve()

    def _load_metro_lookup(self) -> dict[str, str]:
        if self._metro_lookup_cache:
            return dict(self._metro_lookup_cache)
        path = ROOT / "reference" / "metros" / "nj.json"
        payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"metros": {}}
        self._metro_lookup_cache = {str(k).lower(): str(v) for k, v in dict(payload.get("metros") or {}).items()}
        return dict(self._metro_lookup_cache)

    def _load_seeds(self, seed_limit: int | None = None) -> list[DiscoverySeed]:
        batch = load_seeds(str(self._seed_pack_path()), source="seed_pack", priority=100)
        seeds = dedupe_seeds(batch.seeds, limit=seed_limit or self.max_pages)
        self._seed_lookup_cache = {normalize_url(seed.website): seed for seed in seeds}
        return seeds

    def _lookup_seed(self, website: str, name: str, state: str, market: str) -> DiscoverySeed:
        seed = self._seed_lookup_cache.get(normalize_url(website))
        if seed:
            return seed
        return DiscoverySeed(name=name, website=website, state=state, market=market, source="seed_pack")

    def _load_results_for_extraction(self, since: str | None = None) -> list[FetchResult]:
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        query = """
            SELECT cj.crawl_job_pk, cj.seed_name, cj.seed_domain, cr.target_url, cr.status_code, cr.content,
                   cr.content_hash, cr.fetched_at
            FROM crawl_jobs cj
            INNER JOIN crawl_results cr ON cr.crawl_job_pk = cj.crawl_job_pk
            WHERE cr.status_code = 200
              AND cr.content <> ''
        """
        params: tuple[object, ...] = ()
        if since:
            query += " AND cr.fetched_at >= ?"
            params = (since,)
        query += " ORDER BY cr.fetched_at ASC"
        rows = con.execute(query, params).fetchall()
        con.close()
        fetched: list[FetchResult] = []
        for row in rows:
            fetched.append(
                FetchResult(
                    job_pk=row["crawl_job_pk"],
                    seed_name=row["seed_name"],
                    seed_state="NJ",
                    seed_market="",
                    seed_website=row["seed_domain"],
                    target_url=row["target_url"],
                    normalized_url=row["target_url"],
                    status_code=int(row["status_code"] or 0),
                    content=row["content"],
                    content_hash=row["content_hash"],
                    fetched_at=row["fetched_at"],
                )
            )
        return fetched

    def run_seed_ingest(self, seed_limit: int | None = None) -> dict[str, object]:
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        for table in (
            "providers",
            "practices",
            "practice_locations",
            "licenses",
            "provider_practice_records",
            "source_documents",
            "extracted_records",
            "field_evidence",
            "contradictions",
            "review_queue",
            "prescriber_rules",
        ):
            con.execute(f"DELETE FROM {table}")
        rule_count = load_reference_rules(con)
        seeds = self._load_seeds(seed_limit=seed_limit)
        con.commit()
        con.close()
        return {
            "seed_count": len(seeds),
            "seed_pack_path": str(self._seed_pack_path()),
            "rule_count": rule_count,
            "state": "NJ",
        }

    def run_fetch(
        self,
        seeds: list[DiscoverySeed] | None = None,
        max_pages_per_domain: int | None = None,
        max_total_pages: int | None = None,
        max_depth: int | None = None,
        run_state_dir: str | Path | None = None,
    ) -> list[FetchResult]:
        seeds = seeds or self._load_seeds()
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        mode_overrides = self._fetch_mode_overrides()
        effective_max_pages = max_pages_per_domain if max_pages_per_domain is not None else mode_overrides["max_pages_per_domain"]
        effective_max_total = max_total_pages if max_total_pages is not None else mode_overrides["max_total_pages"]
        effective_max_depth = max_depth if max_depth is not None else mode_overrides["max_depth"]
        fetched = run_fetch(
            con=con,
            seeds=seeds,
            cfg=self.config,
            logger=self.logger,
            metrics=self.metrics,
            job_id=self.job_id,
            max_pages_per_domain=effective_max_pages,
            max_total_pages=effective_max_total,
            max_depth=effective_max_depth,
            run_state_dir=run_state_dir,
        )
        con.close()
        return fetched

    def run_extract(self, fetched: list[FetchResult] | None = None, since: str | None = None) -> int:
        fetched_rows = list(fetched or self._load_results_for_extraction(since))
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        metro_lookup = self._load_metro_lookup()
        extracted_count = 0
        for item in fetched_rows:
            seed = self._lookup_seed(item.seed_website, item.seed_name, item.seed_state, item.seed_market)
            source_document_id = make_pk("src", [item.target_url, item.content_hash, seed.tier, seed.source_type])
            con.execute(
                """
                INSERT OR REPLACE INTO source_documents
                (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type,
                 extraction_profile, status_code, content_hash, content, snapshot_path, fetched_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
                """,
                (
                    source_document_id,
                    item.job_pk,
                    item.target_url,
                    item.normalized_url,
                    seed.tier,
                    seed.source_type,
                    seed.extraction_profile,
                    item.status_code,
                    item.content_hash,
                    item.content,
                    item.fetched_at,
                    utcnow_iso(),
                ),
            )
            for extracted in extract_records(item, seed, metro_lookup):
                extracted_id = make_pk("ext", [source_document_id, extracted.provider_name or extracted.practice_name, extracted.source_url])
                con.execute(
                    """
                    INSERT OR REPLACE INTO extracted_records
                    (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile,
                     provider_name, credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state,
                     zip, metro, license_state, license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json,
                     telehealth, insurance_notes, waitlist_notes, referral_requirements, evidence_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        extracted_id,
                        source_document_id,
                        extracted.source_url,
                        extracted.source_tier,
                        extracted.source_type,
                        extracted.extraction_profile,
                        extracted.provider_name,
                        extracted.credentials,
                        extracted.npi,
                        extracted.practice_name,
                        extracted.intake_url,
                        extracted.phone,
                        extracted.fax,
                        extracted.address_1,
                        extracted.city,
                        extracted.state,
                        extracted.zip_code,
                        extracted.metro,
                        extracted.license_state,
                        extracted.license_type,
                        extracted.license_status,
                        extracted.diagnoses_asd,
                        extracted.diagnoses_adhd,
                        json.dumps(extracted.age_groups),
                        extracted.telehealth,
                        extracted.insurance_notes,
                        extracted.waitlist_notes,
                        extracted.referral_requirements,
                        evidence_to_json(extracted.evidence),
                        utcnow_iso(),
                    ),
                )
                extracted_count += 1
        con.commit()
        con.close()
        return extracted_count

    def run_resolve(self) -> dict[str, int]:
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        result = resolve_extracted_records(con)
        con.close()
        return {
            "resolved_count": result.resolved_count,
            "review_only_count": result.review_only_count,
        }

    def run_score(self) -> int:
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        updated = run_score(con)
        con.close()
        return updated

    def run_qa(self) -> dict[str, int]:
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        result = run_qa(con)
        con.close()
        return result

    def run_export(self, limit: int = 100) -> dict[str, object]:
        con = connect_db(self.db_path, SCHEMA_PATH, timeout_ms=self.db_timeout_ms)
        result = export_provider_intel(con, self.runtime_paths.out_root, self.job_id, limit=limit)
        con.close()
        return result

    def _write_last_run_manifest(self, payload: dict[str, object]) -> None:
        manifest_path = self.runtime_paths.manifest_path
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
