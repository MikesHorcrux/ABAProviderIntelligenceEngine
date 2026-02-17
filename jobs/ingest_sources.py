#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from adapters.base import LicenseRow
from adapters.registry import build_adapters


BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data/cannaradar_v1.db"
SCHEMA = BASE / "db/schema.sql"
SCHEMA_TEXT = SCHEMA.read_text()

SCHEMA_VERSION = 5
SCHEMA_MIGRATION_NAME = "v1.5.1"
SCHEMA_CHECKSUM = hashlib.sha256(SCHEMA_TEXT.encode("utf-8")).hexdigest()

SCHEMA_ROLLBACK = """
Rollback guidance:
1) Stop writes to data/cannaradar_v1.db.
2) Backup current DB: cp data/cannaradar_v1.db data/cannaradar_v1.db.<timestamp>.bak
3) Restore latest known-good backup.
4) Re-run: PYTHONPATH=$PWD python3 jobs/ingest_sources.py
""".strip()

REQUIRED_TABLE_COLUMNS = {
    "organizations": {"org_pk", "legal_name", "dba_name", "state", "created_at", "updated_at", "last_seen_at", "deleted_at"},
    "licenses": {"license_pk", "org_pk", "state", "license_id", "license_type", "status", "source_url", "retrieved_at", "fingerprint"},
    "locations": {"location_pk", "org_pk", "canonical_name", "address_1", "city", "state", "zip", "website_domain", "phone", "fit_score", "last_crawled_at", "created_at", "updated_at", "last_seen_at", "deleted_at"},
    "contact_points": {"contact_pk", "location_pk", "type", "value", "confidence", "source_url", "first_seen_at", "last_seen_at", "created_at", "updated_at", "deleted_at"},
    "contacts": {"contact_pk", "location_pk", "full_name", "role", "email", "phone", "source_kind", "confidence", "verification_status", "created_at", "updated_at", "last_seen_at", "deleted_at"},
    "evidence": {"evidence_pk", "entity_type", "entity_pk", "field_name", "field_value", "source_url", "snippet", "captured_at", "deleted_at"},
    "schema_migrations": {"schema_version", "migration_name", "schema_checksum", "applied_at"},
    "companies": {"company_pk", "organization_pk", "legal_name", "dba_name", "state", "created_at", "updated_at", "last_seen_at", "deleted_at"},
    "domains": {"domain_pk", "location_pk", "domain", "is_primary", "confidence", "source_url", "last_seen_at", "created_at", "updated_at", "deleted_at"},
    "enrichment_sources": {"enrichment_source_pk", "source_type", "source_name", "source_url", "fetched_at", "success", "payload_hash"},
    "crawl_jobs": {"crawl_job_pk", "seed_name", "seed_domain", "status", "mode", "created_at", "updated_at", "deleted_at"},
    "crawl_results": {"crawl_result_pk", "crawl_job_pk", "requested_url", "target_url", "status_code", "content_hash", "content", "fetched_at", "created_at", "updated_at", "deleted_at"},
    "entity_resolutions": {"resolution_pk", "canonical_location_pk", "candidate_location_pk", "resolution_status", "reason", "confidence", "created_at", "updated_at", "deleted_at"},
    "lead_scores": {"score_pk", "location_pk", "score_total", "tier", "run_id", "created_at", "as_of", "deleted_at"},
    "scoring_features": {"feature_pk", "score_pk", "feature_name", "feature_value", "created_at"},
    "outreach_events": {"event_pk", "location_pk", "channel", "outcome", "notes", "created_at", "created_by", "deleted_at"},
}

REQUIRED_INDEXES = {
    "locations": {
        "idx_locations_org_pk",
        "idx_locations_state",
        "idx_locations_website_domain",
        "uq_locations_org_name",
        "uq_locations_website_domain_phone",
        "uq_locations_address",
    },
    "domains": {
        "uq_domains_domain",
        "idx_domains_location_pk",
    },
    "contact_points": {
        "idx_contact_points_location_pk",
        "idx_contact_points_type",
        "uq_contact_points",
    },
    "lead_scores": {
        "idx_lead_scores_location",
        "idx_lead_scores_tier",
        "idx_lead_scores_as_of",
    },
    "crawl_jobs": {
        "idx_crawl_jobs_status",
    },
    "crawl_results": {
        "idx_crawl_results_job",
        "uq_crawl_result_lookup",
    },
}


def make_pk(prefix: str, parts: list[str]) -> str:
    s = "|".join((p or "").strip().lower() for p in parts)
    h = hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{prefix}_{h}"


def normalized_domain(url_or_domain: str) -> str:
    v = (url_or_domain or "").strip()
    if not v:
        return ""
    if "://" not in v:
        v = f"https://{v}"
    try:
        host = (urlparse(v).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def assert_schema_layout(con: sqlite3.Connection) -> None:
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    missing_tables = [x for x in REQUIRED_TABLE_COLUMNS if x not in tables]
    if missing_tables:
        raise SystemExit(f"Schema drift detected. Missing tables: {', '.join(sorted(missing_tables))}")

    for table, required in REQUIRED_TABLE_COLUMNS.items():
        cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}
        missing_cols = required - cols
        if missing_cols:
            raise SystemExit(f"Schema drift detected for {table}. Missing columns: {', '.join(sorted(missing_cols))}")

    for table, required_indexes in REQUIRED_INDEXES.items():
        indexes = {r[1] for r in con.execute(f"PRAGMA index_list({table})").fetchall()}
        missing_indexes = required_indexes - indexes
        if missing_indexes:
            raise SystemExit(
                f"Schema drift detected for {table}. Missing indexes: {', '.join(sorted(missing_indexes))}"
            )


def assert_schema_migration(con: sqlite3.Connection) -> None:
    current_version = int((con.execute("PRAGMA user_version").fetchone() or [0])[0])
    if current_version != SCHEMA_VERSION:
        raise SystemExit(
            f"Schema version mismatch. Expected {SCHEMA_VERSION}, found {current_version}. {SCHEMA_ROLLBACK}"
        )

    if con.execute("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone()[0] != 1:
        raise SystemExit(f"Missing schema_migrations table. {SCHEMA_ROLLBACK}")

    row = con.execute(
        "SELECT schema_checksum, migration_name FROM schema_migrations WHERE schema_version=?",
        (SCHEMA_VERSION,),
    ).fetchone()
    if not row:
        # Fail fast on missing migration metadata for non-empty DBs. This protects operators from running
        # against untracked ad-hoc schema modifications.
        raise SystemExit(
            "schema_migrations record is missing for the current schema version. "
            f"Expected row for version {SCHEMA_VERSION}. {SCHEMA_ROLLBACK}"
        )

    migration_checksum, migration_name = row
    if migration_checksum != SCHEMA_CHECKSUM:
        raise SystemExit(
            f"Schema checksum mismatch for version {SCHEMA_VERSION}."
            f" Expected {SCHEMA_CHECKSUM}, found {migration_checksum}. {SCHEMA_ROLLBACK}"
        )
    if migration_name != SCHEMA_MIGRATION_NAME:
        print(f"Schema migration name mismatch. DB has {migration_name}, expected {SCHEMA_MIGRATION_NAME}.")


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA_TEXT)
    con.execute(
        "INSERT OR IGNORE INTO schema_migrations (schema_version, migration_name, schema_checksum, applied_at) VALUES (?, ?, ?, ?)",
        (
            SCHEMA_VERSION,
            SCHEMA_MIGRATION_NAME,
            SCHEMA_CHECKSUM,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    assert_schema_layout(con)
    assert_schema_migration(con)
    con.commit()


def upsert_row(con: sqlite3.Connection, row: LicenseRow, now: str) -> None:
    domain = normalized_domain(row.website)
    org_pk = make_pk("org", [row.legal_name or row.dba_name, row.state, domain])
    loc_pk = make_pk("loc", [row.dba_name or row.legal_name, row.state, domain])
    lic_pk = make_pk("lic", [row.legal_name, row.license_id or domain or row.website, row.state])

    con.execute(
        """
        INSERT OR REPLACE INTO organizations
        (org_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '')
        """,
        (org_pk, row.legal_name, row.dba_name, row.state, now, now, now),
    )

    con.execute(
        """
        INSERT OR REPLACE INTO companies
        (company_pk, organization_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '')
        """,
        (make_pk("co", [org_pk, row.state, row.dba_name or row.legal_name]), org_pk, row.legal_name, row.dba_name, row.state, now, now, now),
    )

    con.execute(
        """
        INSERT OR REPLACE INTO licenses
        (license_pk, org_pk, state, license_id, license_type, status, source_url, retrieved_at, fingerprint, created_at, updated_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
        """,
        (lic_pk, org_pk, row.state, row.license_id, row.license_type, row.status, row.source_url, row.retrieved_at, make_pk("fp", [row.legal_name, row.website, row.state]), now, now),
    )

    con.execute(
        """
        INSERT OR REPLACE INTO locations
        (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at, last_seen_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, '')
        """,
        (loc_pk, org_pk, row.dba_name or row.legal_name, row.address_1, row.city, row.state, row.zip, domain, row.phone, 0, now, now, now),
    )

    if domain:
        con.execute(
            """
            INSERT OR REPLACE INTO domains
            (domain_pk, location_pk, domain, is_primary, confidence, source_url, last_seen_at, created_at, updated_at, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '')
            """,
            (make_pk("dom", [loc_pk, domain]), loc_pk, domain, 1, 0.9, row.source_url, now, now, now),
        )

    if row.phone:
        con.execute(
            """
            INSERT OR REPLACE INTO contact_points
            (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at, created_at, updated_at, deleted_at)
            VALUES (?, ?, 'phone', ?, ?, ?, ?, ?, ?, ?, '')
            """,
            (make_pk("cp", [loc_pk, "phone", row.phone]), loc_pk, row.phone, 0.9, row.source_url, now, now, now, now),
        )

    con.execute(
        """
        INSERT OR REPLACE INTO evidence
        (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
        VALUES (?, 'location', ?, 'website_domain', ?, ?, 'source ingestion', ?, '')
        """,
        (make_pk("ev", [loc_pk, domain]), loc_pk, domain, row.source_url or row.website, now),
    )


def ingest_all(con: sqlite3.Connection) -> int:
    adapters = build_adapters(BASE)
    if not adapters:
        print("No adapters enabled. Nothing to ingest.")
        return 0

    now = datetime.now().isoformat(timespec="seconds")
    total = 0
    for adapter in adapters:
        raw = adapter.fetch_raw()
        rows = adapter.normalize_rows(adapter.parse_raw_to_rows(raw))
        for row in rows:
            upsert_row(con, row, now)
        total += len(rows)
        print(f"Adapter {adapter.source_name}: ingested {len(rows)} rows")
    con.commit()
    return total


def main() -> None:
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    init_db(con)
    total = ingest_all(con)
    print(f"Ingested {total} total rows into canonical DB: {DB}")


if __name__ == "__main__":
    main()
