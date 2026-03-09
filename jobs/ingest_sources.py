#!/usr/bin/env python3.11
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path


BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "provider_intel_v1.db"
SCHEMA = BASE / "db" / "schema.sql"
SCHEMA_TEXT = SCHEMA.read_text(encoding="utf-8")

SCHEMA_VERSION = 1
SCHEMA_MIGRATION_NAME = "provider_intel.v1"
SCHEMA_CHECKSUM = hashlib.sha256(SCHEMA_TEXT.encode("utf-8")).hexdigest()

REQUIRED_TABLES = {
    "schema_migrations",
    "providers",
    "practices",
    "practice_locations",
    "provider_practice_records",
    "licenses",
    "source_documents",
    "extracted_records",
    "field_evidence",
    "contradictions",
    "review_queue",
    "prescriber_rules",
    "crawl_jobs",
    "seed_telemetry",
    "crawl_results",
}
REQUIRED_COLUMNS = {
    "provider_practice_records": {
        "outreach_fit_score",
        "outreach_ready",
        "outreach_reasons_json",
    },
}


def assert_schema_layout(con: sqlite3.Connection) -> None:
    tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    missing = REQUIRED_TABLES - tables
    if missing:
        raise SystemExit(f"Schema drift detected. Missing tables: {', '.join(sorted(missing))}")
    for table_name, columns in REQUIRED_COLUMNS.items():
        existing = {str(row[1]) for row in con.execute(f"PRAGMA table_info({table_name})")}
        missing_columns = columns - existing
        if missing_columns:
            raise SystemExit(f"Schema drift detected. Missing columns on {table_name}: {', '.join(sorted(missing_columns))}")


def assert_schema_migration(con: sqlite3.Connection) -> None:
    version = int((con.execute("PRAGMA user_version").fetchone() or [0])[0])
    if version != SCHEMA_VERSION:
        raise SystemExit(f"Schema version mismatch. Expected {SCHEMA_VERSION}, found {version}.")

    row = con.execute(
        "SELECT migration_name, schema_checksum FROM schema_migrations WHERE schema_version=?",
        (SCHEMA_VERSION,),
    ).fetchone()
    if not row:
        raise SystemExit("schema_migrations record missing for provider_intel.v1")
    if row[0] != SCHEMA_MIGRATION_NAME or row[1] != SCHEMA_CHECKSUM:
        raise SystemExit("Schema migration metadata mismatch for provider_intel.v1")


def init_db(con: sqlite3.Connection) -> None:
    tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "provider_practice_records" in tables:
        provider_record_columns = {str(row[1]) for row in con.execute("PRAGMA table_info(provider_practice_records)")}
        if "outreach_fit_score" not in provider_record_columns:
            con.execute("ALTER TABLE provider_practice_records ADD COLUMN outreach_fit_score REAL NOT NULL DEFAULT 0.0")
        if "outreach_ready" not in provider_record_columns:
            con.execute("ALTER TABLE provider_practice_records ADD COLUMN outreach_ready INTEGER NOT NULL DEFAULT 0")
        if "outreach_reasons_json" not in provider_record_columns:
            con.execute("ALTER TABLE provider_practice_records ADD COLUMN outreach_reasons_json TEXT NOT NULL DEFAULT '[]'")
    con.executescript(SCHEMA_TEXT)
    provider_record_columns = {str(row[1]) for row in con.execute("PRAGMA table_info(provider_practice_records)")}
    if "outreach_fit_score" not in provider_record_columns:
        con.execute("ALTER TABLE provider_practice_records ADD COLUMN outreach_fit_score REAL NOT NULL DEFAULT 0.0")
    if "outreach_ready" not in provider_record_columns:
        con.execute("ALTER TABLE provider_practice_records ADD COLUMN outreach_ready INTEGER NOT NULL DEFAULT 0")
    if "outreach_reasons_json" not in provider_record_columns:
        con.execute("ALTER TABLE provider_practice_records ADD COLUMN outreach_reasons_json TEXT NOT NULL DEFAULT '[]'")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_provider_practice_records_outreach ON provider_practice_records(outreach_ready, outreach_fit_score)"
    )
    con.execute(
        """
        INSERT OR REPLACE INTO schema_migrations
        (schema_version, migration_name, schema_checksum, applied_at)
        VALUES (?, ?, ?, ?)
        """,
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


def load_reference_rules(con: sqlite3.Connection) -> int:
    rules_path = BASE / "reference" / "prescriber_rules" / "nj.json"
    if not rules_path.exists():
        return 0
    payload = json.loads(rules_path.read_text(encoding="utf-8"))
    now = datetime.now().isoformat(timespec="seconds")
    inserted = 0
    for row in payload.get("rules", []):
        con.execute(
            """
            INSERT OR REPLACE INTO prescriber_rules
            (rule_id, schema_name, state, credential, license_type, authority, limitations,
             rationale, citation_title, citation_url, retrieved_at, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(row["rule_id"]),
                str(payload.get("schema_version", "prescriber_rules.v1")),
                str(row.get("state", "")),
                str(row.get("credential", "")),
                str(row.get("license_type", "")),
                str(row.get("authority", "unknown")),
                str(row.get("limitations", "")),
                str(row.get("rationale", "")),
                str(row.get("citation_title", "")),
                str(row.get("citation_url", "")),
                str(row.get("retrieved_at", now)),
                int(bool(row.get("active", True))),
            ),
        )
        inserted += 1
    con.commit()
    return inserted


def main() -> None:
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    init_db(con)
    inserted = load_reference_rules(con)
    con.close()
    print(f"Bootstrapped provider intelligence DB at {DB} and loaded {inserted} prescriber rules.")


if __name__ == "__main__":
    main()
