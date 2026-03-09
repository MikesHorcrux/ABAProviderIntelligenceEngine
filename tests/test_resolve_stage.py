#!/usr/bin/env python3.11
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from pipeline.stages.resolve import resolve_extracted_records


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"


def test_resolve_dedupes_by_npi_and_creates_one_record() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    now = "2026-03-09T00:00:00Z"
    con.execute(
        """
        INSERT INTO source_documents
        (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
         status_code, content_hash, content, snapshot_path, fetched_at, created_at)
        VALUES ('src_one', 'job_one', 'https://practice.example/about', 'https://practice.example/about', 'C', 'practice_site', 'practice',
                200, 'hash1', '<html/>', '', ?, ?)
        """,
        (now, now),
    )
    evidence = json.dumps(
        [
            {"field": "provider_name", "value": "Jane Smith", "quote": "Dr. Jane Smith, PsyD", "source_url": "https://practice.example/about"},
            {"field": "diagnoses_asd", "value": "yes", "quote": "autism diagnostic evaluations", "source_url": "https://practice.example/about"},
            {"field": "diagnoses_adhd", "value": "yes", "quote": "ADHD assessment", "source_url": "https://practice.example/about"}
        ]
    )
    for extracted_id in ("ext_one", "ext_two"):
        con.execute(
            """
            INSERT INTO extracted_records
            (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
             credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
             license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
             waitlist_notes, referral_requirements, evidence_json, created_at)
            VALUES (?, 'src_one', 'https://practice.example/about', 'C', 'practice_site', 'practice', 'Jane Smith',
                    'PsyD', '1234567890', 'Garden State Psychology Center', '', '(973) 555-0112', '', '', 'Newark', 'NJ', '',
                    'Newark', 'NJ', 'psychologist', 'active', 'yes', 'yes', '["child"]', 'yes', '', '', '', ?, ?)
            """,
            (extracted_id, evidence, now),
        )
    result = resolve_extracted_records(con)
    count = con.execute("SELECT COUNT(*) FROM provider_practice_records").fetchone()[0]
    provider_count = con.execute("SELECT COUNT(*) FROM providers").fetchone()[0]
    assert result.resolved_count == 2
    assert count == 1
    assert provider_count == 1
    con.close()


def main() -> None:
    test_resolve_dedupes_by_npi_and_creates_one_record()
    print("test_resolve_stage: ok")


if __name__ == "__main__":
    main()
