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


def test_resolve_routes_practice_only_signal_to_review_queue() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    now = "2026-03-09T00:00:00Z"
    con.execute(
        """
        INSERT INTO source_documents
        (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
         status_code, content_hash, content, snapshot_path, fetched_at, created_at)
        VALUES ('src_review', 'job_review', 'https://hospital.example/autism-evaluations', 'https://hospital.example/autism-evaluations',
                'A', 'hospital_directory', 'hospital', 200, 'hash_review', '<html/>', '', ?, ?)
        """,
        (now, now),
    )
    evidence = json.dumps(
        [
            {
                "field": "diagnoses_asd",
                "value": "yes",
                "quote": "Evaluation for developmental delays and autism is available.",
                "source_url": "https://hospital.example/autism-evaluations",
            }
        ]
    )
    con.execute(
        """
        INSERT INTO extracted_records
        (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
         credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
         license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
         waitlist_notes, referral_requirements, evidence_json, created_at)
        VALUES ('ext_review', 'src_review', 'https://hospital.example/autism-evaluations', 'A', 'hospital_directory', 'hospital',
                '', '', '', 'RWJBarnabas Developmental Evaluations', 'https://hospital.example/request', '(888) 724-7123', '',
                '', 'Livingston', 'NJ', '', 'Newark', 'NJ', 'unknown', 'unknown', 'yes', 'unclear', '[\"child\"]', 'unknown',
                '', '', '', ?, ?)
        """,
        (evidence, now),
    )
    result = resolve_extracted_records(con)
    record_count = con.execute("SELECT COUNT(*) FROM provider_practice_records").fetchone()[0]
    review_count = con.execute("SELECT COUNT(*) FROM review_queue WHERE review_type='missing_provider'").fetchone()[0]
    assert result.resolved_count == 0
    assert result.review_only_count == 1
    assert record_count == 0
    assert review_count == 1
    con.close()


def test_resolve_dedupes_missing_provider_reviews_by_practice_domain() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    now = "2026-03-09T00:00:00Z"
    evidence = json.dumps(
        [
            {
                "field": "diagnoses_asd",
                "value": "yes",
                "quote": "Evaluation for developmental delays and autism is available.",
                "source_url": "https://hospital.example/autism-evaluations",
            }
        ]
    )
    for source_document_id, source_url, extracted_id in (
        ("src_review_a", "https://hospital.example/autism-evaluations/about", "ext_review_a"),
        ("src_review_b", "https://hospital.example/autism-evaluations/contact", "ext_review_b"),
    ):
        con.execute(
            """
            INSERT INTO source_documents
            (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
             status_code, content_hash, content, snapshot_path, fetched_at, created_at)
            VALUES (?, ?, ?, ?, 'A', 'hospital_directory', 'hospital', 200, ?, '<html/>', '', ?, ?)
            """,
            (source_document_id, extracted_id, source_url, source_url, f"hash_{extracted_id}", now, now),
        )
        con.execute(
            """
            INSERT INTO extracted_records
            (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
             credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
             license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
             waitlist_notes, referral_requirements, evidence_json, created_at)
            VALUES (?, ?, ?, 'A', 'hospital_directory', 'hospital', '', '', '', 'RWJBarnabas Developmental Evaluations',
                    '', '(888) 724-7123', '', '', 'Livingston', 'NJ', '', 'Newark', 'NJ', 'unknown', 'unknown', 'yes',
                    'unclear', '[\"child\"]', 'unknown', '', '', '', ?, ?)
            """,
            (extracted_id, source_document_id, source_url, evidence, now),
        )
    result = resolve_extracted_records(con)
    review_count = con.execute("SELECT COUNT(*) FROM review_queue WHERE review_type='missing_provider'").fetchone()[0]
    assert result.review_only_count == 2
    assert review_count == 1
    con.close()


def test_resolve_keeps_multiple_providers_on_one_practice() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    now = "2026-03-09T00:00:00Z"
    con.execute(
        """
        INSERT INTO source_documents
        (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
         status_code, content_hash, content, snapshot_path, fetched_at, created_at)
        VALUES ('src_multi', 'job_multi', 'https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas',
                'https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas', 'B', 'university_directory',
                'hospital', 200, 'hash_multi', '<html/>', '', ?, ?)
        """,
        (now, now),
    )
    for extracted_id, provider_name, credentials in (
        ("ext_multi_one", "James Maraventano", "EdD, BCBA-D"),
        ("ext_multi_two", "Joshua Cohen", ""),
    ):
        evidence = json.dumps(
            [
                {
                    "field": "provider_name",
                    "value": provider_name,
                    "quote": provider_name,
                    "source_url": "https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
                }
            ]
        )
        con.execute(
            """
            INSERT INTO extracted_records
            (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
             credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
             license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
             waitlist_notes, referral_requirements, evidence_json, created_at)
            VALUES (?, 'src_multi', 'https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas',
                    'B', 'university_directory', 'hospital', ?, ?, '', 'RCAAS | Rutgers Center of Adult Autism Services',
                    '', '', '', '', '', 'NJ', '', 'Newark', 'NJ', 'unknown', 'unknown', 'unclear', 'unclear', '[\"adult\"]',
                    'unknown', '', '', '', ?, ?)
            """,
            (extracted_id, provider_name, credentials, evidence, now),
        )
    result = resolve_extracted_records(con)
    provider_count = con.execute("SELECT COUNT(*) FROM providers").fetchone()[0]
    record_count = con.execute("SELECT COUNT(*) FROM provider_practice_records").fetchone()[0]
    assert result.resolved_count == 2
    assert provider_count == 2
    assert record_count == 2
    con.close()


def test_resolve_uses_board_records_to_enrich_existing_provider_records() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    now = "2026-03-09T00:00:00Z"
    con.execute(
        """
        INSERT INTO source_documents
        (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
         status_code, content_hash, content, snapshot_path, fetched_at, created_at)
        VALUES ('src_practice', 'job_practice', 'https://practice.example/about', 'https://practice.example/about',
                'C', 'practice_site', 'practice', 200, 'hash_practice', '<html/>', '', ?, ?)
        """,
        (now, now),
    )
    con.execute(
        """
        INSERT INTO source_documents
        (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
         status_code, content_hash, content, snapshot_path, fetched_at, created_at)
        VALUES ('src_board', 'job_board', 'https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/',
                'https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/', 'A', 'licensing_board', 'board',
                200, 'hash_board', '<html/>', '', ?, ?)
        """,
        (now, now),
    )
    practice_evidence = json.dumps(
        [
            {"field": "provider_name", "value": "Jane Smith", "quote": "Dr. Jane Smith, PsyD", "source_url": "https://practice.example/about"},
            {"field": "diagnoses_asd", "value": "yes", "quote": "autism diagnostic evaluations", "source_url": "https://practice.example/about"},
            {"field": "diagnoses_adhd", "value": "yes", "quote": "ADHD assessment", "source_url": "https://practice.example/about"},
        ]
    )
    board_evidence = json.dumps(
        [
            {"field": "provider_name", "value": "Jane Smith", "quote": "Licensee Name: Dr. Jane Smith", "source_url": "https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/"},
            {"field": "license_status", "value": "active", "quote": "License Status: Active", "source_url": "https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/"},
        ]
    )
    con.execute(
        """
        INSERT INTO extracted_records
        (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
         credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
         license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
         waitlist_notes, referral_requirements, evidence_json, created_at)
        VALUES ('ext_a_practice', 'src_practice', 'https://practice.example/about', 'C', 'practice_site', 'practice', 'Jane Smith',
                'PsyD', '', 'Garden State Psychology Center', 'https://practice.example/intake', '(973) 555-0112', '', '', 'Newark',
                'NJ', '', 'Newark', 'NJ', 'psychologist', 'unknown', 'yes', 'yes', '[\"child\"]', 'yes', '', '', '', ?, ?)
        """,
        (practice_evidence, now),
    )
    con.execute(
        """
        INSERT INTO extracted_records
        (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
         credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
         license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
         waitlist_notes, referral_requirements, evidence_json, created_at)
        VALUES ('ext_b_board', 'src_board', 'https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/',
                'A', 'licensing_board', 'board', 'Jane Smith', '', '', 'NJ Psychology Board', '', '', '', '', '',
                'NJ', '', 'statewide', 'NJ', 'psychologist', 'active', 'unclear', 'unclear', '[]', 'unknown', '', '', '', ?, ?)
        """,
        (board_evidence, now),
    )
    result = resolve_extracted_records(con)
    record = con.execute("SELECT license_status, source_urls_json FROM provider_practice_records").fetchone()
    evidence_count = con.execute("SELECT COUNT(*) FROM field_evidence WHERE field_name='license_status'").fetchone()[0]
    assert result.resolved_count == 2
    assert record["license_status"] == "active"
    assert "practice.example/about" in record["source_urls_json"]
    assert "LicenseVerification" in record["source_urls_json"]
    assert evidence_count == 1
    con.close()


def main() -> None:
    test_resolve_dedupes_by_npi_and_creates_one_record()
    test_resolve_routes_practice_only_signal_to_review_queue()
    test_resolve_dedupes_missing_provider_reviews_by_practice_domain()
    test_resolve_keeps_multiple_providers_on_one_practice()
    test_resolve_uses_board_records_to_enrich_existing_provider_records()
    print("test_resolve_stage: ok")


if __name__ == "__main__":
    main()
