#!/usr/bin/env python3.11
from __future__ import annotations

import sqlite3
from pathlib import Path

from pipeline.stages.qa import run_qa
from pipeline.utils import make_pk


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def _insert_provider_fixture(
    con: sqlite3.Connection,
    *,
    provider_id: str,
    provider_name: str,
    credentials: str,
    practice_id: str,
    practice_name: str,
    website: str,
    location_id: str,
    phone: str,
    record_id: str,
    source_url: str,
    license_status: str = "unknown",
    diagnoses_asd: str = "unclear",
    diagnoses_adhd: str = "unclear",
    record_confidence: float = 0.4,
    outreach_fit_score: float = 0.2,
) -> None:
    con.execute(
        "INSERT INTO providers(provider_id, provider_name, credentials, npi, primary_license_state, primary_license_type, created_at, updated_at) VALUES (?, ?, ?, '', 'NJ', 'psychologist', '', '')",
        (provider_id, provider_name, credentials),
    )
    con.execute(
        "INSERT INTO practices(practice_id, practice_name, website, intake_url, phone, fax, created_at, updated_at) VALUES (?, ?, ?, '', ?, '', '', '')",
        (practice_id, practice_name, website, phone),
    )
    con.execute(
        "INSERT INTO practice_locations(location_id, practice_id, address_1, city, state, zip, metro, phone, telehealth, created_at, updated_at) VALUES (?, ?, '', 'Newark', 'NJ', '', 'Newark', ?, 'unknown', '', '')",
        (location_id, practice_id, phone),
    )
    con.execute(
        """
        INSERT INTO provider_practice_records(
            record_id, provider_id, practice_id, location_id, provider_name_snapshot, practice_name_snapshot,
            license_state, license_type, license_status, diagnoses_asd, diagnoses_adhd, prescriptive_authority,
            source_urls_json, field_confidence_json, record_confidence, outreach_fit_score,
            review_status, export_status, blocked_reason, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'NJ', 'psychologist', ?, ?, ?, 'unknown', ?, '{}', ?, ?, 'pending', 'pending', '', '', '')
        """,
        (
            record_id,
            provider_id,
            practice_id,
            location_id,
            provider_name,
            practice_name,
            license_status,
            diagnoses_asd,
            diagnoses_adhd,
            f'["{source_url}"]',
            record_confidence,
            outreach_fit_score,
        ),
    )


def _insert_evidence(
    con: sqlite3.Connection,
    *,
    record_id: str,
    field_name: str,
    field_value: str,
    quote: str,
    source_url: str,
    source_tier: str = "C",
) -> None:
    con.execute(
        """
        INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at)
        VALUES (?, ?, ?, ?, ?, ?, '', ?, '2026-03-13T23:00:00Z')
        """,
        (make_pk("evi", [record_id, field_name, field_value, source_url]), record_id, field_name, field_value, quote, source_url, source_tier),
    )


def test_run_qa_triages_directory_sludge_and_seed_retire_candidate() -> None:
    con = _connect()
    source_url = "https://psychologytoday.com/us/therapists/nj"
    generic_names = [
        "or other licensed mental",
        "should be able to",
        "may do an initial",
        "help with",
        "or primary care physician",
    ]
    for idx, name in enumerate(generic_names, start=1):
        record_id = f"rec_dir_{idx}"
        _insert_provider_fixture(
            con,
            provider_id=f"prov_dir_{idx}",
            provider_name=name,
            credentials="",
            practice_id=f"prac_dir_{idx}",
            practice_name="Find a Therapist, Psychologist, Counselor - Psychology Today",
            website=f"{source_url}#seed{idx}",
            location_id=f"loc_dir_{idx}",
            phone="",
            record_id=record_id,
            source_url=source_url,
            record_confidence=0.32,
        )
        _insert_evidence(
            con,
            record_id=record_id,
            field_name="diagnoses_asd",
            field_value="yes",
            quote="A child has a diagnosable condition, such as ADHD or autism.",
            source_url=source_url,
        )

    result = run_qa(con)
    assert result["triage_directory_sludge"] == 5
    assert result["seed_retire_candidates"] == 1
    rows = con.execute("SELECT review_type, COUNT(*) AS c FROM review_queue GROUP BY review_type").fetchall()
    by_type = {row["review_type"]: int(row["c"]) for row in rows}
    assert by_type.get("directory_sludge") == 5
    assert by_type.get("seed_retire_candidate") == 1
    blocked = con.execute("SELECT DISTINCT blocked_reason FROM provider_practice_records").fetchall()
    assert any("Directory sludge" in row[0] for row in blocked)
    con.close()


def test_run_qa_triages_indirect_provider_signal() -> None:
    con = _connect()
    source_url = "https://gsapp.rutgers.edu/continuing-education/live-webinars"
    _insert_provider_fixture(
        con,
        provider_id="prov_indirect",
        provider_name="Alexandra Dillon",
        credentials="LCSW",
        practice_id="prac_indirect",
        practice_name="Rutgers Center for Adult Autism Services",
        website="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        location_id="loc_indirect",
        phone="",
        record_id="rec_indirect",
        source_url=source_url,
        record_confidence=0.41,
    )
    _insert_evidence(
        con,
        record_id="rec_indirect",
        field_name="diagnoses_asd",
        field_value="yes",
        quote="Instructor: Alexandra Dillon, LCSW",
        source_url=source_url,
    )
    result = run_qa(con)
    assert result["triage_indirect_provider_signal"] == 1
    review = con.execute("SELECT review_type, reason FROM review_queue WHERE record_id='rec_indirect'").fetchone()
    assert review["review_type"] == "indirect_provider_signal"
    assert "webinar/FAQ/resource" in review["reason"]
    con.close()


def main() -> None:
    test_run_qa_triages_directory_sludge_and_seed_retire_candidate()
    test_run_qa_triages_indirect_provider_signal()
    print("test_qa_stage: ok")


if __name__ == "__main__":
    main()
