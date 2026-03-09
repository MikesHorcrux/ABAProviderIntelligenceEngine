#!/usr/bin/env python3.11
from __future__ import annotations

import csv
import sqlite3
import tempfile
from pathlib import Path

from pipeline.stages.export import export_provider_intel
from pipeline.stages.qa import run_qa
from pipeline.stages.score import run_score


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _connect_with_schema(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def _seed_record(con: sqlite3.Connection) -> None:
    now = "2026-03-09T00:00:00Z"
    con.execute("INSERT INTO providers(provider_id, provider_name, credentials, npi, primary_license_state, primary_license_type, created_at, updated_at) VALUES ('prov_1', 'Jane Smith', 'PsyD', '', 'NJ', 'psychologist', ?, ?)", (now, now))
    con.execute("INSERT INTO practices(practice_id, practice_name, website, intake_url, phone, fax, created_at, updated_at) VALUES ('prac_1', 'Garden State Psychology Center', 'https://practice.example', 'https://practice.example/intake', '(973) 555-0112', '', ?, ?)", (now, now))
    con.execute("INSERT INTO practice_locations(location_id, practice_id, address_1, city, state, zip, metro, phone, telehealth, created_at, updated_at) VALUES ('loc_1', 'prac_1', '', 'Newark', 'NJ', '', 'Newark', '(973) 555-0112', 'yes', ?, ?)", (now, now))
    con.execute("INSERT INTO provider_practice_records(record_id, provider_id, practice_id, location_id, provider_name_snapshot, practice_name_snapshot, npi, license_state, license_type, license_status, diagnoses_asd, diagnoses_adhd, prescriptive_authority, prescriptive_basis, age_groups_json, telehealth, insurance_notes, waitlist_notes, referral_requirements, source_urls_json, field_confidence_json, record_confidence, conflict_note, review_status, export_status, blocked_reason, last_verified_at, created_at, updated_at) VALUES ('rec_1', 'prov_1', 'prac_1', 'loc_1', 'Jane Smith', 'Garden State Psychology Center', '', 'NJ', 'psychologist', 'active', 'yes', 'yes', 'unknown', '', '[\"child\",\"adult\"]', 'yes', 'Accepts major insurance', '', 'Referral preferred', '[\"https://practice.example/providers\"]', '{}', 0.0, '', 'pending', 'pending', '', ?, ?, ?)", (now, now, now))
    con.execute("INSERT INTO prescriber_rules(rule_id, schema_name, state, credential, license_type, authority, limitations, rationale, citation_title, citation_url, retrieved_at, active) VALUES ('rule_psyd', 'prescriber_rules.v1', 'NJ', 'PsyD/PhD', 'psychologist', 'no', '', 'Psychologists in New Jersey do not prescribe.', 'NJ Psychology Board', 'https://www.njconsumeraffairs.gov/psy/Pages/default.aspx', ?, 1)", (now,))
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_asd', 'rec_1', 'diagnoses_asd', 'yes', 'autism diagnostic evaluations', 'https://practice.example/providers', 'src_1', 'C', ?)", (now,))
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_adhd', 'rec_1', 'diagnoses_adhd', 'yes', 'ADHD assessment', 'https://practice.example/providers', 'src_1', 'C', ?)", (now,))
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_license', 'rec_1', 'license_status', 'active', 'License status: active', 'https://www.njconsumeraffairs.gov/psy/Pages/default.aspx', 'src_2', 'A', ?)", (now,))
    con.commit()


def test_score_qa_and_export_generate_provider_outputs() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "provider_intel.db"
        out_dir = root / "out"
        con = _connect_with_schema(db_path)
        _seed_record(con)
        assert run_score(con) == 1
        qa_result = run_qa(con)
        assert qa_result["approved_records"] == 1
        report = export_provider_intel(con, out_dir, "run-1", limit=10)
        con.close()

        records_path = Path(str(report["records_csv"]))
        review_path = Path(str(report["review_queue_csv"]))
        assert records_path.exists()
        assert review_path.exists()
        assert Path(str(report["profiles_dir"])) .exists()
        with records_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 1
        assert rows[0]["provider_name"] == "Jane Smith"
        assert rows[0]["prescriptive_authority"] == "no"
        assert rows[0]["diagnoses_asd"] == "yes"
        assert rows[0]["diagnoses_adhd"] == "yes"


def main() -> None:
    test_score_qa_and_export_generate_provider_outputs()
    print("test_lead_research: ok")


if __name__ == "__main__":
    main()
