#!/usr/bin/env python3.11
from __future__ import annotations

import csv
import json
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


def _seed_review_only_account(con: sqlite3.Connection) -> None:
    now = "2026-03-09T00:00:00Z"
    pages = (
        (
            "src_eval",
            "ext_eval",
            "rev_eval",
            "https://rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/developmental-evaluations",
            "Development Evaluations | RWJBarnabas Health",
        ),
        (
            "src_assessment",
            "ext_assessment",
            "rev_assessment",
            "https://rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/assessment-evaluation",
            "Assessment & Evaluation | RWJBarnabas Health",
        ),
        (
            "src_faq",
            "ext_faq",
            "rev_faq",
            "https://rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/frequently-asked-questions",
            "Frequently Asked Questions - New Jersey Health System",
        ),
    )
    for source_document_id, extracted_id, review_id, source_url, practice_name in pages:
        con.execute(
            """
            INSERT INTO source_documents
            (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
             status_code, content_hash, content, snapshot_path, fetched_at, created_at)
            VALUES (?, 'job_1', ?, ?, 'A', 'hospital_directory', 'hospital', 200, ?, '', '', ?, ?)
            """,
            (source_document_id, source_url, source_url, f"hash:{source_document_id}", now, now),
        )
        con.execute(
            """
            INSERT INTO extracted_records
            (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
             credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
             license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
             waitlist_notes, referral_requirements, evidence_json, created_at)
            VALUES (?, ?, ?, 'A', 'hospital_directory', 'hospital', '', '', '', ?, 'https://rwjbh.org/request', '(973) 555-0112', '',
                    '', 'West Orange', 'NJ', '', 'Edison-New Brunswick', 'NJ', 'unknown', 'unknown', 'yes', 'unclear', '["child"]',
                    'unknown', 'Accepts major plans', '', 'Referral required', '[]', ?)
            """,
            (extracted_id, source_document_id, source_url, practice_name, now),
        )
        con.execute(
            """
            INSERT INTO review_queue
            (review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at)
            VALUES (?, '', 'missing_provider', '', ?, 'Practice offers evaluations but no named clinician was verified.', ?, '', 'pending', ?)
            """,
            (review_id, practice_name, source_url, now),
        )
    con.commit()


def _seed_review_only_signal_for_existing_account(con: sqlite3.Connection) -> None:
    now = "2026-03-09T00:00:00Z"
    source_url = "https://practice.example/autism-evaluations"
    con.execute(
        """
        INSERT INTO source_documents
        (source_document_id, crawl_job_pk, source_url, normalized_url, source_tier, source_type, extraction_profile,
         status_code, content_hash, content, snapshot_path, fetched_at, created_at)
        VALUES ('src_review_existing', 'job_1', ?, ?, 'B', 'practice_page', 'practice', 200, 'hash:src_review_existing', '', '', ?, ?)
        """,
        (source_url, source_url, now, now),
    )
    con.execute(
        """
        INSERT INTO extracted_records
        (extracted_id, source_document_id, source_url, source_tier, source_type, extraction_profile, provider_name,
         credentials, npi, practice_name, intake_url, phone, fax, address_1, city, state, zip, metro, license_state,
         license_type, license_status, diagnoses_asd, diagnoses_adhd, age_groups_json, telehealth, insurance_notes,
         waitlist_notes, referral_requirements, evidence_json, created_at)
        VALUES ('ext_review_existing', 'src_review_existing', ?, 'B', 'practice_page', 'practice', '', '', '', 'Garden State Psychology Center',
                'https://practice.example/intake', '(973) 555-0112', '', '', 'Newark', 'NJ', '', 'Newark', 'NJ', 'unknown',
                'unknown', 'yes', 'yes', '["child"]', 'unknown', 'Accepts major insurance', '', 'Referral preferred', '[]', ?)
        """,
        (source_url, now),
    )
    con.execute(
        """
        INSERT INTO review_queue
        (review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at)
        VALUES ('rev_existing_account', '', 'missing_provider', '', 'Garden State Psychology Center',
                'Service-level evidence is present, but no named clinician is verified yet.', ?, '', 'queued', ?)
        """,
        (source_url, now),
    )
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
        sales_path = Path(str(report["sales_report_csv"]))
        dossiers_path = Path(str(report["dossiers_csv"]))
        assert records_path.exists()
        assert review_path.exists()
        assert sales_path.exists()
        assert dossiers_path.exists()
        assert Path(str(report["profiles_dir"])) .exists()
        assert Path(str(report["outreach_dir"])).exists()
        assert Path(str(report["dossiers_dir"])).exists()
        with records_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        with sales_path.open(newline="", encoding="utf-8") as handle:
            sales_rows = list(csv.DictReader(handle))
        with dossiers_path.open(newline="", encoding="utf-8") as handle:
            dossier_rows = list(csv.DictReader(handle))
        assert len(rows) == 1
        assert len(sales_rows) == 1
        assert len(dossier_rows) == 1
        assert rows[0]["record_id"] == "rec_1"
        assert rows[0]["provider_id"] == "prov_1"
        assert rows[0]["provider_id"] != rows[0]["record_id"]
        assert rows[0]["provider_name"] == "Jane Smith"
        assert rows[0]["prescriptive_authority"] == "no"
        assert rows[0]["diagnoses_asd"] == "yes"
        assert rows[0]["diagnoses_adhd"] == "yes"
        assert rows[0]["outreach_ready"] == "1"
        records_json = Path(str(report["records_json"]))
        exported_records = json.loads(records_json.read_text(encoding="utf-8"))
        assert exported_records[0]["record_id"] == "rec_1"
        assert exported_records[0]["provider_id"] == "prov_1"
        assert exported_records[0]["provider_id"] != exported_records[0]["record_id"]
        assert sales_rows[0]["target_buyer"] == "clinical director or practice owner"
        dossier_md = Path(dossier_rows[0]["dossier_markdown"])
        dossier_profiles_dir = Path(dossier_rows[0]["profiles_dir"])
        assert dossier_md.exists()
        assert dossier_profiles_dir.exists()
        assert any(dossier_profiles_dir.glob("*.md"))
        dossier_text = dossier_md.read_text(encoding="utf-8")
        assert "## Company Snapshot" in dossier_text
        assert "## Decision Network Matrix" in dossier_text
        assert "## Contact Playbook" in dossier_text
        assert "## Recommended Sequence" in dossier_text
        assert "## Method & Files" in dossier_text


def test_export_routes_review_only_pages_to_internal_review_outputs() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "provider_intel.db"
        out_dir = root / "out"
        con = _connect_with_schema(db_path)
        _seed_review_only_account(con)
        report = export_provider_intel(con, out_dir, "run-review-only", limit=10)
        con.close()

        dossiers_path = Path(str(report["dossiers_csv"]))
        with dossiers_path.open(newline="", encoding="utf-8") as handle:
            dossier_rows = list(csv.DictReader(handle))
        assert dossier_rows == []

        internal_review_path = Path(str(report["internal_review_csv"]))
        with internal_review_path.open(newline="", encoding="utf-8") as handle:
            review_rows = list(csv.DictReader(handle))
        assert len(review_rows) == 1
        assert "RWJBarnabas" in review_rows[0]["practice_name"]
        summary_md = Path(review_rows[0]["summary_markdown"])
        summary_text = summary_md.read_text(encoding="utf-8")
        assert "# Internal Review Account Summary" in summary_text
        assert "Frequently Asked Questions" not in summary_text
        assert "Development Evaluations" in summary_text or "Assessment & Evaluation" in summary_text
        assert "Service-level evidence is present, but no named clinician is verified yet." in summary_text


def test_export_dossiers_ignore_review_only_material_for_approved_accounts() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "provider_intel.db"
        out_dir = root / "out"
        con = _connect_with_schema(db_path)
        _seed_record(con)
        _seed_review_only_signal_for_existing_account(con)
        assert run_score(con) == 1
        qa_result = run_qa(con)
        assert qa_result["approved_records"] == 1
        report = export_provider_intel(con, out_dir, "run-mixed", limit=10)
        con.close()

        dossiers_path = Path(str(report["dossiers_csv"]))
        with dossiers_path.open(newline="", encoding="utf-8") as handle:
            dossier_rows = list(csv.DictReader(handle))
        assert len(dossier_rows) == 1
        assert dossier_rows[0]["qa_state"] == "approved_outreach_ready"
        dossier_md = Path(dossier_rows[0]["dossier_markdown"])
        dossier_text = dossier_md.read_text(encoding="utf-8")
        assert "Service-level evidence is present, but no named clinician is verified yet." not in dossier_text
        assert "Internal Review Account Summary" not in dossier_text
        assert "autism-evaluations" not in dossier_text


def main() -> None:
    test_score_qa_and_export_generate_provider_outputs()
    test_export_routes_review_only_pages_to_internal_review_outputs()
    test_export_dossiers_ignore_review_only_material_for_approved_accounts()
    print("test_lead_research: ok")


if __name__ == "__main__":
    main()
