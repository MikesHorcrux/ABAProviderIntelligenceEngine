#!/usr/bin/env python3.11
from __future__ import annotations

import io
import json
import sqlite3
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

from cli.app import main as cli_main
from pipeline.run_control import ensure_run_control
from pipeline.run_state import create_run_state, save_run_state


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = cli_main(argv)
    payload = json.loads(buf.getvalue())
    return code, payload


def _seed_demo_rows(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    now = "2026-03-09T00:00:00Z"
    con.execute("INSERT INTO providers(provider_id, provider_name, credentials, npi, primary_license_state, primary_license_type, created_at, updated_at) VALUES ('prov_demo', 'Jane Smith', 'PsyD', '', 'NJ', 'psychologist', ?, ?)", (now, now))
    con.execute("INSERT INTO practices(practice_id, practice_name, website, intake_url, phone, fax, created_at, updated_at) VALUES ('prac_demo', 'Garden State Psychology Center', 'https://practice.example', 'https://practice.example/intake', '(973) 555-0112', '', ?, ?)", (now, now))
    con.execute("INSERT INTO practice_locations(location_id, practice_id, address_1, city, state, zip, metro, phone, telehealth, created_at, updated_at) VALUES ('loc_demo', 'prac_demo', '', 'Newark', 'NJ', '', 'Newark', '(973) 555-0112', 'yes', ?, ?)", (now, now))
    con.execute("INSERT INTO provider_practice_records(record_id, provider_id, practice_id, location_id, provider_name_snapshot, practice_name_snapshot, npi, license_state, license_type, license_status, diagnoses_asd, diagnoses_adhd, prescriptive_authority, prescriptive_basis, age_groups_json, telehealth, insurance_notes, waitlist_notes, referral_requirements, source_urls_json, field_confidence_json, record_confidence, conflict_note, review_status, export_status, blocked_reason, last_verified_at, created_at, updated_at) VALUES ('rec_demo', 'prov_demo', 'prac_demo', 'loc_demo', 'Jane Smith', 'Garden State Psychology Center', '', 'NJ', 'psychologist', 'active', 'yes', 'yes', 'no', 'Psychologists in New Jersey do not prescribe.', '[\"child\"]', 'yes', 'Accepts major insurance', '', 'Referral preferred', '[\"https://practice.example/providers\"]', '{\"diagnoses_asd\":0.8,\"diagnoses_adhd\":0.8,\"license_status\":0.95,\"prescriptive_authority\":0.95}', 0.875, '', 'ready', 'approved', '', ?, ?, ?)", (now, now, now))
    con.execute("UPDATE provider_practice_records SET outreach_fit_score=0.91, outreach_ready=1, outreach_reasons_json='[\"explicit_asd_diagnostic_signal\",\"active_license\",\"public_contact_channel\"]' WHERE record_id='rec_demo'")
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_demo_asd', 'rec_demo', 'diagnoses_asd', 'yes', 'autism diagnostic evaluations', 'https://practice.example/providers', 'src_demo', 'C', ?)", (now,))
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_demo_adhd', 'rec_demo', 'diagnoses_adhd', 'yes', 'ADHD assessment', 'https://practice.example/providers', 'src_demo', 'C', ?)", (now,))
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_demo_lic', 'rec_demo', 'license_status', 'active', 'License status: active', 'https://www.njconsumeraffairs.gov/psy/Pages/default.aspx', 'src_demo', 'A', ?)", (now,))
    con.execute("INSERT INTO field_evidence(evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at) VALUES ('ev_demo_rx', 'rec_demo', 'prescriptive_authority', 'no', 'Psychologists in New Jersey do not prescribe.', 'https://www.njconsumeraffairs.gov/psy/Pages/default.aspx', 'src_demo', 'A', ?)", (now,))
    con.execute("INSERT INTO seed_telemetry(seed_domain, seed_name, attempts, successes, failures, success_runs, failure_runs, consecutive_failures, last_status_code, last_success_at, last_failure_at, last_run_started_at, last_run_completed_at, last_run_status, last_run_pages_fetched, last_run_success_pages, last_run_failure_pages, last_run_job_pk, created_at, updated_at) VALUES ('blocked.example', 'Blocked Example', 3, 0, 3, 0, 1, 1, 403, '', ?, ?, ?, 'partial', 3, 0, 3, 'job_demo', ?, ?)", (now, now, now, now, now))
    con.execute("INSERT INTO review_queue(review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at) VALUES ('rev_demo', 'rec_demo', 'record_review', 'Jane Smith', 'Garden State Psychology Center', 'manual_check', 'https://practice.example/providers', 'autism diagnostic evaluations', 'pending', ?)", (now,))
    con.commit()
    con.close()


def test_init_doctor_sql_search_status_and_export() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "provider_intel.db"
        config_path = root / "crawler_config.json"
        checkpoint_dir = root / "checkpoints"

        code, payload = _run_cli(["--json", "--db", str(db_path), "--config", str(config_path), "init", "--checkpoint-dir", str(checkpoint_dir)])
        assert code == 0
        assert payload["ok"] is True
        assert payload["data"]["doctor"]["ok"] is True

        _seed_demo_rows(db_path)

        code, payload = _run_cli(["--json", "--db", str(db_path), "sql", "--query", "SELECT provider_name_snapshot, practice_name_snapshot FROM provider_practice_records;"])
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["provider_name_snapshot"] == "Jane Smith"

        code, payload = _run_cli(["--json", "--db", str(db_path), "sql", "--query", "PRAGMA table_info(provider_practice_records)"])
        assert code == 0
        assert payload["data"]["row_count"] > 0
        assert any(row["name"] == "record_id" for row in payload["data"]["rows"])

        code, payload = _run_cli(["--json", "--db", str(db_path), "sql", "--query", "SELECT created_at FROM review_queue LIMIT 1;"])
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["created_at"] == "2026-03-09T00:00:00Z"

        code, payload = _run_cli(["--json", "--db", str(db_path), "search", "Jane"])
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["provider_name"] == "Jane Smith"

        code, payload = _run_cli(["--json", "--db", str(db_path), "search", "--preset", "blocked-domains"])
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["seed_domain"] == "blocked.example"

        code, payload = _run_cli(["--json", "--db", str(db_path), "search", "--preset", "outreach-ready"])
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["provider_name"] == "Jane Smith"

        state = create_run_state(
            run_id="demo-run",
            command="sync",
            db_path=str(db_path),
            config_path=str(config_path),
            seeds_path="seed_packs/nj/seed_pack.json",
            crawl_mode="full",
            options={"seed_limit": 1},
        )
        save_run_state(state, checkpoint_dir)
        ensure_run_control("demo-run", checkpoint_dir)

        code, payload = _run_cli(["--json", "--db", str(db_path), "status", "--run-id", "demo-run", "--checkpoint-dir", str(checkpoint_dir)])
        assert code == 0
        assert payload["data"]["counts"]["records"] == 1
        assert payload["data"]["counts"]["outreach_ready_records"] == 1

        code, payload = _run_cli(["--json", "--db", str(db_path), "export", "--limit", "10"])
        assert code == 0
        assert payload["data"]["record_count"] == 1
        assert Path(str(payload["data"]["records_csv"])).exists()
        assert Path(str(payload["data"]["sales_report_csv"])).exists()


def main() -> None:
    test_init_doctor_sql_search_status_and_export()
    print("test_agent_cli: ok")


if __name__ == "__main__":
    main()
