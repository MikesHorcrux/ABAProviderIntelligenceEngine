#!/usr/bin/env python3.11
from __future__ import annotations

from argparse import Namespace
import io
import json
import sqlite3
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

from cli.app import main as cli_main, make_parser
from cli.sync import execute_export
from pipeline.run_control import ensure_run_control
from pipeline.run_state import create_run_state, save_run_state


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = cli_main(argv)
    payload = json.loads(buf.getvalue())
    return code, payload


def _seed_demo_rows(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    now = "2026-03-08T00:00:00"
    con.execute(
        """
        INSERT OR REPLACE INTO organizations
        (org_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('org_demo', 'Demo Org', 'Demo Org', 'CA', ?, ?, ?, '')
        """,
        (now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO locations
        (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('loc_demo', 'org_demo', 'Demo Dispensary', '', 'Los Angeles', 'CA', '', 'demo.example', '', 55, ?, ?, ?, ?, '')
        """,
        (now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO lead_scores
        (score_pk, location_pk, score_total, tier, run_id, created_at, as_of, deleted_at)
        VALUES ('score_demo', 'loc_demo', 55, 'B', 'demo-run', ?, ?, '')
        """,
        (now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO contacts
        (contact_pk, location_pk, full_name, role, email, phone, source_kind, confidence, verification_status, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('contact_demo', 'loc_demo', 'Jamie Buyer', 'Buyer', 'jamie@example.com', '', 'test', 0.9, 'verified', ?, ?, ?, '')
        """,
        (now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO evidence
        (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
        VALUES ('ev_demo', 'location', 'loc_demo', 'menu_provider', 'dutchie', 'https://demo.example', 'menu provider', ?, '')
        """,
        (now,),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO evidence
        (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
        VALUES ('ev_research_demo', 'location', 'loc_demo', 'agent_research_status', 'research_needed', 'https://demo.example', 'agent research', ?, '')
        """,
        (now,),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO seed_telemetry
        (seed_domain, seed_name, attempts, successes, failures, success_runs, failure_runs, consecutive_failures, last_status_code,
         last_success_at, last_failure_at, last_run_started_at, last_run_completed_at, last_run_status, last_run_pages_fetched,
         last_run_success_pages, last_run_failure_pages, last_run_job_pk, created_at, updated_at, deleted_at)
        VALUES ('blocked.example', 'Blocked Example', 3, 0, 3, 0, 1, 1, 403, '', ?, ?, ?, 'partial', 3, 0, 3, 'job_demo', ?, ?, '')
        """,
        (now, now, now, now, now),
    )
    con.commit()
    con.close()


def test_init_doctor_sql_and_search() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "demo.db"
        config_path = root / "crawler_config.json"
        checkpoint_dir = root / "checkpoints"

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "--config",
                str(config_path),
                "init",
                "--checkpoint-dir",
                str(checkpoint_dir),
            ]
        )
        assert code == 0
        assert payload["ok"] is True
        assert payload["data"]["doctor"]["ok"] is True

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "--config",
                str(config_path),
                "doctor",
                "--checkpoint-dir",
                str(checkpoint_dir),
            ]
        )
        assert code == 0
        assert payload["data"]["ok"] is True

        _seed_demo_rows(db_path)

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "sql",
                "--query",
                "SELECT canonical_name, website_domain FROM locations ORDER BY canonical_name",
            ]
        )
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["canonical_name"] == "Demo Dispensary"

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "sql",
                "--query",
                "DELETE FROM locations",
            ]
        )
        assert code == 13
        assert payload["error"]["code"] == "data_validation_error"

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "search",
                "--preset",
                "failed-domains",
            ]
        )
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["seed_domain"] == "blocked.example"

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "search",
                "Demo",
            ]
        )
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["company_name"] == "Demo Dispensary"

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "search",
                "--preset",
                "research-needed",
            ]
        )
        assert code == 0
        assert payload["data"]["row_count"] == 1
        assert payload["data"]["rows"][0]["research_status"] == "research_needed"

        state = create_run_state(
            run_id="demo-run",
            command="sync",
            db_path=str(db_path),
            config_path=str(config_path),
            seeds_path="seeds.csv",
            crawl_mode="growth",
            options={"seed_limit": 1},
        )
        save_run_state(state, checkpoint_dir)
        ensure_run_control("demo-run", checkpoint_dir)

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "control",
                "--run-id",
                "demo-run",
                "--checkpoint-dir",
                str(checkpoint_dir),
                "quarantine-seed",
                "--domain",
                "demo.example",
                "--reason",
                "agent_test",
            ]
        )
        assert code == 0
        assert "demo.example" in payload["data"]["quarantined_domains"]

        code, payload = _run_cli(
            [
                "--json",
                "--db",
                str(db_path),
                "control",
                "--run-id",
                "demo-run",
                "--checkpoint-dir",
                str(checkpoint_dir),
                "show",
            ]
        )
        assert code == 0
        assert payload["data"]["run_id"] == "demo-run"
        assert "demo.example" in payload["data"]["quarantined_domains"]


def test_json_usage_error_envelope() -> None:
    code, payload = _run_cli(["--json", "sync", "--crawl-mode", "invalid"])
    assert code == 2
    assert payload["ok"] is False
    assert payload["error"]["code"] == "usage_error"


def test_execute_export_supports_intelligence_kind() -> None:
    args = make_parser().parse_args(["export", "--kind", "intelligence", "--limit", "7", "--tier", "B"])
    assert args.kind == "intelligence"

    class DummyRunner:
        def __init__(self, **_kwargs):
            pass

        def run_export(self, **kwargs):
            return {
                "research": "",
                "agent_research": "",
                "intelligence": {"row_count": kwargs.get("intelligence_limit")},
                "echo": kwargs,
            }

    payload = execute_export(
        Namespace(
            kind="intelligence",
            tier="B",
            limit=7,
            research_limit=0,
            agent_research_limit=0,
            since=None,
            new_limit=0,
            signal_limit=0,
            db="demo.db",
        ),
        runner_factory=DummyRunner,
    )
    assert payload["intelligence"]["row_count"] == 7
    assert payload["echo"]["limit"] == 0
    assert payload["echo"]["intelligence_limit"] == 7


def run() -> None:
    test_init_doctor_sql_and_search()
    test_json_usage_error_envelope()
    test_execute_export_supports_intelligence_kind()
    print("test_agent_cli: ok")


if __name__ == "__main__":
    run()
