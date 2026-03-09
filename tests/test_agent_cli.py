#!/usr/bin/env python3.11
from __future__ import annotations

from argparse import Namespace
import io
import json
import sqlite3
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

import cli.query as query_module
from cli.app import main as cli_main, make_parser
from cli.sync import execute_export
from pipeline.run_control import ensure_run_control
from pipeline.run_state import create_run_state, save_run_state
from pipeline.stages.score import run_score


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = cli_main(argv)
    payload = json.loads(buf.getvalue())
    return code, payload


def _connect_with_schema(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def _seed_demo_rows(db_path: Path) -> None:
    con = _connect_with_schema(db_path)
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


def test_json_flag_after_subcommand_matches_documented_usage() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        checkpoint_dir = root / "checkpoints"
        code, payload = _run_cli(
            [
                "--db",
                str(root / "demo.db"),
                "status",
                "--checkpoint-dir",
                str(checkpoint_dir),
                "--json",
            ]
        )
        assert code == 0
        assert payload["ok"] is True
        assert payload["command"] == "status"


def test_run_score_uses_pipeline_run_id_when_provided() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "score.db"
        con = _connect_with_schema(db_path)
        now = "2026-03-08T00:00:00"
        con.execute(
            """
            INSERT OR REPLACE INTO organizations
            (org_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
            VALUES ('org_score', 'Score Org', 'Score Org', 'CA', ?, ?, ?, '')
            """,
            (now, now, now),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO locations
            (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at, last_seen_at, deleted_at)
            VALUES ('loc_score', 'org_score', 'Score Shop', '', 'Los Angeles', 'CA', '', 'score.example', '(555) 000-1111', 0, ?, ?, ?, ?, '')
            """,
            (now, now, now, now),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO contacts
            (contact_pk, location_pk, full_name, role, email, phone, source_kind, confidence, verification_status, created_at, updated_at, last_seen_at, deleted_at)
            VALUES ('contact_score', 'loc_score', 'Pat Buyer', 'Buyer', 'pat@score.example', '', 'test', 0.9, 'verified', ?, ?, ?, '')
            """,
            (now, now, now),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO contact_points
            (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at, created_at, updated_at, deleted_at)
            VALUES
            ('cp_score_email', 'loc_score', 'email', 'pat@score.example', 0.9, 'https://score.example/contact', ?, ?, ?, ?, ''),
            ('cp_score_phone', 'loc_score', 'phone', '(555) 000-1111', 0.9, 'https://score.example/contact', ?, ?, ?, ?, '')
            """,
            (now, now, now, now, now, now, now, now),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO evidence
            (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
            VALUES ('ev_score_menu', 'location', 'loc_score', 'menu_provider', 'dutchie', 'https://score.example/menu', 'menu provider', ?, '')
            """,
            (now,),
        )
        con.commit()

        scores_written = run_score(con, run_id="score-run")
        score_row = con.execute(
            """
            SELECT run_id, score_total, tier
            FROM lead_scores
            WHERE location_pk='loc_score'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        con.close()

        assert scores_written == 1
        assert score_row is not None
        assert score_row["run_id"] == "score-run"
        assert int(score_row["score_total"] or 0) > 0


def test_status_reports_external_research_contract_progress() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "status.db"
        out_dir = root / "out"
        state_dir = root / "data" / "state"
        dossier_dir = out_dir / "lead_intelligence"
        package_dir = dossier_dir / "leads" / "disp001-demo"
        package_dir.mkdir(parents=True, exist_ok=True)
        _connect_with_schema(db_path).close()

        (state_dir / "last_run_manifest.json").parent.mkdir(parents=True, exist_ok=True)
        (state_dir / "last_run_manifest.json").write_text("{}", encoding="utf-8")
        (state_dir / "run_v4.lock").write_text("", encoding="utf-8")
        (dossier_dir / "lead_intelligence_manifest.json").write_text(
            json.dumps(
                {
                    "generated_at": "2026-03-09T00:00:00+00:00",
                    "run_id": "demo-run",
                    "package_count": 1,
                    "external_research_contract_version": "external_research.v1",
                    "packages": [
                        {
                            "lead_id": "DISP001",
                            "company_name": "Demo Dispensary",
                            "package_dir": "leads/disp001-demo",
                            "external_research_status": "leads/disp001-demo/external_research_status.json",
                            "external_research_report": "leads/disp001-demo/external_research_report.md",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        (package_dir / "external_research_status.json").write_text(
            json.dumps(
                {
                    "schema_version": "external_research.v1",
                    "lead_id": "DISP001",
                    "company_name": "Demo Dispensary",
                    "status": "completed",
                    "agent_name": "clawbot",
                    "completed_at": "2026-03-09T00:01:00+00:00",
                    "updated_at": "2026-03-09T00:01:00+00:00",
                    "output_path": "external_research_report.md",
                    "source_count": 4,
                }
            ),
            encoding="utf-8",
        )
        (package_dir / "external_research_report.md").write_text("# External Research\n", encoding="utf-8")

        original_manifest_path = query_module.MANIFEST_PATH
        original_lock_path = query_module.LOCK_PATH
        original_out_dir = query_module.OUT_DIR
        query_module.MANIFEST_PATH = state_dir / "last_run_manifest.json"
        query_module.LOCK_PATH = state_dir / "run_v4.lock"
        query_module.OUT_DIR = out_dir
        try:
            code, payload = _run_cli(
                [
                    "--json",
                    "--db",
                    str(db_path),
                    "status",
                ]
            )
        finally:
            query_module.MANIFEST_PATH = original_manifest_path
            query_module.LOCK_PATH = original_lock_path
            query_module.OUT_DIR = original_out_dir

        assert code == 0
        external = payload["data"]["external_research"]
        assert external["package_count"] == 1
        assert external["completed_count"] == 1
        assert external["completed_with_report_count"] == 1
        assert external["packages"][0]["agent_name"] == "clawbot"
        assert external["packages"][0]["status"] == "completed"
        assert external["packages"][0]["report_exists"] is True


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
    test_json_flag_after_subcommand_matches_documented_usage()
    test_run_score_uses_pipeline_run_id_when_provided()
    test_status_reports_external_research_contract_progress()
    test_execute_export_supports_intelligence_kind()
    print("test_agent_cli: ok")


if __name__ == "__main__":
    run()
