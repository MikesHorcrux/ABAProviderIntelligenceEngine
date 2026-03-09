from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import cli.query as query_module
from agent_runtime.contracts import QAGateMetrics, QAGateThresholds
from agent_runtime.qa import evaluate_qa_gates
from agent_runtime.router import load_agent_runtime_config, select_provider_for_role


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _connect_with_schema(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def test_provider_router_selects_by_role_then_fallback() -> None:
    with tempfile.TemporaryDirectory() as td:
        config_path = Path(td) / "agent_runtime.json"
        config_path.write_text(
            json.dumps(
                {
                    "enabled": True,
                    "provider_modes": {
                        "openai_api": {"available": True},
                        "codex_auth": {"available": False},
                        "clawbot": {"available": True},
                    },
                    "model_role_slots": {
                        "summarize": {
                            "model": "gpt-4.1-mini",
                            "preferred_providers": ["codex_auth", "openai_api"],
                        },
                        "research": {
                            "model": "gpt-4.1",
                            "preferred_providers": ["codex_auth"],
                        },
                        "writer": {
                            "model": "gpt-4.1",
                            "preferred_providers": ["openai_api"],
                        },
                        "qa": {
                            "model": "gpt-4.1-mini",
                            "preferred_providers": ["codex_auth"],
                        },
                    },
                    "fallback_order": ["clawbot", "openai_api"],
                    "qa_thresholds": {
                        "min_sources": 1,
                        "min_signals": 2,
                        "min_contact_coverage_pct": 30.0,
                    },
                }
            ),
            encoding="utf-8",
        )
        config = load_agent_runtime_config(config_path)
        summarize_choice = select_provider_for_role("summarize", config)
        qa_choice = select_provider_for_role("qa", config)

        assert summarize_choice.provider_mode == "openai_api"
        assert summarize_choice.model == "gpt-4.1-mini"
        assert summarize_choice.attempted_order == ("codex_auth", "openai_api", "clawbot")
        assert qa_choice.provider_mode == "clawbot"
        assert qa_choice.attempted_order == ("codex_auth", "clawbot", "openai_api")


def test_qa_gate_evaluator_pass_and_fail() -> None:
    thresholds = QAGateThresholds(min_sources=2, min_signals=3, min_contact_coverage_pct=50.0)
    passing = evaluate_qa_gates(
        QAGateMetrics(source_count=2, signal_count=4, contact_coverage_pct=75.0),
        thresholds,
    )
    failing = evaluate_qa_gates(
        QAGateMetrics(source_count=1, signal_count=2, contact_coverage_pct=40.0),
        thresholds,
    )

    assert passing.passed is True
    assert passing.failures == ()
    assert failing.passed is False
    assert set(failing.failures) == {
        "sources_below_min",
        "signals_below_min",
        "contact_coverage_below_min",
    }


def test_status_payload_includes_agent_runtime_block() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "status.db"
        state_dir = root / "data" / "state"
        out_dir = root / "out"
        config_path = root / "config" / "agent_runtime.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "enabled": True,
                    "provider_modes": {
                        "openai_api": {"available": True},
                        "codex_auth": {"available": True},
                        "clawbot": {"available": False},
                    },
                    "qa_thresholds": {
                        "min_sources": 3,
                        "min_signals": 4,
                        "min_contact_coverage_pct": 60.0,
                    },
                }
            ),
            encoding="utf-8",
        )
        _connect_with_schema(db_path).close()
        state_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / "last_run_manifest.json").write_text("{}", encoding="utf-8")
        (state_dir / "run_v4.lock").write_text("", encoding="utf-8")

        original_manifest_path = query_module.MANIFEST_PATH
        original_lock_path = query_module.LOCK_PATH
        original_out_dir = query_module.OUT_DIR
        original_agent_config = query_module.AGENT_RUNTIME_CONFIG_PATH
        query_module.MANIFEST_PATH = state_dir / "last_run_manifest.json"
        query_module.LOCK_PATH = state_dir / "run_v4.lock"
        query_module.OUT_DIR = out_dir
        query_module.AGENT_RUNTIME_CONFIG_PATH = config_path
        try:
            payload = query_module.run_status(db_path=str(db_path), run_id=None, run_state_dir=None)
        finally:
            query_module.MANIFEST_PATH = original_manifest_path
            query_module.LOCK_PATH = original_lock_path
            query_module.OUT_DIR = original_out_dir
            query_module.AGENT_RUNTIME_CONFIG_PATH = original_agent_config

        block = payload["agent_runtime"]
        assert block["enabled"] is True
        assert block["config_path"] == str(config_path.resolve())
        assert block["provider_modes_available"] == ["openai_api", "codex_auth"]
        assert block["qa_thresholds"]["min_sources"] == 3
        assert block["qa_thresholds"]["min_signals"] == 4
        assert block["qa_thresholds"]["min_contact_coverage_pct"] == 60.0
        assert block["last_error"] == ""
