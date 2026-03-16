#!/usr/bin/env python3.11
from __future__ import annotations

import io
import json
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

import cli.app as cli_app
from cli.app import main as cli_main


ROOT = Path(__file__).resolve().parents[1]


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = cli_main(argv)
    return code, json.loads(buf.getvalue())


def _assert_schema_shape(payload: dict[str, object], schema_path: Path) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == schema["properties"]["schema_version"]["const"]
    assert payload["command"] == schema["properties"]["command"]["const"]
    _assert_object(payload, schema)


def _assert_object(payload: dict[str, object], schema: dict[str, object]) -> None:
    required = list(schema.get("required", []))
    properties = dict(schema.get("properties", {}))
    for key in required:
        assert key in payload, f"missing required key {key}"
    for key, subschema in properties.items():
        if key not in payload:
            continue
        value = payload[key]
        expected_type = subschema.get("type")
        if expected_type == "object":
            assert isinstance(value, dict), f"{key} should be an object"
            _assert_object(value, subschema)
        elif expected_type == "array":
            assert isinstance(value, list), f"{key} should be an array"


def test_status_contract_matches_schema() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "provider_intel.db"
        config_path = root / "crawler_config.json"
        checkpoint_dir = root / "checkpoints"

        code, payload = _run_cli(["--json", "--db", str(db_path), "--config", str(config_path), "init", "--checkpoint-dir", str(checkpoint_dir)])
        assert code == 0
        assert payload["ok"] is True

        code, payload = _run_cli(["--json", "--db", str(db_path), "status", "--checkpoint-dir", str(checkpoint_dir)])
        assert code == 0
        _assert_schema_shape(payload, ROOT / "docs" / "schemas" / "cli" / "v1" / "status.json")


def test_agent_run_contract_matches_schema() -> None:
    original = cli_app.execute_agent_run

    def fake_execute_agent_run(args):  # noqa: ANN001
        del args
        return {
            "tenant_id": "tenant-a",
            "session_id": "sess_123",
            "goal": "Run a bounded operator loop",
            "tools_used": ["doctor", "sync", "status"],
            "run_ids": ["run_123"],
            "exports": [{"key": "records_csv", "path": "/tmp/records.csv"}],
            "unresolved_risks": ["1 review item remains."],
            "recommended_next_actions": ["Inspect the review queue."],
            "memory_updates": {"run_memory": ["run_123"], "domain_tactics": [], "client_profile_used": "default"},
        }

    cli_app.execute_agent_run = fake_execute_agent_run
    try:
        code, payload = _run_cli(["--json", "--tenant", "tenant-a", "agent", "run", "--goal", "Run a bounded operator loop"])
    finally:
        cli_app.execute_agent_run = original

    assert code == 0
    _assert_schema_shape(payload, ROOT / "docs" / "schemas" / "cli" / "v1" / "agent_run.json")


def test_agent_status_works_without_model_credentials() -> None:
    with tempfile.TemporaryDirectory() as td:
        code, payload = _run_cli(["--json", "--tenant", "tenant-a", "--tenant-root-base", td, "agent", "status"])
        assert code == 0
        assert payload["data"]["tenant_id"] == "tenant-a"
        _assert_schema_shape(payload, ROOT / "docs" / "schemas" / "cli" / "v1" / "agent_status.json")


def main() -> None:
    test_status_contract_matches_schema()
    test_agent_run_contract_matches_schema()
    test_agent_status_works_without_model_credentials()
    print("test_cli_contracts: ok")


if __name__ == "__main__":
    main()
