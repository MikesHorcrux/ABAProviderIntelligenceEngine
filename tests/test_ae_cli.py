#!/usr/bin/env python3.11
from __future__ import annotations

import io
import json
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

import cli.ae as ae_cli
from cli.ae import main as ae_main


def _run_ae(argv: list[str]) -> tuple[int, dict[str, object]]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        code = ae_main(argv)
    return code, json.loads(buf.getvalue())


def test_ae_init_passes_through_to_canonical_cli() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "provider_intel.db"
        config_path = root / "crawler_config.json"
        checkpoint_dir = root / "checkpoints"

        code, payload = _run_ae(
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
        assert payload["command"] == "init"


def test_ae_run_translates_to_agent_run() -> None:
    captured: list[list[str]] = []
    original = ae_cli.canonical_main

    def fake_canonical_main(argv: list[str]) -> int:
        captured.append(list(argv))
        print(json.dumps({"schema_version": "provider_intel.cli.v1", "command": "agent", "ok": True, "message": "ok", "data": {}}))
        return 0

    ae_cli.canonical_main = fake_canonical_main
    try:
        code, payload = _run_ae(["--json", "--tenant", "tenant-a", "run", "Run", "a", "bounded", "loop"])
    finally:
        ae_cli.canonical_main = original

    assert code == 0
    assert payload["ok"] is True
    assert captured == [["--tenant", "tenant-a", "--json", "agent", "run", "--goal", "Run a bounded loop"]]


def test_ae_session_resume_translates_to_agent_resume() -> None:
    captured: list[list[str]] = []
    original = ae_cli.canonical_main

    def fake_canonical_main(argv: list[str]) -> int:
        captured.append(list(argv))
        print(json.dumps({"schema_version": "provider_intel.cli.v1", "command": "agent", "ok": True, "message": "ok", "data": {}}))
        return 0

    ae_cli.canonical_main = fake_canonical_main
    try:
        code, payload = _run_ae(["--json", "--tenant", "tenant-a", "session-resume", "--session-id", "sess_123", "--model", "gpt-5"])
    finally:
        ae_cli.canonical_main = original

    assert code == 0
    assert payload["ok"] is True
    assert captured == [["--tenant", "tenant-a", "--json", "agent", "resume", "--session-id", "sess_123", "--model", "gpt-5"]]


def test_ae_run_with_trace_translates_trace_flag() -> None:
    captured: list[list[str]] = []
    original = ae_cli.canonical_main

    def fake_canonical_main(argv: list[str]) -> int:
        captured.append(list(argv))
        print(json.dumps({"schema_version": "provider_intel.cli.v1", "command": "agent", "ok": True, "message": "ok", "data": {}}))
        return 0

    ae_cli.canonical_main = fake_canonical_main
    try:
        code, payload = _run_ae(["--json", "--tenant", "tenant-a", "run", "--trace", "Watch", "the", "loop"])
    finally:
        ae_cli.canonical_main = original

    assert code == 0
    assert payload["ok"] is True
    assert captured == [["--tenant", "tenant-a", "--json", "agent", "run", "--goal", "Watch the loop", "--trace"]]


def test_ae_run_requires_tenant() -> None:
    code, payload = _run_ae(["--json", "run", "Bounded", "test"])
    assert code == 2
    assert payload["ok"] is False
    assert payload["error"]["code"] == "usage_error"


def main() -> None:
    test_ae_init_passes_through_to_canonical_cli()
    test_ae_run_translates_to_agent_run()
    test_ae_session_resume_translates_to_agent_resume()
    test_ae_run_with_trace_translates_trace_flag()
    test_ae_run_requires_tenant()
    print("test_ae_cli: ok")


if __name__ == "__main__":
    main()
