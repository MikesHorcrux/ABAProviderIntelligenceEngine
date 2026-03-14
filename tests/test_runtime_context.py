#!/usr/bin/env python3.11
from __future__ import annotations

import io
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

from cli.app import main as cli_main
from runtime_context import default_runtime_paths, resolve_runtime_paths


ROOT = Path(__file__).resolve().parents[1]


def test_default_runtime_paths_match_legacy_layout() -> None:
    paths = default_runtime_paths()
    assert paths.tenant_id is None
    assert paths.db_path == ROOT / "data" / "provider_intel_v1.db"
    assert paths.config_path == ROOT / "crawler_config.json"
    assert paths.checkpoint_dir == ROOT / "data" / "state" / "agent_runs"
    assert paths.provider_out_dir == ROOT / "out" / "provider_intel"


def test_resolve_runtime_paths_scopes_tenant_roots_and_respects_overrides() -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td).resolve()
        paths = resolve_runtime_paths(
            tenant_id="Acme Health",
            tenant_root_base=base,
            db_path=base / "custom.db",
        )
        assert paths.tenant_id == "acme-health"
        assert paths.tenant_root == base / "acme-health"
        assert paths.config_path == base / "acme-health" / "config" / "crawler_config.json"
        assert paths.checkpoint_dir == base / "acme-health" / "state" / "agent_runs"
        assert paths.provider_out_dir == base / "acme-health" / "out" / "provider_intel"
        assert paths.db_path == base / "custom.db"


def test_cli_init_uses_tenant_runtime_defaults() -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td).resolve()
        with redirect_stdout(io.StringIO()):
            code = cli_main(["--json", "--tenant", "tenant-one", "--tenant-root-base", str(base), "init"])
        assert code == 0
        tenant_root = base / "tenant-one"
        assert (tenant_root / "config" / "crawler_config.json").exists()
        assert (tenant_root / "data" / "provider_intel_v1.db").exists()
        assert (tenant_root / "state" / "agent_runs").exists()


def main() -> None:
    test_default_runtime_paths_match_legacy_layout()
    test_resolve_runtime_paths_scopes_tenant_roots_and_respects_overrides()
    test_cli_init_uses_tenant_runtime_defaults()
    print("test_runtime_context: ok")


if __name__ == "__main__":
    main()
