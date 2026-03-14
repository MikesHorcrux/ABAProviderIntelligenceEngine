from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class RuntimePaths:
    tenant_id: str | None
    tenant_root: Path | None
    config_dir: Path
    data_dir: Path
    state_dir: Path
    out_root: Path
    memory_dir: Path
    db_path: Path
    config_path: Path
    checkpoint_dir: Path

    @property
    def provider_out_dir(self) -> Path:
        return self.out_root / "provider_intel"

    @property
    def manifest_path(self) -> Path:
        return self.state_dir / "last_run_manifest.json"

    @property
    def lock_path(self) -> Path:
        return self.state_dir / "run_v4.lock"

    @property
    def fetch_policies_path(self) -> Path:
        return self.config_dir / "fetch_policies.json"

    @property
    def agent_config_path(self) -> Path:
        return self.config_dir / "agent_config.json"

    @property
    def agent_memory_db_path(self) -> Path:
        return self.memory_dir / "agent_memory_v1.db"


@dataclass(frozen=True)
class TenantContext:
    tenant_id: str | None
    tenant_root_base: Path | None
    runtime_paths: RuntimePaths


def _sanitize_tenant_id(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(value or "").strip())
    cleaned = cleaned.strip("-_")
    if not cleaned:
        raise ValueError("Tenant id cannot be empty.")
    return cleaned


def default_runtime_paths() -> RuntimePaths:
    data_dir = ROOT / "data"
    state_dir = data_dir / "state"
    return RuntimePaths(
        tenant_id=None,
        tenant_root=None,
        config_dir=ROOT,
        data_dir=data_dir,
        state_dir=state_dir,
        out_root=ROOT / "out",
        memory_dir=data_dir / "memory",
        db_path=data_dir / "provider_intel_v1.db",
        config_path=ROOT / "crawler_config.json",
        checkpoint_dir=state_dir / "agent_runs",
    )


def tenant_runtime_paths(tenant_id: str, *, tenant_root_base: str | Path | None = None) -> RuntimePaths:
    normalized = _sanitize_tenant_id(tenant_id)
    base = Path(tenant_root_base).expanduser().resolve() if tenant_root_base else (ROOT / "storage" / "tenants")
    tenant_root = base / normalized
    config_dir = tenant_root / "config"
    data_dir = tenant_root / "data"
    state_dir = tenant_root / "state"
    return RuntimePaths(
        tenant_id=normalized,
        tenant_root=tenant_root,
        config_dir=config_dir,
        data_dir=data_dir,
        state_dir=state_dir,
        out_root=tenant_root / "out",
        memory_dir=tenant_root / "memory",
        db_path=data_dir / "provider_intel_v1.db",
        config_path=config_dir / "crawler_config.json",
        checkpoint_dir=state_dir / "agent_runs",
    )


def resolve_runtime_paths(
    *,
    tenant_id: str | None,
    tenant_root_base: str | Path | None = None,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    checkpoint_dir: str | Path | None = None,
) -> RuntimePaths:
    paths = tenant_runtime_paths(tenant_id, tenant_root_base=tenant_root_base) if tenant_id else default_runtime_paths()
    return RuntimePaths(
        tenant_id=paths.tenant_id,
        tenant_root=paths.tenant_root,
        config_dir=paths.config_dir,
        data_dir=paths.data_dir,
        state_dir=paths.state_dir,
        out_root=paths.out_root,
        memory_dir=paths.memory_dir,
        db_path=Path(db_path).expanduser().resolve() if db_path else paths.db_path,
        config_path=Path(config_path).expanduser().resolve() if config_path else paths.config_path,
        checkpoint_dir=Path(checkpoint_dir).expanduser().resolve() if checkpoint_dir else paths.checkpoint_dir,
    )


def build_tenant_context(
    *,
    tenant_id: str | None,
    tenant_root_base: str | Path | None = None,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    checkpoint_dir: str | Path | None = None,
) -> TenantContext:
    paths = resolve_runtime_paths(
        tenant_id=tenant_id,
        tenant_root_base=tenant_root_base,
        db_path=db_path,
        config_path=config_path,
        checkpoint_dir=checkpoint_dir,
    )
    base = Path(tenant_root_base).expanduser().resolve() if tenant_root_base else None
    return TenantContext(tenant_id=paths.tenant_id, tenant_root_base=base, runtime_paths=paths)


def ensure_runtime_dirs(paths: RuntimePaths) -> None:
    for path in (
        paths.config_dir,
        paths.data_dir,
        paths.state_dir,
        paths.checkpoint_dir,
        paths.out_root,
        paths.memory_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
