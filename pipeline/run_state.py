from __future__ import annotations

import json
from pathlib import Path

from pipeline.stages.discovery import DiscoverySeed
from pipeline.utils import utcnow_iso


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_STATE_DIR = ROOT / "data" / "state" / "agent_runs"
STAGE_ORDER = ("seed_ingest", "crawl", "extract", "resolve", "score", "qa", "export")
RUN_STATE_SCHEMA_VERSION = "provider_intel.run_state.v1"


def ensure_run_state_dir(base_dir: str | Path | None = None) -> Path:
    path = Path(base_dir) if base_dir else DEFAULT_RUN_STATE_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_state_path(run_id: str, base_dir: str | Path | None = None) -> Path:
    return ensure_run_state_dir(base_dir) / f"run_{run_id}.json"


def serialize_seed(seed: DiscoverySeed) -> dict[str, object]:
    return {
        "name": seed.name,
        "website": seed.website,
        "state": seed.state,
        "market": seed.market,
        "source": seed.source,
        "priority": seed.priority,
        "tier": seed.tier,
        "source_type": seed.source_type,
        "browser_required": seed.browser_required,
        "extraction_profile": seed.extraction_profile,
        "metadata": dict(seed.metadata),
    }


def deserialize_seed(payload: dict[str, object]) -> DiscoverySeed:
    return DiscoverySeed(
        name=str(payload.get("name") or ""),
        website=str(payload.get("website") or ""),
        state=str(payload.get("state") or ""),
        market=str(payload.get("market") or ""),
        source=str(payload.get("source") or "seed_file"),
        priority=int(payload.get("priority") or 0),
        tier=str(payload.get("tier") or ""),
        source_type=str(payload.get("source_type") or ""),
        browser_required=bool(payload.get("browser_required") or False),
        extraction_profile=str(payload.get("extraction_profile") or ""),
        metadata=dict(payload.get("metadata") or {}),
    )


def deserialize_seeds(payloads: list[dict[str, object]] | None) -> list[DiscoverySeed]:
    return [deserialize_seed(item) for item in (payloads or [])]


def create_run_state(
    *,
    run_id: str,
    command: str,
    db_path: str,
    config_path: str,
    seeds_path: str,
    crawl_mode: str,
    options: dict[str, object],
) -> dict[str, object]:
    now = utcnow_iso()
    return {
        "schema_version": RUN_STATE_SCHEMA_VERSION,
        "run_id": run_id,
        "command": command,
        "status": "running",
        "created_at": now,
        "updated_at": now,
        "started_at": now,
        "completed_at": "",
        "db_path": db_path,
        "config_path": config_path,
        "seeds_path": seeds_path,
        "crawl_mode": crawl_mode,
        "options": dict(options),
        "recovery_pointer": STAGE_ORDER[0],
        "seed_counts": {"discovery": 0, "monitor": 0},
        "seed_intake": {},
        "governor": {},
        "export_since": "",
        "discovery_seeds": [],
        "monitoring_seeds": [],
        "summary": {},
        "report": {},
        "last_error": {},
        "stages": {
            stage: {
                "status": "pending",
                "started_at": "",
                "completed_at": "",
                "details": {},
            }
            for stage in STAGE_ORDER
        },
    }


def load_run_state(run_id: str, base_dir: str | Path | None = None) -> dict[str, object]:
    path = run_state_path(run_id, base_dir)
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def latest_run_state(base_dir: str | Path | None = None) -> dict[str, object] | None:
    state_dir = ensure_run_state_dir(base_dir)
    candidates = sorted(state_dir.glob("run_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    with candidates[0].open(encoding="utf-8") as f:
        return json.load(f)


def save_run_state(state: dict[str, object], base_dir: str | Path | None = None) -> Path:
    path = run_state_path(str(state["run_id"]), base_dir)
    state["updated_at"] = utcnow_iso()
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return path


def next_stage(state: dict[str, object]) -> str | None:
    stages = state.get("stages", {})
    for stage in STAGE_ORDER:
        payload = stages.get(stage, {})
        if payload.get("status") != "completed":
            return stage
    return None


def update_recovery_pointer(state: dict[str, object]) -> None:
    stage = next_stage(state)
    state["recovery_pointer"] = stage or "done"


def mark_stage_started(state: dict[str, object], stage: str, details: dict[str, object] | None = None) -> None:
    stage_state = state["stages"][stage]
    stage_state["status"] = "running"
    if not stage_state.get("started_at"):
        stage_state["started_at"] = utcnow_iso()
    if details:
        stage_state["details"] = {**stage_state.get("details", {}), **details}
    update_recovery_pointer(state)


def mark_stage_completed(state: dict[str, object], stage: str, details: dict[str, object] | None = None) -> None:
    stage_state = state["stages"][stage]
    if not stage_state.get("started_at"):
        stage_state["started_at"] = utcnow_iso()
    stage_state["status"] = "completed"
    stage_state["completed_at"] = utcnow_iso()
    if details:
        stage_state["details"] = {**stage_state.get("details", {}), **details}
    update_recovery_pointer(state)


def mark_stage_failed(
    state: dict[str, object],
    stage: str,
    *,
    code: str,
    message: str,
    details: dict[str, object] | None = None,
) -> None:
    stage_state = state["stages"][stage]
    if not stage_state.get("started_at"):
        stage_state["started_at"] = utcnow_iso()
    stage_state["status"] = "failed"
    stage_state["completed_at"] = utcnow_iso()
    stage_state["details"] = {
        **stage_state.get("details", {}),
        **(details or {}),
        "error_code": code,
        "error_message": message,
    }
    state["status"] = "failed"
    state["completed_at"] = utcnow_iso()
    state["last_error"] = {
        "stage": stage,
        "code": code,
        "message": message,
        "details": details or {},
        "captured_at": utcnow_iso(),
    }
    update_recovery_pointer(state)


def mark_run_completed(
    state: dict[str, object],
    *,
    summary: dict[str, object],
    report: dict[str, object],
) -> None:
    state["status"] = "completed"
    state["completed_at"] = utcnow_iso()
    state["summary"] = dict(summary)
    state["report"] = dict(report)
    update_recovery_pointer(state)
