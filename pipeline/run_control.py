from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.run_state import DEFAULT_RUN_STATE_DIR, latest_run_state
from pipeline.utils import normalize_domain, utcnow_iso


RUN_CONTROL_SCHEMA_VERSION = "run_control.v1"
MAX_INTERVENTIONS = 200


def ensure_run_control_dir(base_dir: str | Path | None = None) -> Path:
    path = Path(base_dir) if base_dir else DEFAULT_RUN_STATE_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_control_path(run_id: str, base_dir: str | Path | None = None) -> Path:
    return ensure_run_control_dir(base_dir) / f"control_{run_id}.json"


def _now() -> str:
    return utcnow_iso()


def _empty_domain_control() -> dict[str, Any]:
    return {
        "quarantined": False,
        "quarantine_reason": "",
        "suppressed_path_prefixes": [],
        "max_pages_per_domain": None,
        "stop_requested": False,
        "updated_at": "",
    }


def _empty_domain_runtime() -> dict[str, Any]:
    return {
        "status": "pending",
        "processed_urls": 0,
        "success_pages": 0,
        "failure_pages": 0,
        "filtered_urls": 0,
        "last_status_code": 0,
        "last_error": "",
        "discovery_enabled": True,
        "browser_escalated": False,
        "updated_at": "",
    }


def new_run_control_state(run_id: str) -> dict[str, Any]:
    now = _now()
    return {
        "schema_version": RUN_CONTROL_SCHEMA_VERSION,
        "run_id": run_id,
        "status": "active",
        "created_at": now,
        "updated_at": now,
        "agent_controls": {
            "domains": {},
        },
        "runtime": {
            "current_seed_domain": "",
            "domains": {},
            "interventions": [],
        },
    }


def load_run_control(run_id: str, base_dir: str | Path | None = None) -> dict[str, Any]:
    path = run_control_path(run_id, base_dir)
    if not path.exists():
        return new_run_control_state(run_id)
    with path.open(encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        return new_run_control_state(run_id)
    state = new_run_control_state(run_id)
    state.update(payload)
    state["agent_controls"] = dict(payload.get("agent_controls") or {})
    state["agent_controls"]["domains"] = dict((state["agent_controls"]).get("domains") or {})
    state["runtime"] = dict(payload.get("runtime") or {})
    state["runtime"]["domains"] = dict((state["runtime"]).get("domains") or {})
    interventions = (state["runtime"]).get("interventions") or []
    state["runtime"]["interventions"] = [item for item in interventions if isinstance(item, dict)][-MAX_INTERVENTIONS:]
    return state


def save_run_control(state: dict[str, Any], base_dir: str | Path | None = None) -> Path:
    path = run_control_path(str(state["run_id"]), base_dir)
    state["updated_at"] = _now()
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def ensure_run_control(run_id: str, base_dir: str | Path | None = None) -> dict[str, Any]:
    path = run_control_path(run_id, base_dir)
    if path.exists():
        return load_run_control(run_id, base_dir)
    state = new_run_control_state(run_id)
    save_run_control(state, base_dir)
    return state


def resolve_run_control_id(run_id_or_latest: str | None, base_dir: str | Path | None = None) -> str:
    if run_id_or_latest and run_id_or_latest != "latest":
        return run_id_or_latest

    latest_state = latest_run_state(base_dir)
    if latest_state and latest_state.get("run_id"):
        return str(latest_state["run_id"])

    state_dir = ensure_run_control_dir(base_dir)
    candidates = sorted(state_dir.glob("control_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0].stem.removeprefix("control_")
    raise FileNotFoundError("No run control state is available.")


def domain_control_record(state: dict[str, Any], domain: str) -> dict[str, Any]:
    normalized = normalize_domain(domain)
    domains = state.setdefault("agent_controls", {}).setdefault("domains", {})
    record = dict(domains.get(normalized) or _empty_domain_control())
    domains[normalized] = record
    return record


def domain_runtime_record(state: dict[str, Any], domain: str) -> dict[str, Any]:
    normalized = normalize_domain(domain)
    domains = state.setdefault("runtime", {}).setdefault("domains", {})
    record = dict(domains.get(normalized) or _empty_domain_runtime())
    domains[normalized] = record
    return record


def append_intervention(
    state: dict[str, Any],
    *,
    domain: str,
    action: str,
    reason: str,
    source: str,
    details: dict[str, Any] | None = None,
) -> None:
    interventions = state.setdefault("runtime", {}).setdefault("interventions", [])
    interventions.append(
        {
            "at": _now(),
            "domain": normalize_domain(domain),
            "action": action,
            "reason": reason,
            "source": source,
            "details": details or {},
        }
    )
    del interventions[:-MAX_INTERVENTIONS]


def update_agent_controls(
    run_id: str,
    updater,
    *,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    state = ensure_run_control(run_id, base_dir)
    updater(state)
    save_run_control(state, base_dir)
    return state


def update_runtime_controls(
    run_id: str,
    updater,
    *,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    state = ensure_run_control(run_id, base_dir)
    updater(state)
    save_run_control(state, base_dir)
    return state


def summarize_run_control(state: dict[str, Any]) -> dict[str, Any]:
    runtime = dict(state.get("runtime") or {})
    agent_controls = dict(state.get("agent_controls") or {})
    domains = dict(runtime.get("domains") or {})
    control_domains = dict(agent_controls.get("domains") or {})
    quarantined = [domain for domain, payload in control_domains.items() if payload.get("quarantined")]
    stopped = [domain for domain, payload in control_domains.items() if payload.get("stop_requested")]
    capped = [
        {"domain": domain, "max_pages_per_domain": payload.get("max_pages_per_domain")}
        for domain, payload in control_domains.items()
        if payload.get("max_pages_per_domain") not in (None, "")
    ]
    return {
        "run_id": state.get("run_id"),
        "status": state.get("status"),
        "current_seed_domain": runtime.get("current_seed_domain", ""),
        "runtime_domain_count": len(domains),
        "agent_control_domain_count": len(control_domains),
        "quarantined_domains": quarantined,
        "stopped_domains": stopped,
        "capped_domains": capped,
        "recent_interventions": list(runtime.get("interventions") or [])[-10:],
        "domains": domains,
        "agent_controls": control_domains,
    }
