from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from agent_runtime.contracts import AgentRuntimeConfig, ProviderMode, QAGateThresholds, SelectedProvider, TaskRole


DEFAULT_CONFIG = {
    "enabled": False,
    "provider_modes": {
        "openai_api": {"available": False},
        "codex_auth": {"available": False},
        "clawbot": {"available": False},
    },
    "model_role_slots": {
        "summarize": {"model": "gpt-4.1-mini", "preferred_providers": ["openai_api", "codex_auth", "clawbot"]},
        "research": {"model": "gpt-4.1", "preferred_providers": ["codex_auth", "openai_api", "clawbot"]},
        "writer": {"model": "gpt-4.1", "preferred_providers": ["openai_api", "codex_auth", "clawbot"]},
        "qa": {"model": "gpt-4.1-mini", "preferred_providers": ["codex_auth", "openai_api", "clawbot"]},
    },
    "fallback_order": ["codex_auth", "openai_api", "clawbot"],
    "qa_thresholds": {
        "min_sources": 2,
        "min_signals": 3,
        "min_contact_coverage_pct": 50.0,
    },
}

_ROLES: tuple[TaskRole, ...] = ("summarize", "research", "writer", "qa")
_MODES: tuple[ProviderMode, ...] = ("openai_api", "codex_auth", "clawbot")


def _is_mode(value: str) -> bool:
    return value in _MODES


def _merge_defaults(payload: dict[str, Any]) -> dict[str, Any]:
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in payload.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def load_agent_runtime_config(config_path: str | Path) -> AgentRuntimeConfig:
    path = Path(config_path).expanduser().resolve()
    raw: dict[str, Any] = {}
    if path.exists():
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw_payload, dict):
            raise ValueError("agent_runtime config must be a JSON object")
        raw = raw_payload
    merged = _merge_defaults(raw)

    provider_modes = cast(dict[str, dict[str, Any]], merged.get("provider_modes") or {})
    available: list[ProviderMode] = []
    for mode in _MODES:
        details = provider_modes.get(mode) or {}
        if bool(details.get("available")):
            available.append(mode)

    role_slots = cast(dict[TaskRole, dict[str, Any]], merged.get("model_role_slots") or {})
    normalized_slots: dict[TaskRole, dict[str, Any]] = {}
    for role in _ROLES:
        normalized_slots[role] = dict(role_slots.get(role) or {})

    fallback_order_raw = cast(list[str], merged.get("fallback_order") or [])
    fallback_order = tuple(cast(ProviderMode, mode) for mode in fallback_order_raw if _is_mode(mode))

    thresholds_raw = cast(dict[str, Any], merged.get("qa_thresholds") or {})
    thresholds = QAGateThresholds(
        min_sources=max(0, int(thresholds_raw.get("min_sources", 0))),
        min_signals=max(0, int(thresholds_raw.get("min_signals", 0))),
        min_contact_coverage_pct=float(thresholds_raw.get("min_contact_coverage_pct", 0.0)),
    )

    return AgentRuntimeConfig(
        enabled=bool(merged.get("enabled", False)),
        provider_modes_available=tuple(available),
        model_role_slots=normalized_slots,
        fallback_order=fallback_order,
        qa_thresholds=thresholds,
        config_path=str(path),
    )


def select_provider_for_role(role: TaskRole, config: AgentRuntimeConfig) -> SelectedProvider:
    slot = config.model_role_slots.get(role) or {}
    preferred_raw = slot.get("preferred_providers") or []
    preferred = [cast(ProviderMode, mode) for mode in preferred_raw if isinstance(mode, str) and _is_mode(mode)]

    attempts: list[ProviderMode] = []
    for mode in preferred + list(config.fallback_order):
        if mode not in attempts:
            attempts.append(mode)

    for mode in attempts:
        if mode in config.provider_modes_available:
            model = str(slot.get("model") or "gpt-4.1-mini")
            return SelectedProvider(
                role=role,
                provider_mode=mode,
                model=model,
                attempted_order=tuple(attempts),
            )

    raise RuntimeError(f"No available provider mode for role={role}")


def status_snapshot(config_path: str | Path) -> dict[str, Any]:
    path = Path(config_path).expanduser().resolve()
    try:
        config = load_agent_runtime_config(path)
    except Exception as exc:
        return {
            "enabled": False,
            "config_path": str(path),
            "provider_modes_available": [],
            "qa_thresholds": {
                "min_sources": DEFAULT_CONFIG["qa_thresholds"]["min_sources"],
                "min_signals": DEFAULT_CONFIG["qa_thresholds"]["min_signals"],
                "min_contact_coverage_pct": DEFAULT_CONFIG["qa_thresholds"]["min_contact_coverage_pct"],
            },
            "last_error": str(exc),
        }

    return {
        "enabled": config.enabled,
        "config_path": config.config_path,
        "provider_modes_available": list(config.provider_modes_available),
        "qa_thresholds": {
            "min_sources": config.qa_thresholds.min_sources,
            "min_signals": config.qa_thresholds.min_signals,
            "min_contact_coverage_pct": config.qa_thresholds.min_contact_coverage_pct,
        },
        "last_error": "",
    }
