from __future__ import annotations

from typing import Any

from cli.errors import DataValidationError, ResumeStateError
from pipeline.run_control import (
    append_intervention,
    domain_control_record,
    load_run_control,
    resolve_run_control_id,
    summarize_run_control,
    update_agent_controls,
)
from pipeline.utils import normalize_domain


def _normalize_control_domain(value: str) -> str:
    domain = normalize_domain(value)
    if not domain or " " in domain:
        raise DataValidationError("Domain must be a normalized hostname or URL.", details={"value": value})
    return domain


def _normalize_prefix(value: str) -> str:
    prefix = (value or "").strip().lower()
    if not prefix:
        raise DataValidationError("Prefix cannot be empty.")
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    if prefix != "/" and not prefix.endswith("/"):
        prefix = f"{prefix}/" if "." not in prefix.rsplit("/", 1)[-1] else prefix
    return prefix


def run_control_show(*, run_id: str | None, run_state_dir: str | None) -> dict[str, Any]:
    resolved_run_id = resolve_run_control_id(run_id, run_state_dir)
    state = load_run_control(resolved_run_id, run_state_dir)
    return summarize_run_control(state)


def run_control_apply(*, run_id: str | None, run_state_dir: str | None, action: str, domain: str, value: str | int | None, reason: str) -> dict[str, Any]:
    resolved_run_id = resolve_run_control_id(run_id, run_state_dir)
    normalized_domain = _normalize_control_domain(domain)

    def updater(state: dict[str, Any]) -> None:
        record = domain_control_record(state, normalized_domain)
        if action == "quarantine-seed":
            record["quarantined"] = True
            record["quarantine_reason"] = reason.strip() or "agent_quarantine"
        elif action == "suppress-prefix":
            prefix = _normalize_prefix(str(value or ""))
            prefixes = {str(item).strip().lower() for item in record.get("suppressed_path_prefixes", []) if str(item).strip()}
            prefixes.add(prefix)
            record["suppressed_path_prefixes"] = sorted(prefixes)
        elif action == "cap-domain":
            cap = int(value or 0)
            if cap <= 0:
                raise DataValidationError("Domain page cap must be a positive integer.", details={"value": value})
            record["max_pages_per_domain"] = cap
        elif action == "stop-domain":
            record["stop_requested"] = True
        elif action == "clear-domain":
            record.update(
                {
                    "quarantined": False,
                    "quarantine_reason": "",
                    "suppressed_path_prefixes": [],
                    "max_pages_per_domain": None,
                    "stop_requested": False,
                }
            )
        else:
            raise ResumeStateError(f"Unsupported control action: {action}")
        record["updated_at"] = ""
        append_intervention(
            state,
            domain=normalized_domain,
            action=action,
            reason=reason.strip() or action,
            source="agent",
            details={"value": value} if value not in (None, "") else {},
        )

    updated = update_agent_controls(resolved_run_id, updater, base_dir=run_state_dir)
    return summarize_run_control(updated)
