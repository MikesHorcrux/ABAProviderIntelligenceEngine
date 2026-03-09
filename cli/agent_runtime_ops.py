from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agent_runtime.contracts import ProviderCall
from agent_runtime.providers import invoke_provider_mode
from agent_runtime.router import load_agent_runtime_config


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _extract_urls(text: str) -> list[str]:
    urls = re.findall(r"https?://[^\s)\]>]+", text or "")
    uniq: list[str] = []
    for u in urls:
        if u not in uniq:
            uniq.append(u)
    return uniq


def _iter_external_research_packages(out_dir: Path) -> list[dict[str, Any]]:
    manifest = out_dir / "lead_intelligence" / "lead_intelligence_manifest.json"
    if not manifest.exists():
        return []
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    packages = payload.get("packages") or []
    result: list[dict[str, Any]] = []
    for item in packages:
        if not isinstance(item, dict):
            continue
        rel_dir = str(item.get("package_dir") or "").strip()
        if not rel_dir:
            continue
        pkg_dir = (manifest.parent / rel_dir).resolve()
        result.append(
            {
                "company_name": item.get("company_name") or pkg_dir.name,
                "lead_id": item.get("lead_id") or "",
                "package_dir": pkg_dir,
                "status_path": pkg_dir / "external_research_status.json",
                "report_path": pkg_dir / "external_research_report.md",
                "lead_summary_path": pkg_dir / "lead_summary.json",
                "prompt_path": pkg_dir / "agent_research_prompt.md",
                "strategy_path": pkg_dir / "company-strategy.md",
                "dossier_report_path": pkg_dir / "report.md",
            }
        )
    return result


def _make_prompt(company_name: str, lead_summary: dict[str, Any], prompt_hint: str) -> str:
    website = lead_summary.get("website") or "unknown"
    location_count = lead_summary.get("location_count") or "unknown"
    pos_system = lead_summary.get("pos_system") or "unknown"
    compliance = lead_summary.get("compliance_system") or "unknown"

    return (
        "You are a lead intelligence research agent. Create a concise but actionable markdown report using only verifiable public data. "
        "If a fact cannot be verified, write 'unknown'. Include source URLs.\n\n"
        f"Company: {company_name}\n"
        f"Website: {website}\n"
        f"Location count (current estimate): {location_count}\n"
        f"POS system (current estimate): {pos_system}\n"
        f"Compliance system (current estimate): {compliance}\n\n"
        "Required headings exactly:\n"
        "# External Research - <Company Name>\n"
        "## Company Overview\n"
        "## Footprint\n"
        "## Buying Committee\n"
        "## Public Signals\n"
        "## Recommended Outreach Angles\n"
        "## Sources\n\n"
        "Use short bullets. Add only source-backed facts and mark unknown when not verifiable.\n\n"
        f"Additional context from package prompt:\n{prompt_hint[:5000]}"
    )


def run_agent_external_research(*, out_dir: str, config_path: str, limit: int = 0) -> dict[str, Any]:
    out_root = Path(out_dir).resolve()
    config = load_agent_runtime_config(config_path)

    packages = _iter_external_research_packages(out_root)
    processed = 0
    completed = 0
    failed = 0
    results: list[dict[str, Any]] = []

    for pkg in packages:
        if limit and processed >= limit:
            break
        processed += 1

        status_path: Path = pkg["status_path"]
        report_path: Path = pkg["report_path"]
        lead_summary_path: Path = pkg["lead_summary_path"]
        prompt_path: Path = pkg["prompt_path"]

        status: dict[str, Any] = {}
        if status_path.exists():
            status = json.loads(status_path.read_text(encoding="utf-8"))

        status.update(
            {
                "status": "in_progress",
                "agent_name": "cannaradar-agent-runtime",
                "started_at": status.get("started_at") or _utcnow_iso(),
                "updated_at": _utcnow_iso(),
                "last_error": "",
            }
        )
        status_path.write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")

        lead_summary = json.loads(lead_summary_path.read_text(encoding="utf-8")) if lead_summary_path.exists() else {}
        prompt_hint = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""
        prompt = _make_prompt(str(pkg["company_name"]), lead_summary, prompt_hint)

        slot = config.model_role_slots.get("research") or {}
        model = str(slot.get("model") or "gpt-4.1")
        attempted_order = []
        preferred = [m for m in (slot.get("preferred_providers") or []) if isinstance(m, str)]
        for mode in preferred + list(config.fallback_order):
            if mode in config.provider_modes_available and mode not in attempted_order:
                attempted_order.append(mode)

        text = ""
        last_error = ""
        used_mode = ""
        for mode in attempted_order:
            rsp = invoke_provider_mode(mode=mode, model=model, call=ProviderCall(role="research", prompt=prompt))
            candidate = (rsp.get("text") or "").strip()
            if candidate:
                text = candidate
                used_mode = mode
                break
            last_error = (rsp.get("error") or "empty provider response").strip()

        if not text:
            status.update(
                {
                    "status": "failed",
                    "updated_at": _utcnow_iso(),
                    "completed_at": "",
                    "output_path": "external_research_report.md",
                    "source_count": 0,
                    "last_error": last_error or "no provider produced output",
                }
            )
            status_path.write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
            failed += 1
            results.append({"company_name": pkg["company_name"], "status": "failed", "error": status["last_error"]})
            continue

        if not text.startswith("# External Research -"):
            text = f"# External Research - {pkg['company_name']}\n\n" + text

        urls = _extract_urls(text)
        report_path.write_text(text.strip() + "\n", encoding="utf-8")
        status.update(
            {
                "status": "completed",
                "updated_at": _utcnow_iso(),
                "completed_at": _utcnow_iso(),
                "output_path": "external_research_report.md",
                "source_count": len(urls),
                "last_error": "",
                "agent_name": f"cannaradar-agent-runtime:{used_mode or 'unknown'}",
            }
        )
        status_path.write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
        completed += 1
        results.append({"company_name": pkg["company_name"], "status": "completed", "provider": used_mode, "source_count": len(urls)})

    return {
        "processed": processed,
        "completed": completed,
        "failed": failed,
        "config_path": str(Path(config_path).resolve()),
        "results": results,
    }
