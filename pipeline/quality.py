from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter


def run_quality_report(con, out_dir: Path) -> dict[str, object]:
    out_dir.mkdir(parents=True, exist_ok=True)
    total = con.execute("SELECT COUNT(*) AS c FROM locations WHERE deleted_at IS NULL").fetchone()["c"]
    with_email = con.execute(
        "SELECT COUNT(*) AS c FROM locations l "
        "WHERE l.deleted_at IS NULL AND EXISTS (SELECT 1 FROM contact_points cp WHERE cp.location_pk=l.location_pk AND cp.type='email' AND cp.value<>'')",
    ).fetchone()["c"]
    with_buyer = con.execute(
        "SELECT COUNT(*) AS c FROM locations l "
        "WHERE l.deleted_at IS NULL AND EXISTS (SELECT 1 FROM contacts c WHERE c.location_pk=l.location_pk AND LOWER(c.role) LIKE '%buyer%')",
    ).fetchone()["c"]

    duplicate_domains = con.execute(
        "SELECT COUNT(*) AS dups FROM (SELECT website_domain FROM locations WHERE deleted_at IS NULL AND website_domain<>'' GROUP BY website_domain HAVING COUNT(*)>1)",
    ).fetchone()["dups"]

    freshness_rows = con.execute(
        "SELECT last_seen_at FROM locations WHERE deleted_at IS NULL",
    ).fetchall()
    now = datetime.now(timezone.utc)
    buckets = Counter()
    for row in freshness_rows:
        ts = row["last_seen_at"]
        if not ts:
            buckets["unknown"] += 1
            continue
        try:
            delta = (now - datetime.fromisoformat(ts)).days
        except Exception:
            buckets["invalid"] += 1
            continue
        if delta <= 1:
            buckets["0-1d"] += 1
        elif delta <= 7:
            buckets["2-7d"] += 1
        elif delta <= 30:
            buckets["8-30d"] += 1
        else:
            buckets["31+d"] += 1

    provider_rows = con.execute(
        "SELECT field_value, COUNT(*) AS c FROM evidence WHERE field_name='menu_provider' AND entity_type='location' AND deleted_at IS NULL GROUP BY field_value ORDER BY c DESC LIMIT 10",
    ).fetchall()

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_active_leads": int(total or 0),
        "pct_leads_with_email": round((with_email / total * 100.0), 2) if total else 0.0,
        "pct_leads_with_buyer_title": round((with_buyer / total * 100.0), 2) if total else 0.0,
        "duplicate_domain_rate": round((duplicate_domains / total * 100.0), 2) if total else 0.0,
        "freshness_distribution": dict(buckets),
        "top_menu_providers": [f"{r['field_value']}:{r['c']}" for r in provider_rows],
    }

    text_path = out_dir / "v4_quality_report.txt"
    lines = [
        f"CannaRadar Quality Report ({report['generated_at']})",
        f"Total active locations: {report['total_active_leads']}",
        f"Leads w/ email: {report['pct_leads_with_email']}%",
        f"Leads w/ buyer-ish role: {report['pct_leads_with_buyer_title']}%",
        f"Duplicate domain rate: {report['duplicate_domain_rate']}%",
        "Freshness buckets:",
    ]
    for k, v in sorted(buckets.items()):
        lines.append(f"  {k}: {v}")
    lines.append("Top menu providers:")
    for item in report["top_menu_providers"]:
        lines.append(f"  - {item}")
    text_path.write_text("\n".join(lines) + "\n")

    json_path = out_dir / "quality_report.json"
    json_path.write_text(json.dumps(report, indent=2))
    return {
        "text": str(text_path),
        "json": str(json_path),
        "metrics": report,
    }
