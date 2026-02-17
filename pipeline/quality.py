from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def run_quality_report(con, out_dir: Path) -> dict[str, object]:
    total = con.execute("SELECT COUNT(*) AS c FROM locations WHERE COALESCE(deleted_at,'')=''").fetchone()["c"]
    with_email = con.execute(
        "SELECT COUNT(*) AS c FROM locations l "
        "WHERE COALESCE(l.deleted_at,'')='' AND EXISTS (SELECT 1 FROM contact_points cp WHERE cp.location_pk=l.location_pk AND cp.type='email' AND cp.value<>'')",
    ).fetchone()["c"]
    with_buyer = con.execute(
        "SELECT COUNT(*) AS c FROM locations l "
        "WHERE COALESCE(l.deleted_at,'')='' AND EXISTS (SELECT 1 FROM contacts c WHERE c.location_pk=l.location_pk AND LOWER(c.role) LIKE '%buyer%')",
    ).fetchone()["c"]
    dup_domains = con.execute(
        "SELECT COUNT(*) AS dups FROM (SELECT website_domain FROM locations WHERE COALESCE(deleted_at,'')='' AND website_domain<>'' GROUP BY website_domain HAVING COUNT(*)>1)",
    ).fetchone()["dups"]

    freshness_rows = con.execute(
        "SELECT last_seen_at FROM locations WHERE COALESCE(deleted_at,'')=''",
    ).fetchall()
    buckets = {"0-7d": 0, "8-30d": 0, "31-90d": 0, "90d+": 0, "unknown": 0}
    now = datetime.now()
    for row in freshness_rows:
        value = (row["last_seen_at"] or "").strip()
        if not value:
            buckets["unknown"] += 1
            continue
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            age = (now - dt.replace(tzinfo=None)).days
        except Exception:
            buckets["unknown"] += 1
            continue
        if age <= 7:
            buckets["0-7d"] += 1
        elif age <= 30:
            buckets["8-30d"] += 1
        elif age <= 90:
            buckets["31-90d"] += 1
        else:
            buckets["90d+"] += 1

    top_menu = con.execute(
        "SELECT field_value, COUNT(*) AS c FROM evidence WHERE field_name='menu_provider' AND entity_type='location' AND COALESCE(deleted_at,'')='' GROUP BY field_value ORDER BY c DESC LIMIT 10",
    ).fetchall()

    metrics = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_active_leads": int(total),
        "pct_leads_with_email": round((with_email / total) * 100, 2) if total else 0.0,
        "pct_leads_with_buyer_title": round((with_buyer / total) * 100, 2) if total else 0.0,
        "duplicate_domain_rate": round((dup_domains / total) * 100, 2) if total else 0.0,
        "freshness_distribution": buckets,
        "top_menu_providers": [
            {"provider": r["field_value"], "count": int(r["c"])}
            for r in top_menu
            if (r["field_value"] or "").strip()
        ],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    txt_path = out_dir / "v4_quality_report.txt"
    json_path = out_dir / "quality_report.json"

    txt_lines = [
        f"Quality Report ({metrics['generated_at']})",
        f"Total active leads: {metrics['total_active_leads']}",
        f"% with email: {metrics['pct_leads_with_email']}",
        f"% with buyer-ish title: {metrics['pct_leads_with_buyer_title']}",
        f"Duplicate domain rate: {metrics['duplicate_domain_rate']}",
        "Freshness:",
    ]
    for key, value in metrics["freshness_distribution"].items():
        txt_lines.append(f"- {key}: {value}")
    txt_lines.append("Top menu providers:")
    for row in metrics["top_menu_providers"]:
        txt_lines.append(f"- {row['provider']}: {row['count']}")

    txt_path.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    return {"text": str(txt_path), "json": str(json_path), "metrics": metrics}
