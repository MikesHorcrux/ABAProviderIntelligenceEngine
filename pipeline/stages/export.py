from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from pipeline.utils import utcnow_iso


POSITIVE_SEGMENTS = [
    "dispensary",
    "cannabis",
    "marijuana",
    "weed",
    "retail",
    "store",
]
NEGATIVE_SEGMENTS = [
    "distributor",
    "manufacturer",
    "brand",
    "cultivation",
    "lab",
    "testing",
    "delivery",
    "wholesale",
]


def _segment_company(name: str, website: str) -> tuple[str, float]:
    txt = f"{(name or '').lower()} {(website or '').lower()}"
    positives = [x for x in POSITIVE_SEGMENTS if x in txt]
    negatives = [x for x in NEGATIVE_SEGMENTS if x in txt]
    if positives and not negatives:
        return "dispensary", 0.9
    if negatives and not positives:
        return "non-dispensary", 0.9
    if positives and negatives:
        return ("dispensary", 0.65) if len(positives) >= len(negatives) else ("unknown", 0.55)
    return "unknown", 0.24


def _best_contact(con, location_pk: str) -> tuple[str, str, str]:
    row = con.execute(
        """
        SELECT full_name, role, email
        FROM contacts
        WHERE location_pk = ? AND deleted_at IS NULL
        ORDER BY confidence DESC, updated_at DESC
        LIMIT 1
        """,
        (location_pk,),
    ).fetchone()
    if not row:
        return "", "", ""
    return row["full_name"] or "", row["role"] or "", row["email"] or ""


def _best_phone(con, location_pk: str) -> str:
    row = con.execute(
        "SELECT value FROM contact_points WHERE location_pk=? AND type='phone' AND value<>'' ORDER BY confidence DESC LIMIT 1",
        (location_pk,),
    ).fetchone()
    return row["value"] if row else ""


def _menu_provider_for_location(con, location_pk: str) -> str:
    row = con.execute(
        """
        SELECT field_value
        FROM evidence
        WHERE entity_type='location'
          AND entity_pk=?
          AND field_name='menu_provider'
        ORDER BY captured_at DESC
        LIMIT 1
        """,
        (location_pk,),
    ).fetchone()
    return row["field_value"] if row else ""


def _proof_urls(con, location_pk: str) -> str:
    rows = con.execute(
        "SELECT source_url FROM evidence WHERE entity_type='location' AND entity_pk=? AND COALESCE(source_url,'')<>''",
        (location_pk,),
    ).fetchall()
    return "; ".join(sorted({r["source_url"] for r in rows}))


def _active_score(con, loc_pk: str) -> tuple[int, str]:
    row = con.execute(
        "SELECT score_total, tier FROM lead_scores WHERE location_pk=? ORDER BY as_of DESC LIMIT 1",
        (loc_pk,),
    ).fetchone()
    if not row:
        return 0, "C"
    return int(row["score_total"] or 0), row["tier"] or "C"


def export_outreach(con, out_dir: Path, tier: str = "A", limit: int = 200, run_id: str = "") -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    run_id = (run_id.replace(":", "").replace(" ", "_") or datetime.now().strftime("%Y%m%d-%H%M%S"))
    out_file = out_dir / f"outreach_ready_{run_id}.csv"
    legacy_out = out_dir / "outreach_dispensary_100.csv"
    excluded_out = out_dir / "excluded_non_dispensary.csv"

    rows = con.execute(
        "SELECT location_pk, canonical_name, website_domain, state FROM locations WHERE deleted_at IS NULL ORDER BY fit_score DESC, updated_at DESC"
    ).fetchall()
    tier_order = {"A": 3, "B": 2, "C": 1}
    min_tier = tier_order.get(tier, 3)

    outreach_rows: list[dict] = []
    excluded_rows: list[dict] = []
    all_rows: list[dict] = []

    for row in rows:
        loc_pk = row["location_pk"]
        score, current_tier = _active_score(con, loc_pk)
        if current_tier not in ("A", "B", "C"):
            current_tier = "C"

        segment, segment_conf = _segment_company(row["canonical_name"] or "", row["website_domain"] or "")
        contact_name, contact_title, contact_email = _best_contact(con, loc_pk)
        phone = _best_phone(con, loc_pk)
        menu_provider = _menu_provider_for_location(con, loc_pk)
        proofs = _proof_urls(con, loc_pk)

        record = {
            "company_name": row["canonical_name"] or "",
            "location": row["canonical_name"] or "",
            "website": row["website_domain"] or "",
            "menu_provider": menu_provider,
            "contact_name": contact_name,
            "contact_title": contact_title,
            "email": contact_email,
            "phone": phone,
            "score": str(score),
            "tier": current_tier,
            "proof_urls": proofs,
            "segment": segment,
            "segment_confidence": f"{segment_conf:.2f}",
            "state": row["state"] or "",
        }

        all_rows.append(record)
        if segment == "dispensary" and tier_order.get(current_tier, 1) >= min_tier:
            outreach_rows.append(record)
        else:
            excluded_rows.append(record)

    outreach_rows = sorted(outreach_rows, key=lambda x: int(x["score"]), reverse=True)[:limit]
    outreach_fields = [
        "company_name",
        "location",
        "website",
        "menu_provider",
        "contact_name",
        "contact_title",
        "email",
        "phone",
        "score",
        "tier",
        "proof_urls",
    ]
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=outreach_fields)
        writer.writeheader()
        for row in outreach_rows:
            writer.writerow({k: row.get(k, "") for k in outreach_fields})

    legacy_fields = [
        "dispensary",
        "segment",
        "website",
        "state",
        "market",
        "owner_name",
        "owner_role",
        "email",
        "phone",
        "source_url",
        "score",
        "checked_at",
        "segment_confidence",
        "segment_reason",
    ]
    with legacy_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=legacy_fields)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(
                {
                    "dispensary": row["company_name"],
                    "segment": row["segment"],
                    "website": row["website"],
                    "state": row["state"],
                    "market": "",
                    "owner_name": row["contact_name"],
                    "owner_role": row["contact_title"],
                    "email": row["email"],
                    "phone": row["phone"],
                    "source_url": row["proof_urls"].split(";")[0] if row["proof_urls"] else "",
                    "score": row["score"],
                    "checked_at": utcnow_iso(),
                    "segment_confidence": row["segment_confidence"],
                    "segment_reason": "rule-based segment",
                }
            )
    with excluded_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=legacy_fields)
        writer.writeheader()
        for row in sorted(excluded_rows, key=lambda x: int(x["score"]), reverse=True):
            writer.writerow(
                {
                    "dispensary": row["company_name"],
                    "segment": row["segment"],
                    "website": row["website"],
                    "state": row["state"],
                    "market": "",
                    "owner_name": row["contact_name"],
                    "owner_role": row["contact_title"],
                    "email": row["email"],
                    "phone": row["phone"],
                    "source_url": row["proof_urls"].split(";")[0] if row["proof_urls"] else "",
                    "score": row["score"],
                    "checked_at": utcnow_iso(),
                    "segment_confidence": row["segment_confidence"],
                    "segment_reason": "segment guardrail",
                }
            )

    report = {
        "outreach_file": str(out_file),
        "legacy_outreach_file": str(legacy_out),
        "excluded_file": str(excluded_out),
        "count": len(outreach_rows),
        "limit": limit,
        "requested_tier": tier,
        "generated_at": utcnow_iso(),
        "run_id": run_id,
    }
    report_path = out_dir / f"outreach_export_report_{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return report


def export_research_queue(con, out_dir: Path, limit: int = 200, run_id: str = "") -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "research_queue.csv"
    rows = con.execute(
        "SELECT location_pk, canonical_name, website_domain FROM locations WHERE deleted_at IS NULL ORDER BY fit_score DESC",
    ).fetchall()
    candidates: list[dict] = []
    for row in rows:
        loc_pk = row["location_pk"]
        has_buyer = con.execute(
            """
            SELECT 1 FROM contacts
            WHERE location_pk = ?
              AND deleted_at IS NULL
              AND (
                lower(role) LIKE '%buyer%'
                OR lower(role) LIKE '%purchasing%'
                OR lower(role) LIKE '%inventory%'
                OR lower(role) LIKE '%owner%'
              )
            LIMIT 1
            """,
            (loc_pk,),
        ).fetchone()
        if has_buyer:
            continue
        contact_name, contact_title, contact_email = _best_contact(con, loc_pk)
        candidates.append(
            {
                "company_name": row["canonical_name"] or "",
                "website": row["website_domain"] or "",
                "contact_name": contact_name or "",
                "contact_title": contact_title or "",
                "email": contact_email,
                "phone": _best_phone(con, loc_pk),
                "recommended_action": "Call store and ask who handles purchasing/inventory buying.",
                "state": "",
                "score": str(_active_score(con, loc_pk)[0]),
            }
        )
        if len(candidates) >= limit:
            break

    headers = [
        "company_name",
        "website",
        "contact_name",
        "contact_title",
        "email",
        "phone",
        "state",
        "recommended_action",
        "score",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in candidates:
            writer.writerow(row)
    return str(out_path)


def export_merge_suggestions(con, out_dir: Path, run_id: str = "") -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    path = out_dir / f"merge_suggestions_{run_id}.csv"
    rows = con.execute(
        """
        SELECT resolution_pk, canonical_location_pk, candidate_location_pk, reason, confidence, created_at
        FROM entity_resolutions
        WHERE resolution_status='suggest_merge'
        ORDER BY created_at DESC
        """,
    ).fetchall()
    headers = ["resolution_pk", "canonical_location_pk", "candidate_location_pk", "reason", "confidence", "created_at"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    return str(path)
