from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
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


def _days_since(timestamp: str | None, now: datetime | None = None) -> float:
    if not timestamp:
        return float("inf")
    now = now or datetime.now()
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)
        return (now - parsed).total_seconds() / 86400
    except Exception:
        return float("inf")


def _signal_confidence(
    fit_score: int,
    has_buyer_contact: bool,
    has_email: bool,
    has_phone: bool,
    recency_days: float,
) -> float:
    base = min(1.0, max(0.0, fit_score / 100))
    recency_boost = 0.0
    if recency_days <= 1:
        recency_boost = 0.25
    elif recency_days <= 7:
        recency_boost = 0.15
    elif recency_days <= 30:
        recency_boost = 0.05

    return min(
        1.0,
        round(
            base * 0.55
            + (0.22 if has_buyer_contact else 0.0)
            + (0.14 if has_email else 0.0)
            + (0.09 if has_phone else 0.0)
            + recency_boost,
            3,
        ),
    )


def _watch_state(signal_confidence: float, recency_days: float) -> tuple[str, int]:
    if signal_confidence >= 0.9 and recency_days <= 2:
        return "critical", 0
    if signal_confidence >= 0.8:
        return "hot", 1
    if signal_confidence >= 0.65:
        return "warm", 2
    if signal_confidence >= 0.5:
        return "watch", 3
    return "monitor", 4


def _recency_signal(recency_days: float) -> str:
    if recency_days == float("inf"):
        return "unknown"
    if recency_days <= 1:
        return "today"
    if recency_days <= 7:
        return "week"
    if recency_days <= 30:
        return "month"
    return "stale"


def _best_contact(con, location_pk: str) -> tuple[str, str, str]:
    row = con.execute(
        """
        SELECT full_name, role, email
        FROM contacts
        WHERE location_pk = ? AND COALESCE(deleted_at,'')=''
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


def _has_buyer_contact(con, location_pk: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM contacts
        WHERE location_pk=?
          AND COALESCE(deleted_at,'')=''
          AND (
            lower(role) LIKE '%buyer%'
            OR lower(role) LIKE '%purchasing%'
            OR lower(role) LIKE '%inventory%'
            OR lower(role) LIKE '%owner%'
            OR lower(role) LIKE '%operations%'
          )
        LIMIT 1
        """,
        (location_pk,),
    ).fetchone()
    return row is not None


def _has_direct_email(con, location_pk: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM contact_points
        WHERE location_pk=?
          AND type='email'
          AND value<>''
          AND COALESCE(deleted_at,'')=''
        LIMIT 1
        """,
        (location_pk,),
    ).fetchone()
    return row is not None


def _has_direct_phone(con, location_pk: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM contact_points
        WHERE location_pk=?
          AND type='phone'
          AND value<>''
          AND COALESCE(deleted_at,'')=''
        LIMIT 1
        """,
        (location_pk,),
    ).fetchone()
    return row is not None


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
        "SELECT location_pk, canonical_name, website_domain, state FROM locations WHERE COALESCE(deleted_at,'')='' ORDER BY fit_score DESC, updated_at DESC"
    ).fetchall()
    tier_order = {"A": 3, "B": 2, "C": 1}
    min_tier = tier_order.get(tier, 3)

    outreach_rows: list[dict] = []
    excluded_rows: list[dict] = []

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
        for row in outreach_rows:
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
        "SELECT location_pk, canonical_name, website_domain FROM locations WHERE COALESCE(deleted_at,'')='' ORDER BY fit_score DESC",
    ).fetchall()
    candidates: list[dict] = []
    for row in rows:
        loc_pk = row["location_pk"]
        has_buyer = con.execute(
            """
            SELECT 1 FROM contacts
            WHERE location_pk = ?
              AND COALESCE(deleted_at,'')=''
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


def export_new_leads(
    con,
    out_dir: Path,
    since: str | None = None,
    limit: int = 100,
    run_id: str = "",
) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    path = out_dir / f"new_leads_since_run_{run_id}.csv"
    cutoff = since or (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")
    rows = con.execute(
        """
        SELECT location_pk, canonical_name, website_domain, state, created_at, last_crawled_at
        FROM locations
        WHERE COALESCE(deleted_at,'')=''
          AND created_at >= ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (cutoff, limit),
    ).fetchall()

    fields = [
        "company_name",
        "website",
        "state",
        "created_at",
        "last_crawled_at",
        "contact_name",
        "contact_title",
        "email",
        "phone",
        "score",
        "tier",
        "menu_provider",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            loc_pk = row["location_pk"]
            score, tier = _active_score(con, loc_pk)
            contact_name, contact_title, contact_email = _best_contact(con, loc_pk)
            writer.writerow(
                {
                    "company_name": row["canonical_name"] or "",
                    "website": row["website_domain"] or "",
                    "state": row["state"] or "",
                    "created_at": row["created_at"] or "",
                    "last_crawled_at": row["last_crawled_at"] or "",
                    "contact_name": contact_name,
                    "contact_title": contact_title,
                    "email": contact_email,
                    "phone": _best_phone(con, loc_pk),
                    "score": str(score),
                    "tier": tier,
                    "menu_provider": _menu_provider_for_location(con, loc_pk),
                }
            )
    return str(path)


def export_buyer_signal_queue(
    con,
    out_dir: Path,
    since: str | None = None,
    limit: int = 200,
    run_id: str = "",
) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    path = out_dir / f"buying_signal_watchlist_{run_id}.csv"
    cutoff = since or (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")
    rows = con.execute(
        """
        SELECT location_pk, canonical_name, website_domain, state, updated_at, last_seen_at, fit_score
        FROM locations
        WHERE COALESCE(deleted_at,'')=''
          AND fit_score >= 72
          AND (updated_at >= ? OR last_seen_at >= ?)
        ORDER BY fit_score DESC, updated_at DESC
        LIMIT ?
        """,
        (cutoff, cutoff, limit * 2),
    ).fetchall()

    fields = [
        "company_name",
        "website",
        "state",
        "score",
        "watch_state",
        "signal_confidence",
        "recency_days",
        "recency_signal",
        "segment",
        "has_buyer_contact",
        "has_direct_email",
        "has_direct_phone",
        "contact_name",
        "contact_title",
        "email",
        "phone",
        "recommended_action",
        "updated_at",
    ]
    now = datetime.now()
    ranked = []

    for row in rows:
        loc_pk = row["location_pk"]
        has_buyer_contact = _has_buyer_contact(con, loc_pk)
        has_email = _has_direct_email(con, loc_pk)
        has_phone = _has_direct_phone(con, loc_pk)
        if not (has_buyer_contact or has_email or has_phone):
            continue
        recency_days = _days_since(row["updated_at"] or row["last_seen_at"], now=now)
        signal_confidence = _signal_confidence(
            int(row["fit_score"] or 0),
            has_buyer_contact,
            has_email,
            has_phone,
            recency_days,
        )
        watch_state, bucket_rank = _watch_state(signal_confidence, recency_days)
        contact_name, contact_title, contact_email = _best_contact(con, loc_pk)
        ranked.append(
            {
                "company_name": row["canonical_name"] or "",
                "website": row["website_domain"] or "",
                "state": row["state"] or "",
                "score": str(int(row["fit_score"] or 0)),
                "watch_state": watch_state,
                "signal_confidence": str(signal_confidence),
                "recency_days": "" if recency_days == float("inf") else str(round(recency_days, 2)),
                "recency_signal": _recency_signal(recency_days),
                "bucket_rank": bucket_rank,
                "segment": _segment_company(row["canonical_name"] or "", row["website_domain"] or "")[0],
                "has_buyer_contact": str(has_buyer_contact).lower(),
                "has_direct_email": str(has_email).lower(),
                "has_direct_phone": str(has_phone).lower(),
                "contact_name": contact_name,
                "contact_title": contact_title,
                "email": contact_email,
                "phone": _best_phone(con, loc_pk),
                "recommended_action": "Priority outreach target. Verify buyer role then send buying intent message.",
                "updated_at": row["updated_at"] or "",
            }
        )

    ranked.sort(key=lambda item: (item["bucket_rank"], -float(item["signal_confidence"]), -int(item["score"])))

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in ranked[:limit]:
            out = dict(row)
            out.pop("bucket_rank", None)
            writer.writerow(out)
    return str(path)
