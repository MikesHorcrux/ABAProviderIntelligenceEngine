from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from pipeline.config import CrawlConfig
from pipeline.stages.research import build_lead_research_briefs
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
        ORDER BY
          CASE
            WHEN lower(role) LIKE '%owner%' THEN 0
            WHEN lower(role) LIKE '%general manager%' THEN 1
            WHEN lower(role) LIKE '%store manager%' THEN 2
            WHEN lower(role) LIKE '%operations manager%' THEN 3
            WHEN lower(role) LIKE '%manager%' THEN 4
            WHEN lower(role) LIKE '%buyer%' THEN 5
            WHEN lower(role) LIKE '%purchasing%' THEN 6
            WHEN lower(role) LIKE '%inventory%' THEN 7
            ELSE 9
          END ASC,
          confidence DESC,
          updated_at DESC
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
            OR lower(role) LIKE '%manager%'
            OR lower(role) LIKE '%gm%'
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


ROLE_BUCKET_PRIORITY = {
    "owner": 0,
    "operations": 1,
    "compliance": 2,
    "buyer": 3,
    "finance": 4,
    "marketing": 5,
    "store": 6,
    "other": 7,
}
ROLE_BUCKET_LABELS = {
    "owner": "Owner",
    "operations": "Operations",
    "compliance": "Compliance",
    "buyer": "Buyer",
    "finance": "Finance",
    "marketing": "Marketing",
    "store": "Store",
    "other": "Other",
}


def _slugify(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    return lowered.strip("-") or "profile"


def _website_url(website: str) -> str:
    cleaned = str(website or "").strip()
    if not cleaned:
        return ""
    if cleaned.startswith(("http://", "https://")):
        return cleaned
    return f"https://{cleaned}"


def _display_location(city: str, state: str) -> str:
    city_value = str(city or "").strip()
    state_value = str(state or "").strip()
    if city_value and state_value:
        return f"{city_value}, {state_value}"
    return city_value or state_value


def _first_evidence_value(con, location_pk: str, field_names: tuple[str, ...]) -> str:
    for field_name in field_names:
        row = con.execute(
            """
            SELECT field_value
            FROM evidence
            WHERE entity_type='location'
              AND entity_pk=?
              AND field_name=?
              AND COALESCE(deleted_at,'')=''
              AND COALESCE(field_value,'')<>''
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            (location_pk, field_name),
        ).fetchone()
        if row and row["field_value"]:
            return str(row["field_value"] or "")
    return ""


def _org_location_count(con, org_pk: str) -> int:
    row = con.execute(
        "SELECT COUNT(*) AS c FROM locations WHERE org_pk=? AND COALESCE(deleted_at,'')=''",
        (org_pk,),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


def _role_bucket(title: str) -> str:
    lowered = str(title or "").lower()
    if any(token in lowered for token in ("owner", "founder", "ceo", "president", "principal", "co-founder")):
        return "owner"
    if any(
        token in lowered
        for token in ("operations", "general manager", "store manager", "gm", "operator", "regional manager", "district manager")
    ):
        return "operations"
    if any(token in lowered for token in ("compliance", "regulatory", "metrc", "audit")):
        return "compliance"
    if any(token in lowered for token in ("buyer", "purchasing", "inventory", "merch", "category")):
        return "buyer"
    if any(token in lowered for token in ("finance", "controller", "accounting", "cfo", "bookkeeper")):
        return "finance"
    if any(token in lowered for token in ("marketing", "brand", "community", "growth", "social")):
        return "marketing"
    if any(token in lowered for token in ("budtender", "store lead", "floor lead", "front desk", "reception", "assistant manager")):
        return "store"
    return "other"


def _role_bucket_label(bucket: str) -> str:
    return ROLE_BUCKET_LABELS.get(bucket, "Other")


def _role_priority(bucket: str) -> int:
    return ROLE_BUCKET_PRIORITY.get(bucket, ROLE_BUCKET_PRIORITY["other"])


def _decision_network_contacts(con, org_pk: str, fallback_location_pk: str) -> list[dict[str, str]]:
    rows = con.execute(
        """
        SELECT c.location_pk,
               c.full_name,
               c.role,
               c.email,
               c.phone,
               c.confidence,
               c.updated_at,
               l.canonical_name,
               l.city,
               l.state
        FROM contacts c
        JOIN locations l ON l.location_pk = c.location_pk
        WHERE l.org_pk = ?
          AND COALESCE(l.deleted_at,'')=''
          AND COALESCE(c.deleted_at,'')=''
        ORDER BY c.confidence DESC, c.updated_at DESC, c.full_name ASC
        """,
        (org_pk,),
    ).fetchall()
    selected: dict[str, dict[str, str]] = {}
    seen_people: set[tuple[str, str, str]] = set()
    for row in rows:
        name = str(row["full_name"] or "").strip()
        title = str(row["role"] or "").strip()
        email = str(row["email"] or "").strip()
        phone = str(row["phone"] or "").strip() or _best_phone(con, str(row["location_pk"]))
        dedupe_key = (name.lower(), title.lower(), email.lower())
        if dedupe_key in seen_people:
            continue
        seen_people.add(dedupe_key)
        bucket = _role_bucket(title)
        if bucket in selected:
            continue
        selected[bucket] = {
            "location_pk": str(row["location_pk"] or fallback_location_pk),
            "name": name,
            "title": title,
            "email": email,
            "phone": phone,
            "role_bucket": bucket,
            "role_label": _role_bucket_label(bucket),
            "location_name": str(row["canonical_name"] or ""),
            "location": _display_location(str(row["city"] or ""), str(row["state"] or "")),
            "linkedin": "",
        }
    ordered = [selected[key] for key in sorted(selected, key=_role_priority)]
    return ordered[:6]


def _profile_channel(bucket: str) -> str:
    if bucket == "owner":
        return "Email + LinkedIn"
    if bucket in {"operations", "buyer", "compliance"}:
        return "Email"
    if bucket == "store":
        return "Call + Email"
    return "Email"


def _profile_tone(bucket: str) -> str:
    if bucket == "owner":
        return "Strategic and growth-oriented"
    if bucket == "operations":
        return "Practical and workflow-driven"
    if bucket == "compliance":
        return "Risk-aware and detail-oriented"
    if bucket == "buyer":
        return "Commercial and margin-aware"
    if bucket == "finance":
        return "ROI-driven and disciplined"
    if bucket == "marketing":
        return "Commercial and brand-aware"
    if bucket == "store":
        return "Direct and operational"
    return "Concise and practical"


def _profile_avoid(bucket: str) -> str:
    if bucket == "owner":
        return "Avoid feature tours before the business case is clear."
    if bucket == "operations":
        return "Avoid vague strategy language without workflow specifics."
    if bucket == "compliance":
        return "Avoid promising compliance benefits you cannot evidence."
    if bucket == "buyer":
        return "Avoid generic software language detached from assortment or margin impact."
    return "Avoid generic product language without a clear operational reason to respond."


def _profile_goals(bucket: str, location_count: int) -> list[str]:
    if bucket == "owner":
        return [
            f"Scale a {location_count}-location retail footprint without adding founder dependency.",
            "Protect margin while stores become more operationally complex.",
            "Support expansion with cleaner reporting and better operating visibility.",
        ]
    if bucket == "operations":
        return [
            "Reduce manual reporting between systems and stores.",
            "Improve multi-location visibility for inventory and execution.",
            "Standardize workflows that currently rely on ad hoc coordination.",
        ]
    if bucket == "compliance":
        return [
            "Reduce reconciliation risk between internal records and regulatory systems.",
            "Shorten audit-prep work and exception handling.",
            "Make compliance reporting more reliable across locations.",
        ]
    if bucket == "buyer":
        return [
            "Improve purchasing decisions with better inventory visibility.",
            "Reduce SKU aging, dead stock, and stockout risk.",
            "Make assortment planning faster and more consistent.",
        ]
    if bucket == "finance":
        return [
            "Tighten margin visibility and working-capital discipline.",
            "Reduce reporting lag around purchasing and store performance.",
            "Support spend decisions with cleaner operating data.",
        ]
    if bucket == "marketing":
        return [
            "Improve visibility into what inventory and promotions are actually working.",
            "Coordinate campaigns with in-store availability more reliably.",
            "Protect brand experience as the retail footprint grows.",
        ]
    return [
        "Keep store execution stable as the business scales.",
        "Reduce avoidable manual work in the current operating stack.",
        "Make decision-making faster with cleaner retail data.",
    ]


def _profile_pain_points(bucket: str, location_count: int, gaps: list[str]) -> list[str]:
    points = []
    if bucket == "owner":
        points.extend(
            [
                f"Operational complexity increases quickly once the business moves beyond a single store; {location_count} locations usually means more reporting friction.",
                "Founder visibility degrades as decisions spread across locations and managers.",
            ]
        )
    elif bucket == "operations":
        points.extend(
            [
                "Manual reconciliation between commerce, inventory, and reporting workflows.",
                "Cross-store coordination overhead and inconsistent execution.",
            ]
        )
    elif bucket == "compliance":
        points.extend(
            [
                "Audit preparation and reconciliation work spikes when data does not line up cleanly.",
                "System changes are risky if traceability is weak.",
            ]
        )
    elif bucket == "buyer":
        points.extend(
            [
                "SKU aging and dead inventory are hard to see early enough.",
                "Purchasing decisions can be delayed by fragmented data.",
            ]
        )
    elif bucket == "finance":
        points.extend(
            [
                "Margin leakage is harder to isolate when reporting arrives late.",
                "Working-capital decisions suffer when purchasing and inventory data are fragmented.",
            ]
        )
    else:
        points.append("The current workflow likely includes too much manual coordination for a regulated retail business.")
    if "missing_direct_email" in gaps:
        points.append("Direct buyer contact coverage is still thin, which usually signals fragmented ownership of the workflow.")
    if "missing_menu_provider" in gaps:
        points.append("The commerce stack is not fully captured yet, which often means tooling visibility is incomplete.")
    return points[:4]


def _profile_buying_motivation(bucket: str) -> list[str]:
    if bucket == "owner":
        return [
            "Clear margin or growth leverage.",
            "Systems that reduce founder involvement in day-to-day operations.",
            "Operational clarity that supports expansion.",
        ]
    if bucket == "operations":
        return [
            "Less manual reporting.",
            "Faster issue detection across stores.",
            "Cleaner workflows that save operator time every week.",
        ]
    if bucket == "compliance":
        return [
            "Lower audit risk.",
            "Cleaner reconciliation and reporting.",
            "Confidence that the system will not create new edge-case failures.",
        ]
    if bucket == "buyer":
        return [
            "Better assortment and purchasing decisions.",
            "Fewer inventory mistakes.",
            "Practical signal that improves ordering cadence.",
        ]
    return [
        "Clear business value.",
        "Low-friction implementation.",
        "Reliable data that helps the team make better decisions.",
    ]


def _profile_hook(bucket: str, company_name: str, location_count: int, menu_provider: str, gaps: list[str]) -> str:
    stack = menu_provider or "the current commerce stack"
    if bucket == "owner":
        return (
            f"{company_name} is operating across {location_count} locations, which is usually where reporting, inventory, "
            f"and oversight stop behaving like a single-store workflow."
        )
    if bucket == "operations":
        return (
            f"Most multi-location dispensaries end up spending too much time reconciling data between {stack} and the rest "
            "of the operating workflow."
        )
    if bucket == "compliance":
        return (
            "The hardest part for compliance teams is rarely the rulebook itself; it is keeping inventory and reporting data aligned under pressure."
        )
    if bucket == "buyer":
        return (
            "Buying teams usually feel the pain first when aging inventory and reorder timing are spread across too many disconnected views."
        )
    if "missing_buyer_contact" in gaps:
        return "This account still needs clearer buying-committee coverage, which usually points to an opaque decision path."
    return f"{company_name} already shows enough retail signal to justify a sharper, role-specific outreach angle."


def _profile_product_angle(bucket: str) -> list[str]:
    if bucket == "owner":
        return [
            "Position the solution as operational leverage for growth.",
            "Tie the conversation to scale, margin protection, and cleaner visibility.",
        ]
    if bucket == "operations":
        return [
            "Lead with workflow automation and faster reporting.",
            "Show how the system reduces manual reconciliation and cross-store coordination.",
        ]
    if bucket == "compliance":
        return [
            "Lead with reliability, traceability, and reconciliation support.",
            "Frame value around risk reduction before efficiency gains.",
        ]
    if bucket == "buyer":
        return [
            "Lead with inventory intelligence, assortment quality, and purchasing confidence.",
            "Connect the pitch to margin and sell-through, not generic productivity.",
        ]
    return [
        "Anchor the pitch in one concrete workflow improvement.",
        "Use observed retail signals before introducing broader platform language.",
    ]


def _profile_psychology(bucket: str) -> str:
    if bucket == "owner":
        return "Growth-focused and sponsor-minded, but unlikely to stay engaged if the pitch sounds tactical without a business case."
    if bucket == "operations":
        return "Systems-oriented and evidence-driven; will care more about workflow impact than product branding."
    if bucket == "compliance":
        return "Risk-sensitive and detail-oriented; trust depends on reliability and clear auditability."
    if bucket == "buyer":
        return "Commercially pragmatic and likely to respond when the message improves purchasing judgment."
    return "Likely to respond to concrete, role-relevant operational value rather than broad positioning."


def _format_markdown_list(values: list[str]) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned:
        return "- None captured"
    return "\n".join(f"- {value}" for value in cleaned)


def _write_contact_profile(
    path: Path,
    *,
    lead_id: str,
    company_name: str,
    website: str,
    location_label: str,
    location_count: int,
    score: int,
    tier: str,
    menu_provider: str,
    compliance_system: str,
    google_rating: str,
    revenue_est: str,
    research_status: str,
    gaps: list[str],
    proof_urls: str,
    social_urls: str,
    contact: dict[str, str],
) -> None:
    bucket = str(contact.get("role_bucket") or "other")
    name = str(contact.get("name") or "").strip() or "Research Target"
    title = str(contact.get("title") or "").strip() or "Unverified buying-committee role"
    operational_environment = [
        f"Lead ID: {lead_id}",
        f"Primary market: {location_label or 'Unknown'}",
        f"Locations: {location_count}",
        f"Lead score / tier: {score} / {tier}",
        f"Website: {_website_url(website) or 'Not captured'}",
        f"POS / commerce stack: {menu_provider or 'Not captured'}",
        f"Compliance system: {compliance_system or 'Not captured'}",
        f"Google rating: {google_rating or 'Not captured'}",
        f"Revenue estimate: {revenue_est or 'Not captured'}",
        f"Research status: {research_status or 'unknown'}",
        f"Open gaps: {', '.join(gaps) if gaps else 'None'}",
    ]
    proof_list = [value.strip() for value in proof_urls.split(";") if value.strip()][:4]
    social_list = [value.strip() for value in social_urls.split(";") if value.strip()][:4]
    profile = "\n".join(
        [
            f"# {name}",
            f"{title} - {company_name}",
            "",
            f"LinkedIn: {contact.get('linkedin') or 'Not captured'}",
            "",
            "---",
            "",
            "## Overview",
            (
                f"{name} appears to sit in the {_role_bucket_label(bucket).lower()} lane of the buying committee for {company_name}. "
                f"For a {location_count}-location dispensary footprint, that role usually matters because it influences whether a new system gets championed, blocked, or deprioritized."
            ),
            "",
            "## Observed Operational Environment",
            _format_markdown_list(operational_environment),
            "",
            "## Likely Strategic Goals",
            _format_markdown_list(_profile_goals(bucket, location_count)),
            "",
            "## Pain Points",
            _format_markdown_list(_profile_pain_points(bucket, location_count, gaps)),
            "",
            "## Buying Motivation",
            _format_markdown_list(_profile_buying_motivation(bucket)),
            "",
            "## Conversation Hook",
            _profile_hook(bucket, company_name, location_count, menu_provider, gaps),
            "",
            "## Product Angle",
            _format_markdown_list(_profile_product_angle(bucket)),
            "",
            "## Psychological Profile",
            _profile_psychology(bucket),
            "",
            "## Outreach Strategy",
            f"- Channel: {_profile_channel(bucket)}",
            f"- Tone: {_profile_tone(bucket)}",
            f"- {_profile_avoid(bucket)}",
            "",
            "## Public Proof",
            _format_markdown_list(proof_list),
            "",
            "## Social Signals",
            _format_markdown_list(social_list),
            "",
        ]
    )
    path.write_text(profile, encoding="utf-8")


def _write_company_strategy(
    path: Path,
    *,
    lead_id: str,
    company_name: str,
    website: str,
    location_label: str,
    location_count: int,
    score: int,
    tier: str,
    menu_provider: str,
    compliance_system: str,
    google_rating: str,
    revenue_est: str,
    research_status: str,
    gaps: list[str],
    recommended_action: str,
    proof_urls: str,
    contacts: list[dict[str, str]],
) -> None:
    committee_rows = [
        f"{contact['role_label']}: {contact['name'] or 'Unknown'} - {contact['title'] or 'Unverified'}"
        for contact in contacts
    ]
    suggested_sequence = []
    for bucket in ("operations", "buyer", "owner", "compliance"):
        match = next((contact for contact in contacts if contact.get("role_bucket") == bucket), None)
        if match:
            suggested_sequence.append(f"Start with {match['role_label']} ({match['name'] or match['title'] or 'contact'}).")
    if not suggested_sequence:
        suggested_sequence.append("Start with the strongest named contact and validate who owns the buying workflow.")
    body = "\n".join(
        [
            f"# Company Strategy - {company_name}",
            "",
            "## Snapshot",
            _format_markdown_list(
                [
                    f"Lead ID: {lead_id}",
                    f"Website: {_website_url(website) or 'Not captured'}",
                    f"Primary market: {location_label or 'Unknown'}",
                    f"Locations: {location_count}",
                    f"Lead score / tier: {score} / {tier}",
                    f"POS / commerce stack: {menu_provider or 'Not captured'}",
                    f"Compliance system: {compliance_system or 'Not captured'}",
                    f"Google rating: {google_rating or 'Not captured'}",
                    f"Revenue estimate: {revenue_est or 'Not captured'}",
                    f"Research status: {research_status or 'unknown'}",
                ]
            ),
            "",
            "## Decision Network",
            _format_markdown_list(committee_rows),
            "",
            "## Messaging Priorities",
            _format_markdown_list(
                [
                    "Owner: position the solution as leverage for growth, margin protection, and cleaner visibility.",
                    "Operations: lead with reporting reduction, workflow standardization, and cross-store visibility.",
                    "Compliance: lead with auditability, reconciliation support, and lower process risk.",
                    "Buyer: lead with inventory intelligence, assortment quality, and better purchasing cadence.",
                ]
            ),
            "",
            "## Open Research Gaps",
            _format_markdown_list(gaps),
            "",
            "## Suggested Outreach Sequence",
            _format_markdown_list(suggested_sequence),
            "",
            "## Recommended Action",
            recommended_action or "Validate the buying committee, then tailor outreach by role.",
            "",
            "## Public Proof",
            _format_markdown_list([value.strip() for value in proof_urls.split(";") if value.strip()][:5]),
            "",
        ]
    )
    path.write_text(body, encoding="utf-8")


def _split_joined_values(value: str, *, limit: int | None = None) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").split(";"):
        cleaned = raw.strip()
        lowered = cleaned.lower()
        if not cleaned or lowered in seen:
            continue
        seen.add(lowered)
        items.append(cleaned)
    if limit is not None:
        return items[:limit]
    return items


def _budget_band(location_count: int, revenue_est: str, score: int) -> tuple[str, str]:
    cleaned = str(revenue_est or "").strip().lower()
    revenue_hint = 0.0
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([mk]?)", cleaned)
    if match:
        revenue_hint = float(match.group(1))
        if match.group(2) == "m":
            revenue_hint *= 1_000_000
        elif match.group(2) == "k":
            revenue_hint *= 1_000
    if location_count >= 8 or score >= 95 or revenue_hint >= 50_000_000:
        return "$100k+", "heuristic"
    if location_count >= 3 or score >= 80 or revenue_hint >= 10_000_000:
        return "$25k-$100k", "heuristic"
    if location_count >= 2 or score >= 65:
        return "$10k-$25k", "heuristic"
    return "$5k-$10k", "heuristic"


def _recommended_sales_sequence(contacts: list[dict[str, str]], gaps: list[str]) -> list[str]:
    sequence: list[str] = []
    ordered_buckets = ("operations", "buyer", "owner", "compliance", "finance", "store", "other")
    for bucket in ordered_buckets:
        match = next((contact for contact in contacts if contact.get("role_bucket") == bucket), None)
        if not match:
            continue
        role = str(match.get("role_label") or "Contact")
        person = str(match.get("name") or match.get("title") or role)
        if bucket == "operations":
            sequence.append(f"Start with {role}: {person}. Validate the operational problem and current workflow friction.")
        elif bucket == "buyer":
            sequence.append(f"Then move to {role}: {person}. Pressure-test purchasing and inventory implications.")
        elif bucket == "owner":
            sequence.append(f"Bring in {role}: {person} once the operational case is clear and tie it to growth or margin.")
        elif bucket == "compliance":
            sequence.append(f"Use {role}: {person} to de-risk the evaluation and answer reporting or audit concerns.")
        else:
            sequence.append(f"Use {role}: {person} as a supporting stakeholder in the buying process.")
    if "missing_buyer_contact" in gaps:
        sequence.append("Before sending a full pitch, confirm who actually owns inventory buying or purchasing.")
    if "missing_direct_email" in gaps or "missing_direct_phone" in gaps:
        sequence.append("Use the first live conversation to capture direct contact details for the real decision-maker.")
    if not sequence:
        sequence.append("Start with the strongest named contact, validate the buying path, then expand to the rest of the committee.")
    return sequence


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _external_research_status_payload(
    *,
    lead_id: str,
    report_id: str,
    company_name: str,
    report_relpath: str,
) -> dict[str, object]:
    return {
        "schema_version": "external_research.v1",
        "lead_id": lead_id,
        "report_id": report_id,
        "company_name": company_name,
        "status": "pending",
        "agent_name": "",
        "started_at": "",
        "completed_at": "",
        "updated_at": utcnow_iso(),
        "output_path": report_relpath,
        "source_count": 0,
        "last_error": "",
        "notes": "",
    }


def _write_lead_map_csv(path: Path, rows: list[dict[str, str]]) -> None:
    headers = [
        "lead_id",
        "dispensary",
        "website",
        "location",
        "locations",
        "pos_system",
        "compliance_system",
        "google_rating",
        "revenue_est",
        "contact_role",
        "name",
        "title",
        "linkedin",
        "profile",
        "company_strategy",
        "lead_package",
        "lead_summary",
        "report",
        "agent_brief",
        "agent_prompt",
        "external_research_status",
        "external_research_report",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def _write_outreach_sequence(
    path: Path,
    *,
    lead_id: str,
    company_name: str,
    sequence: list[str],
) -> None:
    body = "\n".join(
        [
            f"# Outreach Sequence - {company_name}",
            "",
            f"Lead ID: {lead_id}",
            "",
            "## Recommended Sequence",
            "\n".join(f"{idx}. {step}" for idx, step in enumerate(sequence, start=1)) or "1. Validate the buying committee.",
            "",
        ]
    )
    path.write_text(body, encoding="utf-8")


def _write_agent_research_brief(
    path: Path,
    *,
    lead_id: str,
    company_name: str,
    website: str,
    location_label: str,
    location_count: int,
    score: int,
    tier: str,
    research_status: str,
    gaps: list[str],
    target_roles: list[str],
    suggested_paths: list[str],
    proof_urls: list[str],
    packet_files: dict[str, object],
) -> None:
    body = "\n".join(
        [
            f"# Agent Research Brief - {company_name}",
            "",
            "## Objective",
            (
                f"Produce a lead-ready dossier for {lead_id} by verifying the company, expanding the buying committee, "
                "and enriching each relevant contact with source-backed public intelligence."
            ),
            "",
            "## Current Snapshot",
            _format_markdown_list(
                [
                    f"Website: {_website_url(website) or 'Not captured'}",
                    f"Primary market: {location_label or 'Unknown'}",
                    f"Locations: {location_count}",
                    f"Lead score / tier: {score} / {tier}",
                    f"Research status: {research_status or 'unknown'}",
                ]
            ),
            "",
            "## Research Priorities",
            _format_markdown_list(
                [
                    "Verify the company overview, store footprint, and operating stack.",
                    "Map the full decision network across owner, operations, compliance, and buyer roles.",
                    "Find role-specific context for each named contact using public sources only.",
                    "Capture expansion, hiring, budget, and competitive signals that affect outreach angle.",
                ]
            ),
            "",
            "## Open Gaps",
            _format_markdown_list(gaps),
            "",
            "## Target Roles To Verify",
            _format_markdown_list(target_roles),
            "",
            "## Public Paths To Check",
            _format_markdown_list(suggested_paths),
            "",
            "## Existing Public Proof",
            _format_markdown_list(proof_urls),
            "",
            "## Output Files To Update",
            _format_markdown_list(
                [
                    f"Lead summary: {packet_files['lead_summary']}",
                    f"Lead map: {packet_files['lead_map']}",
                    f"Company strategy: {packet_files['company_strategy']}",
                    f"Outreach sequence: {packet_files['outreach_sequence']}",
                    f"Report: {packet_files['report']}",
                    f"External research status: {packet_files['external_research_status']}",
                    f"External research report: {packet_files['external_research_report']}",
                    "Contact profiles under contacts/",
                ]
            ),
            "",
            "## Rules",
            _format_markdown_list(
                [
                    "Use public web sources only.",
                    "Add source URLs for every factual claim introduced.",
                    "If you cannot verify a fact, write unknown instead of guessing.",
                    "Keep company facts separate from inferred messaging guidance.",
                    "Set external_research_status.json to in_progress when work starts and completed only after the external report exists.",
                ]
            ),
            "",
        ]
    )
    path.write_text(body, encoding="utf-8")


def _write_agent_prompt(
    path: Path,
    *,
    lead_id: str,
    company_name: str,
    website: str,
    location_label: str,
    target_roles: list[str],
    suggested_paths: list[str],
    gaps: list[str],
    packet_files: dict[str, object],
) -> None:
    prompt = "\n".join(
        [
            f"You are the research agent for lead {lead_id}: {company_name}.",
            "",
            "Goal:",
            (
                "Turn this lead package into a source-backed sales intelligence dossier that can support personalized outreach "
                "to the company and its buying committee."
            ),
            "",
            "Use only public web sources.",
            "",
            "Tasks:",
            "1. Verify the company overview, market footprint, and operating stack.",
            "2. Identify or confirm the real decision network for software evaluation.",
            "3. Enrich each named contact with role context, public background, and likely priorities.",
            "4. Capture expansion, hiring, budget, and competitive signals that sharpen messaging.",
            "5. Update the report and strategy files with source-backed findings.",
            "",
            "Current known inputs:",
            f"- Company: {company_name}",
            f"- Website: {_website_url(website) or 'unknown'}",
            f"- Primary market: {location_label or 'unknown'}",
            f"- Target roles: {', '.join(target_roles) if target_roles else 'owner, operations, compliance, buyer'}",
            f"- Suggested public paths: {', '.join(suggested_paths) if suggested_paths else 'about, team, contact, menu, locations'}",
            f"- Open gaps: {', '.join(gaps) if gaps else 'none'}",
            "",
            "Deliverables to update:",
            f"- {packet_files['lead_summary']}",
            f"- {packet_files['lead_map']}",
            f"- {packet_files['company_strategy']}",
            f"- {packet_files['outreach_sequence']}",
            f"- {packet_files['report']}",
            f"- {packet_files['external_research_status']}",
            f"- {packet_files['external_research_report']}",
            "- contacts/*.md",
            "",
            "Output rules:",
            "- Add a source URL beside each newly verified fact.",
            "- Mark unverifiable facts as unknown.",
            "- Keep inferred messaging guidance clearly separate from factual observations.",
            "- Do not overwrite verified facts with lower-confidence guesses.",
            "- Set external_research_status.json to in_progress when you begin, then completed with completed_at and source_count after writing external_research_report.md.",
            "",
            "Start by reviewing the current package files, then research the company website and named contacts.",
            "",
        ]
    )
    path.write_text(prompt, encoding="utf-8")


def _write_report_markdown(
    path: Path,
    *,
    report_id: str,
    lead_id: str,
    company_name: str,
    location_label: str,
    location_count: int,
    score: int,
    tier: str,
    budget_band: str,
    budget_basis: str,
    research_status: str,
    contacts: list[dict[str, str]],
    sequence: list[str],
    packet_files: dict[str, object],
) -> None:
    contact_lines = [
        f"{contact['role_label']}: {contact['name'] or 'Unknown'} - {contact['title'] or 'Unverified'} ([profile]({packet_files['contacts_dir']}/{packet_files['contact_files'][idx]}))"
        for idx, contact in enumerate(contacts)
    ]
    body = "\n".join(
        [
            f"# Lead Intelligence Dossier - {company_name}",
            "",
            "## Report Details",
            _format_markdown_list(
                [
                    f"Report ID: {report_id}",
                    f"Lead ID: {lead_id}",
                    f"Prepared: {datetime.now().strftime('%B %d, %Y')}",
                    f"Primary market: {location_label or 'Unknown'}",
                    f"Status: {research_status or 'unknown'}",
                ]
            ),
            "",
            "## Snapshot",
            _format_markdown_list(
                [
                    f"Fit score: {score}/100 ({tier} tier)",
                    f"Locations: {location_count}",
                    f"Contacts mapped: {len(contacts)}",
                    f"Budget band: {budget_band} ({budget_basis})",
                ]
            ),
            "",
            "## Decision Network",
            _format_markdown_list(contact_lines),
            "",
            "## Recommended Sales Sequence",
            "\n".join(f"{idx}. {step}" for idx, step in enumerate(sequence, start=1)),
            "",
            "## Package Files",
            _format_markdown_list(
                [
                    f"Lead summary: [{Path(packet_files['lead_summary']).name}]({packet_files['lead_summary']})",
                    f"Lead map: [{Path(packet_files['lead_map']).name}]({packet_files['lead_map']})",
                    f"Company strategy: [{Path(packet_files['company_strategy']).name}]({packet_files['company_strategy']})",
                    f"Outreach sequence: [{Path(packet_files['outreach_sequence']).name}]({packet_files['outreach_sequence']})",
                    f"Agent brief: [{Path(packet_files['agent_brief']).name}]({packet_files['agent_brief']})",
                    f"Agent prompt: [{Path(packet_files['agent_prompt']).name}]({packet_files['agent_prompt']})",
                    f"External research status: [{Path(packet_files['external_research_status']).name}]({packet_files['external_research_status']})",
                ]
            ),
            "",
        ]
    )
    path.write_text(body, encoding="utf-8")


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


def _copy_latest_csv(src: Path, dest: Path) -> None:
    if not src.exists():
        return
    dest.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def export_lead_intelligence_dossier(
    con,
    out_dir: Path,
    *,
    cfg: CrawlConfig,
    tier: str = "A",
    limit: int = 100,
    run_id: str = "",
) -> dict[str, object]:
    dossier_dir = out_dir / "lead_intelligence"
    packages_dir = dossier_dir / "leads"
    dossier_dir.mkdir(parents=True, exist_ok=True)
    packages_dir.mkdir(parents=True, exist_ok=True)

    headers = [
        "lead_id",
        "dispensary",
        "website",
        "location",
        "locations",
        "pos_system",
        "compliance_system",
        "google_rating",
        "revenue_est",
        "contact_role",
        "name",
        "title",
        "linkedin",
        "profile",
        "company_strategy",
        "lead_package",
        "lead_summary",
        "report",
        "agent_brief",
        "agent_prompt",
        "agent_packet",
        "external_research_status",
        "external_research_report",
    ]

    if int(limit) <= 0:
        empty_index = dossier_dir / "lead_intelligence_index.csv"
        empty_table = dossier_dir / "lead_intelligence_table.md"
        empty_manifest = dossier_dir / "lead_intelligence_manifest.json"
        with empty_index.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        empty_table.write_text("# Dispensary Lead Intelligence Table\n\n_No rows generated._\n", encoding="utf-8")
        _write_json(
            empty_manifest,
            {
                "generated_at": utcnow_iso(),
                "run_id": run_id,
                "company_count": 0,
                "row_count": 0,
                "package_count": 0,
                "external_research_contract_version": "external_research.v1",
                "packages": [],
            },
        )
        return {
            "index_csv": str(empty_index),
            "table_md": str(empty_table),
            "manifest_json": str(empty_manifest),
            "profiles_dir": str(packages_dir),
            "packages_dir": str(packages_dir),
            "company_count": 0,
            "package_count": 0,
            "row_count": 0,
            "generated_at": utcnow_iso(),
            "run_id": run_id,
        }

    active_location_count = con.execute(
        """
        SELECT COUNT(*) AS c
        FROM locations
        WHERE COALESCE(deleted_at,'')=''
          AND COALESCE(website_domain,'')<>''
        """
    ).fetchone()
    brief_limit = int((active_location_count["c"] if active_location_count else 0) or 0)
    briefs = build_lead_research_briefs(
        con,
        cfg=cfg,
        since="1970-01-01T00:00:00",
        min_score=0,
        limit=max(1, brief_limit),
    )
    brief_by_location = {brief.location_pk: brief for brief in briefs}

    tier_order = {"A": 3, "B": 2, "C": 1}
    min_tier = tier_order.get(tier, 3)
    rows = con.execute(
        """
        SELECT l.location_pk,
               l.org_pk,
               l.canonical_name,
               l.website_domain,
               l.city,
               l.state,
               l.updated_at,
               COALESCE(o.dba_name, '') AS dba_name,
               COALESCE(o.legal_name, '') AS legal_name
        FROM locations l
        JOIN organizations o ON o.org_pk = l.org_pk
        WHERE COALESCE(l.deleted_at,'')=''
          AND COALESCE(l.website_domain,'')<>''
        ORDER BY l.fit_score DESC, l.updated_at DESC
        """
    ).fetchall()

    selected_orgs: list[dict[str, object]] = []
    seen_orgs: set[str] = set()
    for row in rows:
        org_pk = str(row["org_pk"] or "")
        if not org_pk or org_pk in seen_orgs:
            continue
        location_pk = str(row["location_pk"] or "")
        score, current_tier = _active_score(con, location_pk)
        if tier_order.get(current_tier, 1) < min_tier:
            continue
        segment, _ = _segment_company(str(row["canonical_name"] or ""), str(row["website_domain"] or ""))
        if segment != "dispensary":
            continue
        seen_orgs.add(org_pk)
        selected_orgs.append(
            {
                "org_pk": org_pk,
                "location_pk": location_pk,
                "company_name": str(row["dba_name"] or row["canonical_name"] or row["legal_name"] or ""),
                "website": str(row["website_domain"] or ""),
                "location": _display_location(str(row["city"] or ""), str(row["state"] or "")),
                "score": score,
                "tier": current_tier,
            }
        )
        if len(selected_orgs) >= int(limit):
            break

    index_rows: list[dict[str, str]] = []
    package_records: list[dict[str, object]] = []
    for index, candidate in enumerate(selected_orgs, start=1):
        lead_id = f"DISP{index:03d}"
        report_id = f"CR-{lead_id}"
        location_pk = str(candidate["location_pk"])
        org_pk = str(candidate["org_pk"])
        location_count = _org_location_count(con, org_pk)
        brief = brief_by_location.get(location_pk)
        gaps = list(brief.gaps) if brief else []
        research_status = brief.research_status if brief else "unknown"
        recommended_action = brief.recommended_action if brief else ""
        proof_urls = brief.proof_urls if brief else _proof_urls(con, location_pk)
        social_urls = brief.social_urls if brief else ""
        menu_provider = brief.menu_provider if brief else _menu_provider_for_location(con, location_pk)
        target_roles = list(brief.target_roles) if brief else []
        suggested_paths = list(brief.suggested_paths) if brief else list(cfg.agent_research_paths)
        compliance_system = _first_evidence_value(
            con,
            location_pk,
            ("compliance_system", "track_trace_system", "seed_to_sale_system"),
        )
        google_rating = _first_evidence_value(
            con,
            location_pk,
            ("google_rating", "google_maps_rating", "google_review_rating"),
        )
        revenue_est = _first_evidence_value(
            con,
            location_pk,
            ("revenue_estimate", "estimated_revenue", "revenue_est"),
        )
        budget_band, budget_basis = _budget_band(location_count, revenue_est, int(candidate["score"]))
        company_dir = packages_dir / f"{lead_id.lower()}-{_slugify(str(candidate['company_name']))}"
        contacts_dir = company_dir / "contacts"
        contacts_dir.mkdir(parents=True, exist_ok=True)

        contacts = _decision_network_contacts(con, org_pk, location_pk)
        if not contacts:
            fallback_title = brief.target_roles[0] if brief and brief.target_roles else "General Manager"
            bucket = _role_bucket(fallback_title)
            contacts = [
                {
                    "location_pk": location_pk,
                    "name": "",
                    "title": fallback_title,
                    "email": brief.email if brief else "",
                    "phone": brief.phone if brief else _best_phone(con, location_pk),
                    "role_bucket": bucket,
                    "role_label": _role_bucket_label(bucket),
                    "location_name": str(candidate["company_name"]),
                    "location": str(candidate["location"]),
                    "linkedin": "",
                }
            ]
        if not target_roles:
            target_roles = [str(contact["title"] or contact["role_label"]) for contact in contacts]

        proof_list = _split_joined_values(proof_urls, limit=6)
        social_list = _split_joined_values(social_urls, limit=6)
        sequence = _recommended_sales_sequence(contacts, gaps)

        contact_files: list[str] = []
        lead_rows: list[dict[str, str]] = []
        for contact in contacts:
            profile_name = contact["name"] or contact["title"] or contact["role_label"]
            profile_path = contacts_dir / f"{_slugify(profile_name)}-{_slugify(contact['role_label'])}.md"
            _write_contact_profile(
                profile_path,
                lead_id=lead_id,
                company_name=str(candidate["company_name"]),
                website=str(candidate["website"]),
                location_label=str(candidate["location"]),
                location_count=location_count,
                score=int(candidate["score"]),
                tier=str(candidate["tier"]),
                menu_provider=menu_provider,
                compliance_system=compliance_system,
                google_rating=google_rating,
                revenue_est=revenue_est,
                research_status=research_status,
                gaps=gaps,
                proof_urls=proof_urls,
                social_urls=social_urls,
                contact=contact,
            )
            contact_file_rel = str(profile_path.relative_to(company_dir))
            contact_files.append(contact_file_rel)
            lead_rows.append(
                {
                    "lead_id": lead_id,
                    "dispensary": str(candidate["company_name"]),
                    "website": _website_url(str(candidate["website"])),
                    "location": str(candidate["location"]),
                    "locations": str(location_count),
                    "pos_system": menu_provider,
                    "compliance_system": compliance_system,
                    "google_rating": google_rating,
                    "revenue_est": revenue_est,
                    "contact_role": str(contact["role_label"]),
                    "name": str(contact["name"]),
                    "title": str(contact["title"]),
                    "linkedin": str(contact["linkedin"]),
                    "profile": str(profile_path.relative_to(dossier_dir)),
                    "company_strategy": str((company_dir / "company-strategy.md").relative_to(dossier_dir)),
                    "lead_package": str(company_dir.relative_to(dossier_dir)),
                    "lead_summary": str((company_dir / "lead_summary.json").relative_to(dossier_dir)),
                    "report": str((company_dir / "report.md").relative_to(dossier_dir)),
                    "agent_brief": str((company_dir / "agent_research_brief.md").relative_to(dossier_dir)),
                    "agent_prompt": str((company_dir / "agent_research_prompt.md").relative_to(dossier_dir)),
                    "agent_packet": str((company_dir / "agent_research_packet.json").relative_to(dossier_dir)),
                    "external_research_status": str((company_dir / "external_research_status.json").relative_to(dossier_dir)),
                    "external_research_report": str((company_dir / "external_research_report.md").relative_to(dossier_dir)),
                }
            )

        packet_files = {
            "lead_summary": "lead_summary.json",
            "lead_map": "lead_map.csv",
            "company_strategy": "company-strategy.md",
            "outreach_sequence": "outreach_sequence.md",
            "agent_brief": "agent_research_brief.md",
            "agent_prompt": "agent_research_prompt.md",
            "agent_packet": "agent_research_packet.json",
            "external_research_status": "external_research_status.json",
            "external_research_report": "external_research_report.md",
            "report": "report.md",
            "contacts_dir": "contacts",
            "contact_files": contact_files,
        }

        _write_company_strategy(
            company_dir / "company-strategy.md",
            lead_id=lead_id,
            company_name=str(candidate["company_name"]),
            website=str(candidate["website"]),
            location_label=str(candidate["location"]),
            location_count=location_count,
            score=int(candidate["score"]),
            tier=str(candidate["tier"]),
            menu_provider=menu_provider,
            compliance_system=compliance_system,
            google_rating=google_rating,
            revenue_est=revenue_est,
            research_status=research_status,
            gaps=gaps,
            recommended_action=recommended_action,
            proof_urls=proof_urls,
            contacts=contacts,
        )

        _write_lead_map_csv(company_dir / "lead_map.csv", lead_rows)
        _write_outreach_sequence(
            company_dir / "outreach_sequence.md",
            lead_id=lead_id,
            company_name=str(candidate["company_name"]),
            sequence=sequence,
        )

        lead_summary_payload: dict[str, object] = {
            "lead_id": lead_id,
            "report_id": report_id,
            "company_name": str(candidate["company_name"]),
            "website": _website_url(str(candidate["website"])),
            "location": str(candidate["location"]),
            "location_count": location_count,
            "score": int(candidate["score"]),
            "tier": str(candidate["tier"]),
            "research_status": research_status,
            "recommended_action": recommended_action,
            "budget_band": budget_band,
            "budget_basis": budget_basis,
            "pos_system": menu_provider,
            "compliance_system": compliance_system,
            "google_rating": google_rating,
            "revenue_est": revenue_est,
            "gaps": gaps,
            "target_roles": target_roles,
            "suggested_paths": suggested_paths,
            "proof_urls": proof_list,
            "social_urls": social_list,
            "sales_sequence": sequence,
            "contacts": [
                {
                    "role": str(contact["role_label"]),
                    "name": str(contact["name"]),
                    "title": str(contact["title"]),
                    "email": str(contact["email"]),
                    "phone": str(contact["phone"]),
                    "linkedin": str(contact["linkedin"]),
                    "profile": contact_files[idx],
                }
                for idx, contact in enumerate(contacts)
            ],
        }
        _write_json(company_dir / "lead_summary.json", lead_summary_payload)

        _write_json(
            company_dir / "external_research_status.json",
            _external_research_status_payload(
                lead_id=lead_id,
                report_id=report_id,
                company_name=str(candidate["company_name"]),
                report_relpath=packet_files["external_research_report"],
            ),
        )

        agent_packet_payload: dict[str, object] = {
            "lead_id": lead_id,
            "report_id": report_id,
            "company_name": str(candidate["company_name"]),
            "website": _website_url(str(candidate["website"])),
            "location": str(candidate["location"]),
            "research_status": research_status,
            "gaps": gaps,
            "target_roles": target_roles,
            "suggested_paths": suggested_paths,
            "proof_urls": proof_list,
            "deliverables": {
                "lead_summary": packet_files["lead_summary"],
                "lead_map": packet_files["lead_map"],
                "company_strategy": packet_files["company_strategy"],
                "outreach_sequence": packet_files["outreach_sequence"],
                "report": packet_files["report"],
                "agent_brief": packet_files["agent_brief"],
                "agent_prompt": packet_files["agent_prompt"],
                "external_research_status": packet_files["external_research_status"],
                "external_research_report": packet_files["external_research_report"],
                "contacts_dir": packet_files["contacts_dir"],
            },
            "external_completion_contract": {
                "status_file": packet_files["external_research_status"],
                "report_file": packet_files["external_research_report"],
                "valid_status_values": ["pending", "in_progress", "completed", "failed"],
                "completion_rule": (
                    "Set status to completed only after external_research_report.md exists and completed_at/source_count are populated."
                ),
            },
            "tasks": [
                "Verify company overview, footprint, and operational stack.",
                "Map or confirm owner, operations, compliance, and buyer roles.",
                "Research named contacts using public web sources.",
                "Capture expansion, hiring, budget, and competitive signals.",
                "Update the dossier files with source-backed findings.",
            ],
            "rules": [
                "Use public web sources only.",
                "Record a source URL for every factual addition.",
                "Write unknown when a fact cannot be verified.",
            ],
        }
        _write_json(company_dir / "agent_research_packet.json", agent_packet_payload)

        _write_agent_research_brief(
            company_dir / "agent_research_brief.md",
            lead_id=lead_id,
            company_name=str(candidate["company_name"]),
            website=str(candidate["website"]),
            location_label=str(candidate["location"]),
            location_count=location_count,
            score=int(candidate["score"]),
            tier=str(candidate["tier"]),
            research_status=research_status,
            gaps=gaps,
            target_roles=target_roles,
            suggested_paths=suggested_paths,
            proof_urls=proof_list,
            packet_files=packet_files,
        )
        _write_agent_prompt(
            company_dir / "agent_research_prompt.md",
            lead_id=lead_id,
            company_name=str(candidate["company_name"]),
            website=str(candidate["website"]),
            location_label=str(candidate["location"]),
            target_roles=target_roles,
            suggested_paths=suggested_paths,
            gaps=gaps,
            packet_files=packet_files,
        )
        _write_report_markdown(
            company_dir / "report.md",
            report_id=report_id,
            lead_id=lead_id,
            company_name=str(candidate["company_name"]),
            location_label=str(candidate["location"]),
            location_count=location_count,
            score=int(candidate["score"]),
            tier=str(candidate["tier"]),
            budget_band=budget_band,
            budget_basis=budget_basis,
            research_status=research_status,
            contacts=contacts,
            sequence=sequence,
            packet_files=packet_files,
        )

        index_rows.extend(lead_rows)
        package_records.append(
            {
                "lead_id": lead_id,
                "report_id": report_id,
                "company_name": str(candidate["company_name"]),
                "package_dir": str(company_dir.relative_to(dossier_dir)),
                "report": str((company_dir / "report.md").relative_to(dossier_dir)),
                "agent_prompt": str((company_dir / "agent_research_prompt.md").relative_to(dossier_dir)),
                "agent_packet": str((company_dir / "agent_research_packet.json").relative_to(dossier_dir)),
                "lead_summary": str((company_dir / "lead_summary.json").relative_to(dossier_dir)),
                "external_research_status": str((company_dir / "external_research_status.json").relative_to(dossier_dir)),
                "external_research_report": str((company_dir / "external_research_report.md").relative_to(dossier_dir)),
                "contact_count": len(contacts),
                "location_count": location_count,
                "research_status": research_status,
                "gaps": gaps,
            }
        )

    index_path = dossier_dir / "lead_intelligence_index.csv"
    with index_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in index_rows:
            writer.writerow(row)

    table_lines = [
        "# Dispensary Lead Intelligence Table",
        "",
        "Top table = navigational map. Profiles = deep intelligence for messaging and sequencing.",
        "",
        "| Lead ID | Dispensary | Website | Location | Locations | POS | Compliance System | Google Rating | Revenue Est | Contact Role | Name | Title | LinkedIn | Profile | Company Strategy | Report |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in index_rows:
        website_value = row["website"]
        website_cell = f"[{website_value}]({website_value})" if website_value else ""
        linkedin_value = row["linkedin"]
        linkedin_cell = f"[link]({linkedin_value})" if linkedin_value else ""
        profile_value = row["profile"]
        strategy_value = row["company_strategy"]
        report_value = row["report"]
        profile_cell = f"[{Path(profile_value).name}]({profile_value})" if profile_value else ""
        strategy_cell = f"[{Path(strategy_value).name}]({strategy_value})" if strategy_value else ""
        report_cell = f"[{Path(report_value).name}]({report_value})" if report_value else ""
        table_lines.append(
            "| "
            + " | ".join(
                [
                    row["lead_id"],
                    row["dispensary"],
                    website_cell,
                    row["location"],
                    row["locations"],
                    row["pos_system"],
                    row["compliance_system"],
                    row["google_rating"],
                    row["revenue_est"],
                    row["contact_role"],
                    row["name"],
                    row["title"],
                    linkedin_cell,
                    profile_cell,
                    strategy_cell,
                    report_cell,
                ]
            )
            + " |"
        )
    table_path = dossier_dir / "lead_intelligence_table.md"
    table_path.write_text("\n".join(table_lines) + "\n", encoding="utf-8")
    manifest_path = dossier_dir / "lead_intelligence_manifest.json"
    _write_json(
        manifest_path,
        {
            "generated_at": utcnow_iso(),
            "run_id": run_id,
            "company_count": len(selected_orgs),
            "row_count": len(index_rows),
            "package_count": len(package_records),
            "external_research_contract_version": "external_research.v1",
            "packages": package_records,
        },
    )

    return {
        "index_csv": str(index_path),
        "table_md": str(table_path),
        "manifest_json": str(manifest_path),
        "profiles_dir": str(packages_dir),
        "packages_dir": str(packages_dir),
        "company_count": len(selected_orgs),
        "package_count": len(package_records),
        "row_count": len(index_rows),
        "generated_at": utcnow_iso(),
        "run_id": run_id,
    }


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


def export_agent_research_queue(
    con,
    out_dir: Path,
    *,
    cfg: CrawlConfig,
    since: str | None = None,
    limit: int = 200,
    min_score: int | None = None,
    run_id: str = "",
) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "agent_research_queue.csv"
    headers = [
        "company_name",
        "website",
        "state",
        "score",
        "tier",
        "research_status",
        "contact_name",
        "contact_title",
        "email",
        "phone",
        "menu_provider",
        "proof_urls",
        "social_urls",
        "gaps",
        "target_roles",
        "suggested_paths",
        "recommended_action",
        "enhancement_summary",
    ]
    briefs = []
    if int(limit) > 0:
        briefs = build_lead_research_briefs(
            con,
            cfg=cfg,
            since=since,
            min_score=int(min_score if min_score is not None else cfg.agent_research_min_score),
            limit=limit,
        )
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for brief in briefs:
            writer.writerow(
                {
                    "company_name": brief.company_name,
                    "website": brief.website,
                    "state": brief.state,
                    "score": str(brief.score),
                    "tier": brief.tier,
                    "research_status": brief.research_status,
                    "contact_name": brief.contact_name,
                    "contact_title": brief.contact_title,
                    "email": brief.email,
                    "phone": brief.phone,
                    "menu_provider": brief.menu_provider,
                    "proof_urls": brief.proof_urls,
                    "social_urls": brief.social_urls,
                    "gaps": "; ".join(brief.gaps),
                    "target_roles": "; ".join(brief.target_roles),
                    "suggested_paths": "; ".join(brief.suggested_paths),
                    "recommended_action": brief.recommended_action,
                    "enhancement_summary": brief.enhancement_summary,
                }
            )
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
