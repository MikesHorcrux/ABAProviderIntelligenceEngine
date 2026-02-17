from __future__ import annotations

from dataclasses import dataclass

from pipeline.utils import make_pk, normalize_text, utcnow_iso


FEATURE_KEYS = (
    "has_named_buyer_contact",
    "has_role_inbox",
    "has_direct_email",
    "menu_provider_detected",
    "sku_complexity_proxy",
    "multi_location",
    "enterprise_chain_signal",
    "has_direct_phone",
    "has_website",
)


@dataclass(frozen=True)
class ScoreInputs:
    location_pk: str
    has_named_buyer_contact: bool
    has_role_inbox: bool
    has_direct_email: bool
    menu_provider: str
    sku_complexity: int
    multi_location: bool
    enterprise_chain_signal: bool
    has_direct_phone: bool
    has_website: bool


def _is_buyer_like(contact_name: str, role: str) -> bool:
    text = normalize_text(f"{contact_name} {role}").lower()
    return any(x in text for x in ("buyer", "purchasing", "inventory", "operations", "owner", "gm", "general manager"))


def score_location(inputs: ScoreInputs) -> tuple[int, str, dict[str, float]]:
    features: dict[str, float] = {
        "has_named_buyer_contact": 1.0 if inputs.has_named_buyer_contact else 0.0,
        "has_role_inbox": 1.0 if inputs.has_role_inbox else 0.0,
        "has_direct_email": 1.0 if inputs.has_direct_email else 0.0,
        "menu_provider_detected": 1.0 if bool(inputs.menu_provider) else 0.0,
        "sku_complexity_proxy": min(max(inputs.sku_complexity, 0), 1),
        "multi_location": 1.0 if inputs.multi_location else 0.0,
        "enterprise_chain_signal": 1.0 if inputs.enterprise_chain_signal else 0.0,
        "has_direct_phone": 1.0 if inputs.has_direct_phone else 0.0,
        "has_website": 1.0 if inputs.has_website else 0.0,
    }

    score = 0
    score += 30 if inputs.has_named_buyer_contact else 0
    score += 16 if inputs.has_role_inbox else 0
    score += 16 if inputs.has_direct_email else 0
    score += 14 if inputs.has_direct_phone else 0
    score += 10 if inputs.has_website else 0
    score += 14 if inputs.menu_provider else 0
    score += 8 if inputs.sku_complexity else 0
    score += 8 if inputs.multi_location else 0
    score -= 20 if inputs.enterprise_chain_signal else 0

    total = max(0, min(100, score))
    if total >= 72:
        tier = "A"
    elif total >= 48:
        tier = "B"
    else:
        tier = "C"
    return total, tier, features


def _parse_tier_chain(signal: str) -> bool:
    txt = (signal or "").lower()
    if not txt:
        return False
    bad = ("holding", "group", "corporation", "corporate", "inc.", "llc", "enterprise", "brands", "distributor", "chain")
    return any(x in txt for x in bad)


def run_score(con):
    now = utcnow_iso()
    rows = con.execute(
        "SELECT location_pk, canonical_name, website_domain, fit_score, state FROM locations WHERE COALESCE(deleted_at,'')=''"
    ).fetchall()
    for row in rows:
        loc_pk = row["location_pk"]
        emails = con.execute(
            "SELECT value FROM contact_points WHERE location_pk=? AND type='email' AND value<>''",
            (loc_pk,),
        ).fetchall()
        has_email = bool(emails)

        phone = con.execute(
            "SELECT value FROM contact_points WHERE location_pk=? AND type='phone' AND value<>'' LIMIT 1",
            (loc_pk,),
        ).fetchone()
        contacts = con.execute(
            "SELECT full_name, role FROM contacts WHERE location_pk=? AND COALESCE(deleted_at,'')='' AND full_name<>''",
            (loc_pk,),
        ).fetchall()

        has_named_buyer = any(_is_buyer_like(r["full_name"], r["role"]) for r in contacts)
        has_role_inbox = any(bool((r["role"] or "").strip()) for r in contacts)

        provider_row = con.execute(
            "SELECT field_value FROM evidence WHERE entity_type='location' AND entity_pk=? AND field_name='menu_provider' LIMIT 1",
            (loc_pk,),
        ).fetchone()
        provider = provider_row["field_value"] if provider_row else ""

        org_pk = con.execute("SELECT org_pk FROM locations WHERE location_pk=?", (loc_pk,)).fetchone()["org_pk"]
        location_count = con.execute(
            "SELECT COUNT(*) AS c FROM locations WHERE org_pk=? AND COALESCE(deleted_at,'')=''",
            (org_pk,),
        ).fetchone()["c"]

        enterprise_chain = _parse_tier_chain(row["canonical_name"] or "")
        feature_input = ScoreInputs(
            location_pk=loc_pk,
            has_named_buyer_contact=has_named_buyer,
            has_role_inbox=has_role_inbox,
            has_direct_email=has_email,
            menu_provider=provider,
            sku_complexity=1 if provider else 0,
            multi_location=location_count > 1,
            enterprise_chain_signal=enterprise_chain,
            has_direct_phone=bool(phone and phone[0]),
            has_website=bool(row["website_domain"]),
        )
        total, tier, features = score_location(feature_input)

        score_pk = make_pk("ls", [loc_pk, now])
        con.execute(
            "INSERT OR REPLACE INTO lead_scores (score_pk, location_pk, score_total, tier, run_id, created_at, as_of, deleted_at) VALUES (?,?,?,?,?,?,?,'')",
            (score_pk, loc_pk, total, tier, now, now, now),
        )
        for key in FEATURE_KEYS:
            con.execute(
                """
                INSERT OR REPLACE INTO scoring_features
                (feature_pk, score_pk, feature_name, feature_value, created_at)
                VALUES (?,?,?,?,?)
                """,
                (make_pk("sf", [score_pk, key]), score_pk, key, float(features[key]), now),
            )
        con.execute(
            "UPDATE locations SET fit_score = ?, updated_at=?, last_seen_at=? WHERE location_pk=?",
            (total, now, now, loc_pk),
        )
    con.commit()
