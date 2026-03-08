from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from pipeline.config import CrawlConfig
from pipeline.utils import make_pk, utcnow_iso


DEFAULT_RESEARCH_PATHS = (
    "/about",
    "/team",
    "/leadership",
    "/our-team",
    "/staff",
    "/contact",
    "/menu",
    "/locations",
    "/careers",
    "/jobs",
    "/brands",
    "/vendors",
)


@dataclass(frozen=True)
class LeadResearchBrief:
    location_pk: str
    company_name: str
    website: str
    state: str
    score: int
    tier: str
    contact_name: str
    contact_title: str
    email: str
    phone: str
    menu_provider: str
    proof_urls: str
    social_urls: str
    research_status: str
    gaps: list[str]
    target_roles: list[str]
    suggested_paths: list[str]
    recommended_action: str
    enhancement_summary: str


def _latest_scored_locations(
    con,
    *,
    since: str | None,
    min_score: int,
    limit: int,
) -> list[dict[str, object]]:
    cutoff = since or (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")
    return [
        dict(row)
        for row in con.execute(
            """
            SELECT l.location_pk,
                   l.canonical_name,
                   l.website_domain,
                   l.state,
                   l.created_at,
                   l.updated_at,
                   l.last_seen_at,
                   COALESCE((
                     SELECT ls.score_total
                     FROM lead_scores ls
                     WHERE ls.location_pk = l.location_pk
                       AND COALESCE(ls.deleted_at, '') = ''
                     ORDER BY ls.as_of DESC
                     LIMIT 1
                   ), 0) AS score_total,
                   COALESCE((
                     SELECT ls.tier
                     FROM lead_scores ls
                     WHERE ls.location_pk = l.location_pk
                       AND COALESCE(ls.deleted_at, '') = ''
                     ORDER BY ls.as_of DESC
                     LIMIT 1
                   ), 'C') AS tier
            FROM locations l
            WHERE COALESCE(l.deleted_at, '') = ''
              AND COALESCE(l.website_domain, '') <> ''
              AND (
                COALESCE(l.created_at, '') >= ?
                OR COALESCE(l.updated_at, '') >= ?
                OR COALESCE(l.last_seen_at, '') >= ?
              )
              AND COALESCE((
                SELECT ls.score_total
                FROM lead_scores ls
                WHERE ls.location_pk = l.location_pk
                  AND COALESCE(ls.deleted_at, '') = ''
                ORDER BY ls.as_of DESC
                LIMIT 1
              ), 0) >= ?
            ORDER BY score_total DESC, COALESCE(l.updated_at, '') DESC
            LIMIT ?
            """,
            (cutoff, cutoff, cutoff, int(min_score), max(1, int(limit))),
        ).fetchall()
    ]


def _first_row_value(con, query: str, params: Iterable[object]) -> str:
    row = con.execute(query, tuple(params)).fetchone()
    if not row:
        return ""
    value = row[0]
    return str(value or "")


def _best_contact(con, location_pk: str) -> tuple[str, str, str]:
    row = con.execute(
        """
        SELECT full_name, role, email
        FROM contacts
        WHERE location_pk = ?
          AND COALESCE(deleted_at, '') = ''
        ORDER BY confidence DESC, updated_at DESC
        LIMIT 1
        """,
        (location_pk,),
    ).fetchone()
    if not row:
        return "", "", ""
    return str(row["full_name"] or ""), str(row["role"] or ""), str(row["email"] or "")


def _best_phone(con, location_pk: str) -> str:
    return _first_row_value(
        con,
        """
        SELECT value
        FROM contact_points
        WHERE location_pk = ?
          AND type = 'phone'
          AND COALESCE(value, '') <> ''
          AND COALESCE(deleted_at, '') = ''
        ORDER BY confidence DESC, updated_at DESC
        LIMIT 1
        """,
        (location_pk,),
    )


def _menu_provider(con, location_pk: str) -> str:
    return _first_row_value(
        con,
        """
        SELECT field_value
        FROM evidence
        WHERE entity_type = 'location'
          AND entity_pk = ?
          AND field_name = 'menu_provider'
          AND COALESCE(deleted_at, '') = ''
        ORDER BY captured_at DESC
        LIMIT 1
        """,
        (location_pk,),
    )


def _joined_values(con, location_pk: str, field_name: str, *, limit: int = 5) -> str:
    rows = con.execute(
        """
        SELECT field_value
        FROM evidence
        WHERE entity_type = 'location'
          AND entity_pk = ?
          AND field_name = ?
          AND COALESCE(deleted_at, '') = ''
          AND COALESCE(field_value, '') <> ''
        ORDER BY captured_at DESC
        LIMIT ?
        """,
        (location_pk, field_name, limit),
    ).fetchall()
    values = []
    seen: set[str] = set()
    for row in rows:
        value = str(row["field_value"] or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return "; ".join(values)


def _has_buyer_contact(con, location_pk: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM contacts
        WHERE location_pk = ?
          AND COALESCE(deleted_at, '') = ''
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


def _has_contact_point(con, location_pk: str, type_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM contact_points
        WHERE location_pk = ?
          AND type = ?
          AND COALESCE(value, '') <> ''
          AND COALESCE(deleted_at, '') = ''
        LIMIT 1
        """,
        (location_pk, type_name),
    ).fetchone()
    return row is not None


def _status_for_gaps(gaps: list[str], *, has_buyer: bool, has_email: bool, has_phone: bool) -> str:
    if not gaps:
        return "ready"
    if has_buyer and (has_email or has_phone):
        return "enhanced"
    if has_email or has_phone:
        return "contactable"
    return "research_needed"


def _target_roles(has_buyer: bool, contact_title: str) -> list[str]:
    if has_buyer and contact_title:
        return [contact_title]
    ordered = [
        "Inventory Manager",
        "Purchasing Manager",
        "General Manager",
        "Owner",
    ]
    if contact_title:
        ordered.insert(0, contact_title)
    seen: set[str] = set()
    result: list[str] = []
    for value in ordered:
        cleaned = str(value).strip()
        lowered = cleaned.lower()
        if not cleaned or lowered in seen:
            continue
        seen.add(lowered)
        result.append(cleaned)
    return result[:4]


def _research_paths(cfg: CrawlConfig) -> list[str]:
    merged = list(cfg.extra_paths) + list(cfg.agent_research_paths)
    result: list[str] = []
    seen: set[str] = set()
    for path in merged:
        cleaned = str(path).strip()
        if not cleaned.startswith("/"):
            cleaned = f"/{cleaned}"
        lowered = cleaned.lower()
        if not cleaned or lowered in seen:
            continue
        seen.add(lowered)
        result.append(cleaned)
    return result


def _build_recommended_action(gaps: list[str], target_roles: list[str], website: str) -> str:
    if not gaps:
        return "Lead is ready for outreach. Use proof URLs and the strongest contact signal for first contact."
    role_hint = ", ".join(target_roles[:2]) if target_roles else "store leadership"
    focus = []
    if "missing_buyer_contact" in gaps:
        focus.append(f"identify {role_hint}")
    if "missing_direct_email" in gaps:
        focus.append("confirm a direct email")
    if "missing_direct_phone" in gaps:
        focus.append("confirm a direct phone line")
    if "missing_menu_provider" in gaps:
        focus.append("confirm the online menu stack")
    if "missing_social_signal" in gaps:
        focus.append("capture social proof")
    if not focus:
        focus.append("review latest public proof")
    website_hint = website or "the public site"
    return f"Research {website_hint} and close these gaps: {', '.join(focus)}."


def _build_summary(
    *,
    score: int,
    tier: str,
    has_buyer: bool,
    has_email: bool,
    has_phone: bool,
    menu_provider: str,
    gaps: list[str],
) -> str:
    strengths = []
    if has_buyer:
        strengths.append("buyer-like contact")
    if has_email:
        strengths.append("email")
    if has_phone:
        strengths.append("phone")
    if menu_provider:
        strengths.append(f"menu provider {menu_provider}")
    strength_text = ", ".join(strengths) if strengths else "no strong contact signals yet"
    if not gaps:
        return f"{tier}-tier lead scored {score} with {strength_text}. Ready for outreach."
    return f"{tier}-tier lead scored {score} with {strength_text}. Remaining gaps: {', '.join(gaps)}."


def build_lead_research_briefs(
    con,
    *,
    cfg: CrawlConfig,
    since: str | None,
    min_score: int,
    limit: int,
) -> list[LeadResearchBrief]:
    briefs: list[LeadResearchBrief] = []
    for row in _latest_scored_locations(con, since=since, min_score=min_score, limit=limit):
        location_pk = str(row["location_pk"])
        company_name = str(row["canonical_name"] or "")
        website = str(row["website_domain"] or "")
        state = str(row["state"] or "")
        score = int(row["score_total"] or 0)
        tier = str(row["tier"] or "C")
        contact_name, contact_title, email = _best_contact(con, location_pk)
        phone = _best_phone(con, location_pk)
        menu_provider = _menu_provider(con, location_pk)
        proof_urls = _joined_values(con, location_pk, "social_url", limit=3)
        evidence_urls = _joined_values(con, location_pk, "menu_provider", limit=1)
        source_urls = [
            str(item["source_url"] or "").strip()
            for item in con.execute(
                """
                SELECT source_url
                FROM evidence
                WHERE entity_type = 'location'
                  AND entity_pk = ?
                  AND COALESCE(source_url, '') <> ''
                  AND COALESCE(deleted_at, '') = ''
                ORDER BY captured_at DESC
                LIMIT 6
                """,
                (location_pk,),
            ).fetchall()
        ]
        deduped_source_urls = []
        seen_source_urls: set[str] = set()
        for value in source_urls:
            if not value or value in seen_source_urls:
                continue
            seen_source_urls.add(value)
            deduped_source_urls.append(value)
        proof_bundle = "; ".join(deduped_source_urls)
        social_urls = _joined_values(con, location_pk, "social_url", limit=4)

        has_buyer = _has_buyer_contact(con, location_pk)
        has_email = _has_contact_point(con, location_pk, "email") or bool(email)
        has_phone = _has_contact_point(con, location_pk, "phone") or bool(phone)
        has_menu = bool(menu_provider or evidence_urls)
        has_social = bool(social_urls)

        gaps: list[str] = []
        if not has_buyer:
            gaps.append("missing_buyer_contact")
        if not has_email:
            gaps.append("missing_direct_email")
        if not has_phone:
            gaps.append("missing_direct_phone")
        if not has_menu:
            gaps.append("missing_menu_provider")
        if not proof_bundle:
            gaps.append("missing_proof_urls")
        if not has_social:
            gaps.append("missing_social_signal")

        target_roles = _target_roles(has_buyer, contact_title)
        suggested_paths = _research_paths(cfg)
        research_status = _status_for_gaps(gaps, has_buyer=has_buyer, has_email=has_email, has_phone=has_phone)
        recommended_action = _build_recommended_action(gaps, target_roles, website)
        enhancement_summary = _build_summary(
            score=score,
            tier=tier,
            has_buyer=has_buyer,
            has_email=has_email,
            has_phone=has_phone,
            menu_provider=menu_provider,
            gaps=gaps,
        )

        briefs.append(
            LeadResearchBrief(
                location_pk=location_pk,
                company_name=company_name,
                website=website,
                state=state,
                score=score,
                tier=tier,
                contact_name=contact_name,
                contact_title=contact_title,
                email=email,
                phone=phone,
                menu_provider=menu_provider,
                proof_urls=proof_bundle,
                social_urls=social_urls,
                research_status=research_status,
                gaps=gaps,
                target_roles=target_roles,
                suggested_paths=suggested_paths,
                recommended_action=recommended_action,
                enhancement_summary=enhancement_summary,
            )
        )
    return briefs


def _upsert_research_evidence(con, brief: LeadResearchBrief, *, run_id: str) -> None:
    now = utcnow_iso()
    source_url = f"https://{brief.website}" if brief.website else ""
    payload = {
        "location_pk": brief.location_pk,
        "score": brief.score,
        "tier": brief.tier,
        "research_status": brief.research_status,
        "gaps": brief.gaps,
        "target_roles": brief.target_roles,
        "suggested_paths": brief.suggested_paths,
        "recommended_action": brief.recommended_action,
        "summary": brief.enhancement_summary,
    }
    payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    con.execute(
        """
        INSERT OR REPLACE INTO enrichment_sources
        (enrichment_source_pk, source_type, source_name, source_url, fetched_at, success, payload_hash, status_code, error_message, created_at, deleted_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            make_pk("es", [brief.location_pk, "agent_research", run_id]),
            "agent_research",
            f"lead_brief:{run_id}",
            source_url,
            now,
            1,
            payload_hash,
            200,
            "",
            now,
            "",
        ),
    )
    evidence_values = {
        "agent_research_status": brief.research_status,
        "agent_research_gaps": "; ".join(brief.gaps),
        "agent_research_target_roles": "; ".join(brief.target_roles),
        "agent_research_suggested_paths": "; ".join(brief.suggested_paths),
        "agent_research_recommended_action": brief.recommended_action,
        "agent_research_summary": brief.enhancement_summary,
    }
    for field_name, field_value in evidence_values.items():
        con.execute(
            """
            INSERT OR REPLACE INTO evidence
            (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
            VALUES (?,?,?,?,?,?,?,?,'')
            """,
            (
                make_pk("ev", [brief.location_pk, field_name, run_id]),
                "location",
                brief.location_pk,
                field_name,
                field_value,
                source_url,
                f"agent research {run_id}",
                now,
            ),
        )


def run_lead_research(
    con,
    *,
    cfg: CrawlConfig,
    run_id: str,
    since: str | None = None,
    limit: int | None = None,
    min_score: int | None = None,
) -> dict[str, object]:
    if not cfg.agent_research_enabled:
        return {
            "enabled": False,
            "researched_locations": 0,
            "ready_locations": 0,
            "enhanced_locations": 0,
            "research_needed_locations": 0,
        }

    effective_limit = max(1, int(limit if limit is not None else cfg.agent_research_limit))
    effective_min_score = int(min_score if min_score is not None else cfg.agent_research_min_score)
    briefs = build_lead_research_briefs(
        con,
        cfg=cfg,
        since=since,
        min_score=effective_min_score,
        limit=effective_limit,
    )
    for brief in briefs:
        _upsert_research_evidence(con, brief, run_id=run_id)
    con.commit()

    return {
        "enabled": True,
        "researched_locations": len(briefs),
        "ready_locations": sum(1 for brief in briefs if brief.research_status == "ready"),
        "enhanced_locations": sum(1 for brief in briefs if brief.research_status in {"ready", "enhanced", "contactable"}),
        "research_needed_locations": sum(1 for brief in briefs if brief.research_status == "research_needed"),
        "min_score": effective_min_score,
        "limit": effective_limit,
    }
