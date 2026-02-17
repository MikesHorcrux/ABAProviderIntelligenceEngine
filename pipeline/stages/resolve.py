from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable

import sqlite3

from pipeline.utils import make_pk, normalize_domain, utcnow_iso
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.parse import ParsedPage


RESOLVE_DOMAIN_CONFIDENCE = 0.98
RESOLVE_PHONE_CONFIDENCE = 0.95
RESOLVE_NAME_MERGE_THRESHOLD = 0.92
RESOLVE_NAME_SUGGEST_THRESHOLD = 0.80


@dataclass(frozen=True)
class ResolvedLocation:
    company_pk: str
    location_pk: str
    domain: str
    segment: str
    merge_suggestions: int = 0


def _match_similarity(a: str, b: str) -> float:
    return SequenceMatcher(a=(a or "").lower(), b=(b or "").lower()).ratio()


def _match_by_domain(con: sqlite3.Connection, domain: str) -> tuple[str, str, float] | None:
    row = con.execute(
        """
        SELECT d.location_pk, l.org_pk
        FROM domains d
        INNER JOIN locations l ON l.location_pk = d.location_pk
        WHERE d.domain = ?
        LIMIT 1
        """,
        (domain,),
    ).fetchone()
    if not row:
        return None
    return row["org_pk"], row["location_pk"], RESOLVE_DOMAIN_CONFIDENCE


def _match_by_phone(con: sqlite3.Connection, phone: str) -> tuple[str, str, float] | None:
    if not phone:
        return None
    row = con.execute(
        """
        SELECT l.location_pk, l.org_pk
        FROM contact_points cp
        INNER JOIN locations l ON l.location_pk = cp.location_pk
        WHERE cp.type = 'phone' AND cp.value = ?
        LIMIT 1
        """,
        (phone,),
    ).fetchone()
    if not row:
        return None
    return row["org_pk"], row["location_pk"], RESOLVE_PHONE_CONFIDENCE


def _match_by_name_state(con: sqlite3.Connection, seed: DiscoverySeed) -> tuple[str, str, float] | None:
    if not seed.name:
        return None
    rows = con.execute(
        """
        SELECT location_pk, org_pk, canonical_name, state
        FROM locations
        WHERE lower(state) = ?
        LIMIT 80
        """,
        (seed.state.lower(),),
    ).fetchall()
    best: tuple[str, str] | None = None
    best_score = 0.0
    for row in rows:
        score = _match_similarity(seed.name, row["canonical_name"])
        if score > best_score:
            best_score = score
            best = row["org_pk"], row["location_pk"]
    if not best or best_score < RESOLVE_NAME_SUGGEST_THRESHOLD:
        return None
    return best[0], best[1], best_score


def _upsert_domain(con: sqlite3.Connection, location_pk: str, domain: str, source_url: str, now: str) -> None:
    if not domain:
        return
    domain_pk = make_pk("dom", [location_pk, domain])
    con.execute(
        """
        INSERT OR REPLACE INTO domains
        (domain_pk, location_pk, domain, is_primary, confidence, source_url, last_seen_at, created_at, updated_at, deleted_at)
        VALUES (?,?,?,?,?,?,?,?,?,'')
        """,
        (domain_pk, location_pk, domain, 1, 0.82, source_url, now, now, now),
    )


def _safe_location_state(existing: str | None, incoming: str | None, match_confidence: float) -> str:
    current = (existing or "").strip()
    incoming = (incoming or "").strip()
    if not current:
        return incoming
    if incoming and current.lower() == incoming.lower():
        return incoming
    if match_confidence >= 0.99:
        return incoming
    return current


def _write_merge_suggestion(
    con: sqlite3.Connection,
    canonical_location_pk: str,
    candidate_location_pk: str,
    reason: str,
    confidence: float = 0.74,
) -> bool:
    suggestion_pk = make_pk("mrg", [canonical_location_pk, candidate_location_pk, reason])
    now = utcnow_iso()
    con.execute(
        """
        INSERT OR REPLACE INTO entity_resolutions
        (resolution_pk, canonical_location_pk, candidate_location_pk, resolution_status, reason, confidence, created_at, updated_at, deleted_at)
        VALUES (?,?,?,?,?,?,?,?,'')
        """,
        (suggestion_pk, canonical_location_pk, candidate_location_pk, "suggest_merge", reason, confidence, now, now),
    )
    return True


def resolve_and_upsert_locations(
    con: sqlite3.Connection,
    seed: DiscoverySeed,
    parsed_pages: Iterable[ParsedPage],
) -> ResolvedLocation:
    now = utcnow_iso()
    domain = normalize_domain(seed.website)

    name_state_review: tuple[str, str, float] | None = None
    matched: tuple[str, str, float] | None = None

    if domain:
        matched = _match_by_domain(con, domain)

    if not matched:
        phones = [signal.value for page in parsed_pages for signal in page.phones if signal.value]
        for value in phones:
            matched = _match_by_phone(con, value)
            if matched:
                break

    if not matched:
        candidate = _match_by_name_state(con, seed)
        if candidate:
            candidate_org_pk, candidate_location_pk, candidate_conf = candidate
            if candidate_conf >= RESOLVE_NAME_MERGE_THRESHOLD:
                matched = candidate
            else:
                name_state_review = candidate

    if matched:
        org_pk, location_pk, match_confidence = matched
        existing = con.execute(
            "SELECT canonical_name, state, website_domain FROM locations WHERE location_pk = ?",
            (location_pk,),
        ).fetchone()
        if existing is None:
            matched = None
        else:
            location_state = _safe_location_state(existing["state"], seed.state, match_confidence)
            canonical_name = existing["canonical_name"]
            if not canonical_name and seed.name:
                canonical_name = seed.name
            con.execute(
                """
                UPDATE locations
                SET canonical_name = COALESCE(NULLIF(?, ''), canonical_name),
                    website_domain = COALESCE(NULLIF(?, ''), website_domain),
                    state = COALESCE(NULLIF(?, ''), state),
                    last_seen_at = ?,
                    last_crawled_at = ?,
                    updated_at = ?
                WHERE location_pk = ?
                """,
                (
                    canonical_name,
                    domain,
                    location_state,
                    now,
                    now,
                    now,
                    location_pk,
                ),
            )

            if domain:
                duplicate = con.execute(
                    """
                    SELECT location_pk FROM domains
                    WHERE domain = ? AND location_pk <> ?
                    LIMIT 1
                    """,
                    (domain, location_pk),
                ).fetchone()
                if duplicate:
                    _write_merge_suggestion(
                        con,
                        location_pk,
                        duplicate["location_pk"],
                        "domain_collision",
                        0.94,
                    )
                    merge_count = 1
                else:
                    merge_count = 0
            else:
                merge_count = 0

            _upsert_domain(con, location_pk, domain, seed.website, now)
            return ResolvedLocation(
                company_pk=org_pk,
                location_pk=location_pk,
                domain=domain,
                segment="unknown",
                merge_suggestions=merge_count,
            )

    org_pk = make_pk("org", [seed.name, seed.state, domain])
    con.execute(
        """
        INSERT OR REPLACE INTO organizations
        (org_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '')
        """,
        (
            org_pk,
            seed.name or "Unknown",
            seed.name or "Unknown",
            seed.state,
            now,
            now,
            now,
        ),
    )
    company_pk = make_pk("co", [org_pk, seed.state, domain or "unknown"])
    con.execute(
        """
        INSERT OR REPLACE INTO companies
        (company_pk, organization_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '')
        """,
        (company_pk, org_pk, seed.name or "Unknown", seed.name or "Unknown", seed.state, now, now, now),
    )
    location_pk = make_pk("loc", [seed.name, domain, seed.state])
    con.execute(
        """
        INSERT OR REPLACE INTO locations
        (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at, last_seen_at, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, '')
        """,
        (
            location_pk,
            org_pk,
            seed.name or "Unknown",
            "",
            "",
            seed.state,
            "",
            domain,
            "",
            0,
            now,
            now,
            now,
        ),
    )
    _upsert_domain(con, location_pk, domain, seed.website, now)

    merge_count = 0
    if name_state_review:
        _, review_location_pk, review_conf = name_state_review
        _write_merge_suggestion(
            con,
            location_pk,
            review_location_pk,
            f"low_confidence_name_state_match:{round(review_conf, 2)}",
            max(0.6, review_conf),
        )
        merge_count = 1

    return ResolvedLocation(company_pk=company_pk, location_pk=location_pk, domain=domain, segment="unknown", merge_suggestions=merge_count)
