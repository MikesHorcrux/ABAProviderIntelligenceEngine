from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from pipeline.utils import make_pk, normalize_domain, normalize_text, utcnow_iso


@dataclass(frozen=True)
class ResolveResult:
    resolved_count: int
    review_only_count: int


def _provider_lookup_by_npi(con: sqlite3.Connection, npi: str) -> str:
    if not npi:
        return ""
    row = con.execute("SELECT provider_id FROM providers WHERE npi=? LIMIT 1", (npi,)).fetchone()
    return str(row["provider_id"]) if row else ""


def _provider_lookup_by_domain_state(con: sqlite3.Connection, provider_name: str, practice_website: str, state: str) -> str:
    if not provider_name or not practice_website or not state:
        return ""
    domain = normalize_domain(practice_website)
    row = con.execute(
        """
        SELECT pr.provider_id
        FROM provider_practice_records pr
        INNER JOIN practices p ON p.practice_id = pr.practice_id
        WHERE lower(pr.provider_name_snapshot)=?
          AND lower(pr.license_state)=?
          AND lower(p.website) LIKE ?
        LIMIT 1
        """,
        (provider_name.lower(), state.lower(), f"%{domain}%"),
    ).fetchone()
    return str(row["provider_id"]) if row else ""


def _provider_lookup_by_city_phone(con: sqlite3.Connection, provider_name: str, city: str, phone: str) -> str:
    if not provider_name or not city or not phone:
        return ""
    row = con.execute(
        """
        SELECT pr.provider_id
        FROM provider_practice_records pr
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        WHERE lower(pr.provider_name_snapshot)=?
          AND lower(pl.city)=?
          AND pl.phone=?
        LIMIT 1
        """,
        (provider_name.lower(), city.lower(), phone),
    ).fetchone()
    return str(row["provider_id"]) if row else ""


def resolve_extracted_records(con: sqlite3.Connection) -> ResolveResult:
    now = utcnow_iso()
    rows = con.execute(
        """
        SELECT *
        FROM extracted_records
        ORDER BY created_at ASC, extracted_id ASC
        """
    ).fetchall()
    resolved = 0
    review_only = 0

    for row in rows:
        evidence = json.loads(row["evidence_json"] or "[]")
        if not row["provider_name"]:
            if row["diagnoses_asd"] == "yes" or row["diagnoses_adhd"] == "yes":
                con.execute(
                    """
                    INSERT OR REPLACE INTO review_queue
                    (review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at)
                    VALUES (?, '', 'missing_provider', '', ?, ?, ?, ?, 'pending', ?)
                    """,
                    (
                        make_pk("rev", [row["practice_name"], row["source_url"], "missing_provider"]),
                        row["practice_name"],
                        "Practice offers evaluations but no named clinician was verified.",
                        row["source_url"],
                        (evidence[0]["quote"] if evidence else ""),
                        now,
                    ),
                )
                review_only += 1
            continue

        provider_id = (
            _provider_lookup_by_npi(con, row["npi"])
            or _provider_lookup_by_domain_state(con, row["provider_name"], row["source_url"], row["state"])
            or _provider_lookup_by_city_phone(con, row["provider_name"], row["city"], row["phone"])
            or make_pk("prov", [row["provider_name"], row["credentials"], row["state"], row["npi"] or row["source_url"]])
        )
        practice_id = make_pk("prac", [row["practice_name"], normalize_domain(row["source_url"]) or row["source_url"]])
        location_id = make_pk("loc", [practice_id, row["city"], row["state"], row["phone"] or normalize_domain(row["source_url"])])
        record_id = make_pk("rec", [provider_id, practice_id, location_id, row["state"]])

        con.execute(
            """
            INSERT OR REPLACE INTO providers
            (provider_id, provider_name, credentials, npi, primary_license_state, primary_license_type, created_at, updated_at)
            VALUES (
              ?,
              COALESCE(NULLIF((SELECT provider_name FROM providers WHERE provider_id=?), ''), ?),
              ?,
              ?,
              ?,
              ?,
              COALESCE((SELECT created_at FROM providers WHERE provider_id=?), ?),
              ?
            )
            """,
            (
                provider_id,
                provider_id,
                row["provider_name"],
                row["credentials"],
                row["npi"],
                row["license_state"],
                row["license_type"],
                provider_id,
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO practices
            (practice_id, practice_name, website, intake_url, phone, fax, created_at, updated_at)
            VALUES (
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              COALESCE((SELECT created_at FROM practices WHERE practice_id=?), ?),
              ?
            )
            """,
            (
                practice_id,
                row["practice_name"],
                row["source_url"],
                row["intake_url"],
                row["phone"],
                row["fax"],
                practice_id,
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO practice_locations
            (location_id, practice_id, address_1, city, state, zip, metro, phone, telehealth, created_at, updated_at)
            VALUES (
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              ?,
              COALESCE((SELECT created_at FROM practice_locations WHERE location_id=?), ?),
              ?
            )
            """,
            (
                location_id,
                practice_id,
                row["address_1"],
                row["city"],
                row["state"],
                row["zip"],
                row["metro"],
                row["phone"],
                row["telehealth"],
                location_id,
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO licenses
            (license_id, provider_id, license_state, license_type, license_number, license_status, source_url, retrieved_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM licenses WHERE license_id=?), ?), ?)
            """,
            (
                make_pk("lic", [provider_id, row["license_state"], row["license_type"], row["npi"] or row["provider_name"]]),
                provider_id,
                row["license_state"],
                row["license_type"],
                row["npi"],
                row["license_status"],
                row["source_url"],
                now,
                make_pk("lic", [provider_id, row["license_state"], row["license_type"], row["npi"] or row["provider_name"]]),
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO provider_practice_records
            (record_id, provider_id, practice_id, location_id, provider_name_snapshot, practice_name_snapshot, npi,
             license_state, license_type, license_status, diagnoses_asd, diagnoses_adhd, prescriptive_authority,
             prescriptive_basis, age_groups_json, telehealth, insurance_notes, waitlist_notes, referral_requirements,
             source_urls_json, field_confidence_json, record_confidence, conflict_note, review_status, export_status,
             blocked_reason, last_verified_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unknown', '', ?, ?, ?, ?, ?, ?, '{}', 0.0, '', 'pending', 'pending', '', ?, COALESCE((SELECT created_at FROM provider_practice_records WHERE record_id=?), ?), ?)
            """,
            (
                record_id,
                provider_id,
                practice_id,
                location_id,
                row["provider_name"],
                row["practice_name"],
                row["npi"],
                row["license_state"],
                row["license_type"],
                row["license_status"],
                row["diagnoses_asd"],
                row["diagnoses_adhd"],
                row["age_groups_json"],
                row["telehealth"],
                row["insurance_notes"],
                row["waitlist_notes"],
                row["referral_requirements"],
                json.dumps([row["source_url"]]),
                now,
                record_id,
                now,
                now,
            ),
        )

        for item in evidence:
            field_name = normalize_text(item.get("field") or "")
            value = normalize_text(item.get("value") or "")
            if not field_name:
                continue
            con.execute(
                """
                INSERT OR REPLACE INTO field_evidence
                (evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    make_pk("evi", [record_id, field_name, value, item.get("source_url") or row["source_url"]]),
                    record_id,
                    field_name,
                    value,
                    str(item.get("quote") or ""),
                    str(item.get("source_url") or row["source_url"]),
                    row["source_document_id"],
                    row["source_tier"],
                    now,
                ),
            )

        resolved += 1

    con.commit()
    return ResolveResult(resolved_count=resolved, review_only_count=review_only)
