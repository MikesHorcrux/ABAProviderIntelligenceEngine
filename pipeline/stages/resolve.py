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


def _provider_lookup_by_name_state(con: sqlite3.Connection, provider_name: str, state: str) -> str:
    if not provider_name or not state:
        return ""
    row = con.execute(
        """
        SELECT provider_id
        FROM providers
        WHERE lower(provider_name)=?
          AND lower(primary_license_state)=?
        LIMIT 1
        """,
        (provider_name.lower(), state.lower()),
    ).fetchone()
    if row:
        return str(row["provider_id"])
    row = con.execute(
        """
        SELECT provider_id
        FROM provider_practice_records
        WHERE lower(provider_name_snapshot)=?
          AND lower(license_state)=?
        LIMIT 1
        """,
        (provider_name.lower(), state.lower()),
    ).fetchone()
    return str(row["provider_id"]) if row else ""


def _merge_source_urls(current_json: str, source_url: str) -> str:
    urls = [str(item) for item in json.loads(current_json or "[]") if str(item)]
    if source_url and source_url not in urls:
        urls.append(source_url)
    return json.dumps(urls)


def _store_field_evidence(
    con: sqlite3.Connection,
    *,
    record_id: str,
    source_document_id: str,
    source_tier: str,
    default_source_url: str,
    evidence: list[dict[str, object]],
    captured_at: str,
) -> None:
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
                make_pk("evi", [record_id, field_name, value, item.get("source_url") or default_source_url]),
                record_id,
                field_name,
                value,
                str(item.get("quote") or ""),
                str(item.get("source_url") or default_source_url),
                source_document_id,
                source_tier,
                captured_at,
            ),
        )


def _queue_review(
    con: sqlite3.Connection,
    *,
    review_id_parts: list[str],
    record_id: str,
    review_type: str,
    provider_name: str,
    practice_name: str,
    reason: str,
    source_url: str,
    evidence_quote: str,
    created_at: str,
) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO review_queue
        (review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            make_pk("rev", review_id_parts),
            record_id,
            review_type,
            provider_name,
            practice_name,
            reason,
            source_url,
            evidence_quote,
            created_at,
        ),
    )


def _resolve_board_record(con: sqlite3.Connection, row: sqlite3.Row, evidence: list[dict[str, object]], now: str) -> bool:
    provider_id = (
        _provider_lookup_by_npi(con, row["npi"])
        or _provider_lookup_by_name_state(con, row["provider_name"], row["license_state"] or row["state"])
    )
    license_state = row["license_state"] or row["state"]
    license_type = row["license_type"] or "unknown"
    if not provider_id:
        if row["provider_name"] and row["license_status"] != "unknown":
            _queue_review(
                con,
                review_id_parts=[row["provider_name"], license_state, row["source_url"], "unmatched_license"],
                record_id="",
                review_type="unmatched_license",
                provider_name=row["provider_name"],
                practice_name=row["practice_name"],
                reason="Official license detail found but no provider-practice record matched for enrichment.",
                source_url=row["source_url"],
                evidence_quote=next((str(item.get("quote") or "") for item in evidence if str(item.get("quote") or "")), ""),
                created_at=now,
            )
        return False

    con.execute(
        """
        UPDATE providers
        SET provider_name = CASE WHEN ? <> '' THEN ? ELSE provider_name END,
            credentials = CASE WHEN ? <> '' THEN ? ELSE credentials END,
            npi = CASE WHEN ? <> '' THEN ? ELSE npi END,
            primary_license_state = CASE WHEN ? <> '' THEN ? ELSE primary_license_state END,
            primary_license_type = CASE WHEN ? <> 'unknown' THEN ? ELSE primary_license_type END,
            updated_at = ?
        WHERE provider_id = ?
        """,
        (
            row["provider_name"],
            row["provider_name"],
            row["credentials"],
            row["credentials"],
            row["npi"],
            row["npi"],
            license_state,
            license_state,
            license_type,
            license_type,
            now,
            provider_id,
        ),
    )
    license_id = make_pk("lic", [provider_id, license_state, license_type, row["npi"] or row["provider_name"]])
    con.execute(
        """
        INSERT INTO licenses
        (license_id, provider_id, license_state, license_type, license_number, license_status, source_url, retrieved_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(license_id) DO UPDATE SET
          provider_id = excluded.provider_id,
          license_state = CASE WHEN excluded.license_state <> '' THEN excluded.license_state ELSE licenses.license_state END,
          license_type = CASE WHEN excluded.license_type <> '' THEN excluded.license_type ELSE licenses.license_type END,
          license_number = CASE WHEN excluded.license_number <> '' THEN excluded.license_number ELSE licenses.license_number END,
          license_status = CASE WHEN excluded.license_status <> 'unknown' THEN excluded.license_status ELSE licenses.license_status END,
          source_url = CASE WHEN excluded.source_url <> '' THEN excluded.source_url ELSE licenses.source_url END,
          retrieved_at = excluded.retrieved_at,
          updated_at = excluded.updated_at
        """,
        (
            license_id,
            provider_id,
            license_state,
            license_type,
            row["npi"],
            row["license_status"],
            row["source_url"],
            now,
            now,
            now,
        ),
    )
    record_rows = con.execute(
        """
        SELECT record_id, source_urls_json
        FROM provider_practice_records
        WHERE provider_id=?
          AND (lower(license_state)=lower(?) OR ?='')
        """,
        (provider_id, license_state, license_state),
    ).fetchall()
    if not record_rows:
        _queue_review(
            con,
            review_id_parts=[provider_id, license_state, row["source_url"], "board_orphan"],
            record_id="",
            review_type="unmatched_license",
            provider_name=row["provider_name"],
            practice_name=row["practice_name"],
            reason="Official license detail matched a provider but no provider-practice affiliation exists for export.",
            source_url=row["source_url"],
            evidence_quote=next((str(item.get("quote") or "") for item in evidence if str(item.get("quote") or "")), ""),
            created_at=now,
        )
        return False

    for record in record_rows:
        merged_sources = _merge_source_urls(str(record["source_urls_json"] or "[]"), row["source_url"])
        con.execute(
            """
            UPDATE provider_practice_records
            SET npi = CASE WHEN ? <> '' THEN ? ELSE npi END,
                license_state = CASE WHEN ? <> '' THEN ? ELSE license_state END,
                license_type = CASE WHEN ? <> 'unknown' THEN ? ELSE license_type END,
                license_status = CASE WHEN ? <> 'unknown' THEN ? ELSE license_status END,
                source_urls_json = ?,
                last_verified_at = ?,
                updated_at = ?
            WHERE record_id = ?
            """,
            (
                row["npi"],
                row["npi"],
                license_state,
                license_state,
                license_type,
                license_type,
                row["license_status"],
                row["license_status"],
                merged_sources,
                now,
                now,
                record["record_id"],
            ),
        )
        _store_field_evidence(
            con,
            record_id=record["record_id"],
            source_document_id=row["source_document_id"],
            source_tier=row["source_tier"],
            default_source_url=row["source_url"],
            evidence=evidence,
            captured_at=now,
        )
    return True


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
        if row["source_type"] == "licensing_board" or row["extraction_profile"] == "board":
            if _resolve_board_record(con, row, evidence, now):
                resolved += 1
            continue

        if not row["provider_name"]:
            if row["diagnoses_asd"] == "yes" or row["diagnoses_adhd"] == "yes":
                _queue_review(
                    con,
                    review_id_parts=[row["practice_name"], normalize_domain(row["source_url"]) or row["source_url"], "missing_provider"],
                    record_id="",
                    review_type="missing_provider",
                    provider_name="",
                    practice_name=row["practice_name"],
                    reason="Practice offers evaluations but no named clinician was verified.",
                    source_url=row["source_url"],
                    evidence_quote=(evidence[0]["quote"] if evidence else ""),
                    created_at=now,
                )
                review_only += 1
            continue

        provider_id = (
            _provider_lookup_by_npi(con, row["npi"])
            or _provider_lookup_by_domain_state(con, row["provider_name"], row["source_url"], row["state"])
            or _provider_lookup_by_city_phone(con, row["provider_name"], row["city"], row["phone"])
            or _provider_lookup_by_name_state(con, row["provider_name"], row["license_state"] or row["state"])
            or make_pk("prov", [row["provider_name"], row["credentials"], row["state"], row["npi"] or row["source_url"]])
        )
        practice_id = make_pk("prac", [row["practice_name"], normalize_domain(row["source_url"]) or row["source_url"]])
        location_id = make_pk("loc", [practice_id, row["city"], row["state"], row["phone"] or normalize_domain(row["source_url"])])
        record_id = make_pk("rec", [provider_id, practice_id, location_id, row["state"]])

        con.execute(
            """
            INSERT INTO providers
            (provider_id, provider_name, credentials, npi, primary_license_state, primary_license_type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_id) DO UPDATE SET
              provider_name = CASE WHEN excluded.provider_name <> '' THEN excluded.provider_name ELSE providers.provider_name END,
              credentials = CASE WHEN excluded.credentials <> '' THEN excluded.credentials ELSE providers.credentials END,
              npi = CASE WHEN excluded.npi <> '' THEN excluded.npi ELSE providers.npi END,
              primary_license_state = CASE WHEN excluded.primary_license_state <> '' THEN excluded.primary_license_state ELSE providers.primary_license_state END,
              primary_license_type = CASE WHEN excluded.primary_license_type <> '' THEN excluded.primary_license_type ELSE providers.primary_license_type END,
              updated_at = excluded.updated_at
            """,
            (
                provider_id,
                row["provider_name"],
                row["credentials"],
                row["npi"],
                row["license_state"],
                row["license_type"],
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT INTO practices
            (practice_id, practice_name, website, intake_url, phone, fax, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(practice_id) DO UPDATE SET
              practice_name = CASE WHEN excluded.practice_name <> '' THEN excluded.practice_name ELSE practices.practice_name END,
              website = CASE WHEN excluded.website <> '' THEN excluded.website ELSE practices.website END,
              intake_url = CASE WHEN excluded.intake_url <> '' THEN excluded.intake_url ELSE practices.intake_url END,
              phone = CASE WHEN excluded.phone <> '' THEN excluded.phone ELSE practices.phone END,
              fax = CASE WHEN excluded.fax <> '' THEN excluded.fax ELSE practices.fax END,
              updated_at = excluded.updated_at
            """,
            (
                practice_id,
                row["practice_name"],
                row["source_url"],
                row["intake_url"],
                row["phone"],
                row["fax"],
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT INTO practice_locations
            (location_id, practice_id, address_1, city, state, zip, metro, phone, telehealth, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(location_id) DO UPDATE SET
              practice_id = excluded.practice_id,
              address_1 = CASE WHEN excluded.address_1 <> '' THEN excluded.address_1 ELSE practice_locations.address_1 END,
              city = CASE WHEN excluded.city <> '' THEN excluded.city ELSE practice_locations.city END,
              state = CASE WHEN excluded.state <> '' THEN excluded.state ELSE practice_locations.state END,
              zip = CASE WHEN excluded.zip <> '' THEN excluded.zip ELSE practice_locations.zip END,
              metro = CASE WHEN excluded.metro <> '' THEN excluded.metro ELSE practice_locations.metro END,
              phone = CASE WHEN excluded.phone <> '' THEN excluded.phone ELSE practice_locations.phone END,
              telehealth = CASE WHEN excluded.telehealth <> 'unknown' THEN excluded.telehealth ELSE practice_locations.telehealth END,
              updated_at = excluded.updated_at
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
                now,
                now,
            ),
        )
        license_id = make_pk("lic", [provider_id, row["license_state"], row["license_type"], row["npi"] or row["provider_name"]])
        con.execute(
            """
            INSERT INTO licenses
            (license_id, provider_id, license_state, license_type, license_number, license_status, source_url, retrieved_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(license_id) DO UPDATE SET
              provider_id = excluded.provider_id,
              license_state = CASE WHEN excluded.license_state <> '' THEN excluded.license_state ELSE licenses.license_state END,
              license_type = CASE WHEN excluded.license_type <> '' THEN excluded.license_type ELSE licenses.license_type END,
              license_number = CASE WHEN excluded.license_number <> '' THEN excluded.license_number ELSE licenses.license_number END,
              license_status = CASE WHEN excluded.license_status <> 'unknown' THEN excluded.license_status ELSE licenses.license_status END,
              source_url = CASE WHEN excluded.source_url <> '' THEN excluded.source_url ELSE licenses.source_url END,
              retrieved_at = excluded.retrieved_at,
              updated_at = excluded.updated_at
            """,
            (
                license_id,
                provider_id,
                row["license_state"],
                row["license_type"],
                row["npi"],
                row["license_status"],
                row["source_url"],
                now,
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT INTO provider_practice_records
            (record_id, provider_id, practice_id, location_id, provider_name_snapshot, practice_name_snapshot, npi,
             license_state, license_type, license_status, diagnoses_asd, diagnoses_adhd, prescriptive_authority,
             prescriptive_basis, age_groups_json, telehealth, insurance_notes, waitlist_notes, referral_requirements,
             source_urls_json, field_confidence_json, record_confidence, conflict_note, review_status, export_status,
             blocked_reason, last_verified_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unknown', '', ?, ?, ?, ?, ?, ?, '{}', 0.0, '', 'pending', 'pending', '', ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
              provider_id = excluded.provider_id,
              practice_id = excluded.practice_id,
              location_id = excluded.location_id,
              provider_name_snapshot = CASE WHEN excluded.provider_name_snapshot <> '' THEN excluded.provider_name_snapshot ELSE provider_practice_records.provider_name_snapshot END,
              practice_name_snapshot = CASE WHEN excluded.practice_name_snapshot <> '' THEN excluded.practice_name_snapshot ELSE provider_practice_records.practice_name_snapshot END,
              npi = CASE WHEN excluded.npi <> '' THEN excluded.npi ELSE provider_practice_records.npi END,
              license_state = CASE WHEN excluded.license_state <> '' THEN excluded.license_state ELSE provider_practice_records.license_state END,
              license_type = CASE WHEN excluded.license_type <> '' THEN excluded.license_type ELSE provider_practice_records.license_type END,
              license_status = CASE WHEN excluded.license_status <> 'unknown' THEN excluded.license_status ELSE provider_practice_records.license_status END,
              diagnoses_asd = CASE WHEN excluded.diagnoses_asd <> 'unclear' THEN excluded.diagnoses_asd ELSE provider_practice_records.diagnoses_asd END,
              diagnoses_adhd = CASE WHEN excluded.diagnoses_adhd <> 'unclear' THEN excluded.diagnoses_adhd ELSE provider_practice_records.diagnoses_adhd END,
              age_groups_json = CASE WHEN excluded.age_groups_json <> '[]' THEN excluded.age_groups_json ELSE provider_practice_records.age_groups_json END,
              telehealth = CASE WHEN excluded.telehealth <> 'unknown' THEN excluded.telehealth ELSE provider_practice_records.telehealth END,
              insurance_notes = CASE WHEN excluded.insurance_notes <> '' THEN excluded.insurance_notes ELSE provider_practice_records.insurance_notes END,
              waitlist_notes = CASE WHEN excluded.waitlist_notes <> '' THEN excluded.waitlist_notes ELSE provider_practice_records.waitlist_notes END,
              referral_requirements = CASE WHEN excluded.referral_requirements <> '' THEN excluded.referral_requirements ELSE provider_practice_records.referral_requirements END,
              last_verified_at = excluded.last_verified_at,
              updated_at = excluded.updated_at
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
                now,
                now,
            ),
        )
        current_sources = con.execute(
            "SELECT source_urls_json FROM provider_practice_records WHERE record_id=?",
            (record_id,),
        ).fetchone()
        con.execute(
            "UPDATE provider_practice_records SET source_urls_json=?, updated_at=? WHERE record_id=?",
            (_merge_source_urls(str((current_sources or {})["source_urls_json"] if current_sources else "[]"), row["source_url"]), now, record_id),
        )

        _store_field_evidence(
            con,
            record_id=record_id,
            source_document_id=row["source_document_id"],
            source_tier=row["source_tier"],
            default_source_url=row["source_url"],
            evidence=evidence,
            captured_at=now,
        )

        resolved += 1

    con.commit()
    return ResolveResult(resolved_count=resolved, review_only_count=review_only)
