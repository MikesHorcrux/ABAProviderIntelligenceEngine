from __future__ import annotations

import json
import sqlite3

from pipeline.utils import make_pk, utcnow_iso


TIER_WEIGHT = {"A": 0.92, "B": 0.78, "C": 0.55}


def _credential_key(credentials: str, license_type: str) -> str:
    lowered = f"{credentials} {license_type}".lower()
    if "md" in lowered or "do" in lowered or "physician" in lowered:
        return "MD/DO"
    if "psyd" in lowered or "phd" in lowered or "psychologist" in lowered:
        return "PsyD/PhD"
    if "apn" in lowered or "np" in lowered:
        return "APN/NP"
    if lowered.strip() == "pa" or "physician_assistant" in lowered:
        return "PA"
    if "lcsw" in lowered:
        return "LCSW"
    return ""


def _field_confidence(con: sqlite3.Connection, record_id: str, field_name: str, current_value: str) -> float:
    rows = con.execute(
        """
        SELECT source_tier, source_url, quote
        FROM field_evidence
        WHERE record_id=? AND field_name=?
        ORDER BY captured_at DESC
        """,
        (record_id, field_name),
    ).fetchall()
    if not rows:
        return 0.0
    best = max(TIER_WEIGHT.get(str(row["source_tier"] or "").upper(), 0.4) for row in rows if row["source_url"])
    if current_value in {"unknown", "unclear"}:
        best = min(best, 0.45)
    if current_value == "limited":
        best = min(best, 0.68)
    return round(best, 3)


def _has_direct_contact(*, phone: str, website: str, intake_url: str) -> bool:
    return bool((phone or "").strip() or (website or "").strip() or (intake_url or "").strip())


def _outreach_fit(
    *,
    diagnoses_asd: str,
    diagnoses_adhd: str,
    license_status: str,
    prescriptive_authority: str,
    telehealth: str,
    age_groups_json: str,
    phone: str,
    website: str,
    intake_url: str,
    field_confidence: dict[str, float],
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    age_groups = {str(item).lower() for item in json.loads(age_groups_json or "[]")}

    if diagnoses_asd == "yes":
        score += 0.30
        reasons.append("explicit_asd_diagnostic_signal")
    if diagnoses_adhd == "yes":
        score += 0.20
        reasons.append("explicit_adhd_diagnostic_signal")
    if license_status == "active":
        score += 0.20
        reasons.append("active_license")
    if _has_direct_contact(phone=phone, website=website, intake_url=intake_url):
        score += 0.10
        reasons.append("public_contact_channel")
    if telehealth == "yes":
        score += 0.05
        reasons.append("telehealth_available")
    if {"child", "adolescent"} & age_groups:
        score += 0.05
        reasons.append("pediatric_access")
    if prescriptive_authority in {"yes", "limited"}:
        score += 0.05
        reasons.append("medication_support_path")
    if field_confidence.get("diagnoses_asd", 0.0) >= 0.78 or field_confidence.get("diagnoses_adhd", 0.0) >= 0.78:
        score += 0.03
        reasons.append("high_confidence_diagnostic_evidence")
    if field_confidence.get("license_status", 0.0) >= 0.90:
        score += 0.02
        reasons.append("official_license_evidence")

    return round(min(score, 1.0), 3), reasons


def run_score(con: sqlite3.Connection) -> int:
    now = utcnow_iso()
    rows = con.execute(
        """
        SELECT pr.record_id, p.credentials, pr.license_type, pr.license_state,
               pr.diagnoses_asd, pr.diagnoses_adhd, pr.license_status, pr.telehealth, pr.age_groups_json,
               pt.website, pt.intake_url, COALESCE(pl.phone, pt.phone, '') AS phone
        FROM provider_practice_records pr
        INNER JOIN providers p ON p.provider_id = pr.provider_id
        INNER JOIN practices pt ON pt.practice_id = pr.practice_id
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        """
    ).fetchall()
    updated = 0
    for row in rows:
        credential = _credential_key(row["credentials"], row["license_type"])
        rule = con.execute(
            """
            SELECT authority, limitations, rationale, citation_title, citation_url
            FROM prescriber_rules
            WHERE state=? AND credential=? AND active=1
            ORDER BY rule_id ASC
            LIMIT 1
            """,
            (row["license_state"], credential),
        ).fetchone()
        authority = "unknown"
        basis = ""
        if rule:
            authority = str(rule["authority"] or "unknown")
            basis = str(rule["rationale"] or "")
            con.execute(
                """
                INSERT OR REPLACE INTO field_evidence
                (evidence_id, record_id, field_name, field_value, quote, source_url, source_document_id, source_tier, captured_at)
                VALUES (?, ?, 'prescriptive_authority', ?, ?, ?, '', 'A', ?)
                """,
                (
                    make_pk("evi", [row["record_id"], "prescriptive_authority", authority, rule["citation_url"]]),
                    row["record_id"],
                    authority,
                    str(rule["rationale"] or rule["citation_title"] or ""),
                    str(rule["citation_url"] or ""),
                    now,
                ),
            )

        field_confidence = {
            "diagnoses_asd": _field_confidence(con, row["record_id"], "diagnoses_asd", row["diagnoses_asd"]),
            "diagnoses_adhd": _field_confidence(con, row["record_id"], "diagnoses_adhd", row["diagnoses_adhd"]),
            "license_status": _field_confidence(con, row["record_id"], "license_status", row["license_status"]),
            "prescriptive_authority": _field_confidence(con, row["record_id"], "prescriptive_authority", authority),
        }
        confidence = round(sum(field_confidence.values()) / max(1, len(field_confidence)), 3)
        outreach_fit_score, outreach_reasons = _outreach_fit(
            diagnoses_asd=row["diagnoses_asd"],
            diagnoses_adhd=row["diagnoses_adhd"],
            license_status=row["license_status"],
            prescriptive_authority=authority,
            telehealth=row["telehealth"],
            age_groups_json=row["age_groups_json"],
            phone=row["phone"],
            website=row["website"],
            intake_url=row["intake_url"],
            field_confidence=field_confidence,
        )
        con.execute(
            """
            UPDATE provider_practice_records
            SET prescriptive_authority=?,
                prescriptive_basis=?,
                field_confidence_json=?,
                record_confidence=?,
                outreach_fit_score=?,
                outreach_ready=0,
                outreach_reasons_json=?,
                last_verified_at=?,
                updated_at=?
            WHERE record_id=?
            """,
            (
                authority,
                basis,
                json.dumps(field_confidence, sort_keys=True),
                confidence,
                outreach_fit_score,
                json.dumps(outreach_reasons, sort_keys=True),
                now,
                now,
                row["record_id"],
            ),
        )
        updated += 1
    con.commit()
    return updated
