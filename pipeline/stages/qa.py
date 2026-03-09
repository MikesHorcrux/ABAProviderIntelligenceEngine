from __future__ import annotations

import json
import sqlite3

from pipeline.utils import make_pk, utcnow_iso


CRITICAL_FIELDS = ("diagnoses_asd", "diagnoses_adhd", "license_status", "prescriptive_authority")


def _missing_critical_evidence(con: sqlite3.Connection, record_id: str) -> list[str]:
    missing: list[str] = []
    for field in CRITICAL_FIELDS:
        row = con.execute(
            """
            SELECT 1
            FROM field_evidence
            WHERE record_id=? AND field_name=? AND source_url<>'' AND quote<>''
            LIMIT 1
            """,
            (record_id, field),
        ).fetchone()
        if not row:
            missing.append(field)
    return missing


def _has_public_contact(*, phone: str, website: str, intake_url: str) -> bool:
    return bool((phone or "").strip() or (website or "").strip() or (intake_url or "").strip())


def run_qa(con: sqlite3.Connection) -> dict[str, int]:
    now = utcnow_iso()
    rows = con.execute(
        """
        SELECT pr.record_id, pr.provider_name_snapshot, pr.practice_name_snapshot, pr.prescriptive_authority,
               pr.record_confidence, pr.diagnoses_asd, pr.diagnoses_adhd, pr.license_status, pr.outreach_fit_score,
               pt.website, pt.intake_url, COALESCE(pl.phone, pt.phone, '') AS phone
        FROM provider_practice_records pr
        INNER JOIN practices pt ON pt.practice_id = pr.practice_id
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        """
    ).fetchall()
    approved = 0
    queued = 0
    contradictions = 0
    outreach_ready = 0
    for row in rows:
        conflict_notes: list[str] = []
        for field in CRITICAL_FIELDS:
            evidence_rows = con.execute(
                """
                SELECT field_value, source_url
                FROM field_evidence
                WHERE record_id=? AND field_name=?
                ORDER BY CASE source_tier WHEN 'A' THEN 0 WHEN 'B' THEN 1 ELSE 2 END,
                         captured_at DESC
                """,
                (row["record_id"], field),
            ).fetchall()
            distinct = [(str(item["field_value"] or ""), str(item["source_url"] or "")) for item in evidence_rows if str(item["field_value"] or "")]
            if len({value for value, _ in distinct}) > 1:
                preferred_value, preferred_source = distinct[0]
                for conflicting_value, conflicting_source in distinct[1:]:
                    con.execute(
                        """
                        INSERT OR REPLACE INTO contradictions
                        (contradiction_id, record_id, field_name, preferred_value, conflicting_value, preferred_source_url,
                         conflicting_source_url, note, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            make_pk("cnf", [row["record_id"], field, preferred_value, conflicting_value, conflicting_source]),
                            row["record_id"],
                            field,
                            preferred_value,
                            conflicting_value,
                            preferred_source,
                            conflicting_source,
                            f"Preferred {preferred_value} over {conflicting_value}",
                            now,
                        ),
                    )
                conflict_notes.append(f"{field} conflict")
                contradictions += 1

        missing = _missing_critical_evidence(con, row["record_id"])
        record_confidence = float(row["record_confidence"] or 0.0)
        if conflict_notes:
            record_confidence = max(0.0, round(record_confidence - 0.15, 3))

        reasons: list[str] = []
        if record_confidence < 0.60:
            reasons.append("low_confidence")
        if row["prescriptive_authority"] in {"limited", "unknown"}:
            reasons.append("prescriptive_review")
        if missing:
            reasons.append(f"missing_critical:{','.join(missing)}")
        if conflict_notes:
            reasons.extend(conflict_notes)

        if reasons:
            reason_text = "; ".join(reasons)
            evidence_row = con.execute(
                "SELECT source_url, quote FROM field_evidence WHERE record_id=? ORDER BY captured_at DESC LIMIT 1",
                (row["record_id"],),
            ).fetchone()
            con.execute(
                """
                INSERT OR REPLACE INTO review_queue
                (review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at)
                VALUES (?, ?, 'record_review', ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    make_pk("rev", [row["record_id"], reason_text]),
                    row["record_id"],
                    row["provider_name_snapshot"],
                    row["practice_name_snapshot"],
                    reason_text,
                    str((evidence_row or {})["source_url"] if evidence_row else ""),
                    str((evidence_row or {})["quote"] if evidence_row else ""),
                    now,
                ),
            )
            con.execute(
                """
                UPDATE provider_practice_records
                SET review_status='queued',
                    export_status='blocked',
                    outreach_ready=0,
                    blocked_reason=?,
                    conflict_note=?,
                    record_confidence=?,
                    updated_at=?
                WHERE record_id=?
                """,
                (reason_text, "; ".join(conflict_notes), record_confidence, now, row["record_id"]),
            )
            queued += 1
            continue

        outreach_ready_flag = int(
            record_confidence >= 0.70
            and float(row["outreach_fit_score"] or 0.0) >= 0.70
            and row["license_status"] == "active"
            and (row["diagnoses_asd"] == "yes" or row["diagnoses_adhd"] == "yes")
            and _has_public_contact(phone=row["phone"], website=row["website"], intake_url=row["intake_url"])
        )
        con.execute(
            """
            UPDATE provider_practice_records
            SET review_status='ready',
                export_status='approved',
                outreach_ready=?,
                blocked_reason='',
                conflict_note=?,
                record_confidence=?,
                updated_at=?
            WHERE record_id=?
            """,
            (outreach_ready_flag, "; ".join(conflict_notes), record_confidence, now, row["record_id"]),
        )
        approved += 1
        outreach_ready += outreach_ready_flag

    con.commit()
    return {
        "approved_records": approved,
        "queued_records": queued,
        "contradictions": contradictions,
        "outreach_ready_records": outreach_ready,
    }
