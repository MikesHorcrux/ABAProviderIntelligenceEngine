from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from urllib.parse import urlparse

from pipeline.utils import make_pk, normalize_domain, utcnow_iso


CRITICAL_FIELDS = ("diagnoses_asd", "diagnoses_adhd", "license_status", "prescriptive_authority")
DIRECTORY_SOURCE_MARKERS = (
    "psychologytoday.com/us/therapists",
    "psychologytoday.com/us/psychiatrists",
    "psychologytoday.com/us/treatment-rehab",
)
INDIRECT_SOURCE_PATH_MARKERS = (
    "/continuing-education/",
    "/live-webinars",
    "/frequently-asked-questions",
    "/faq",
    "/news/",
    "/blog/",
    "/events/",
)
PRACTICE_SIGNAL_MARKERS = (
    "evaluation",
    "evaluations",
    "assessment",
    "assessments",
    "diagnostic",
    "diagnostics",
    "diagnosis",
    "autism",
    "adhd",
    "clinic",
    "center",
)
GENERIC_PROVIDER_PREFIXES = (
    "and ",
    "are ",
    "can ",
    "for ",
    "from ",
    "help ",
    "is ",
    "may ",
    "or ",
    "should ",
    "sleep ",
    "to ",
    "who ",
    "will ",
    "with ",
)
GENERIC_PROVIDER_TOKENS = {
    "about",
    "adhd",
    "adult",
    "assessment",
    "autism",
    "because",
    "beneficial",
    "can",
    "child",
    "children",
    "connected",
    "conduct",
    "counselor",
    "covered",
    "diagnosable",
    "doctor",
    "evaluation",
    "evaluations",
    "expert",
    "experienced",
    "first",
    "gather",
    "health",
    "help",
    "initial",
    "insurance",
    "licensed",
    "medical",
    "mental",
    "network",
    "order",
    "other",
    "physician",
    "primary",
    "psychiatrist",
    "psychologist",
    "refer",
    "rule",
    "sleep",
    "specialists",
    "substance",
    "testing",
    "tests",
    "therapist",
    "whose",
}


@dataclass(frozen=True)
class TriageDecision:
    review_type: str
    reason: str
    source_url: str
    evidence_quote: str
    review_id_parts: list[str]


@dataclass(frozen=True)
class SourceSummary:
    latest_source_url: str
    latest_quote: str
    domains: tuple[str, ...]
    source_count: int
    evidence_count: int
    critical_fields_present: tuple[str, ...]


@dataclass(frozen=True)
class TriageStats:
    directory_sludge: int = 0
    practice_only_signal: int = 0
    indirect_provider_signal: int = 0
    seed_retire_candidates: int = 0


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


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _titleish_token_count(tokens: list[str]) -> int:
    total = 0
    for token in tokens:
        cleaned = token.strip(".,;:()[]{}\"'")
        if not cleaned:
            continue
        if cleaned[0].isupper():
            total += 1
    return total


def _looks_like_person_name(value: str) -> bool:
    candidate = _normalize_spaces(value)
    if not candidate or len(candidate) < 6 or len(candidate) > 80:
        return False
    lowered = candidate.lower()
    if "|" in candidate or any(lowered.startswith(prefix) for prefix in GENERIC_PROVIDER_PREFIXES):
        return False
    if any(char.isdigit() for char in candidate):
        return False
    tokens = [token for token in re.split(r"\s+", candidate) if token]
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    alpha_tokens = [re.sub(r"[^A-Za-z'\-]", "", token) for token in tokens]
    alpha_tokens = [token for token in alpha_tokens if token]
    if len(alpha_tokens) < 2:
        return False
    lowered_tokens = {token.lower() for token in alpha_tokens}
    if lowered_tokens & GENERIC_PROVIDER_TOKENS:
        return False
    if _titleish_token_count(alpha_tokens) < 2:
        return False
    return True


def _is_directory_source(*, practice_name: str, source_url: str) -> bool:
    lowered_practice = _normalize_spaces(practice_name).lower()
    lowered_url = str(source_url or "").lower()
    if any(marker in lowered_url for marker in DIRECTORY_SOURCE_MARKERS):
        return True
    return lowered_practice.startswith("find a therapist") or lowered_practice.startswith("find child therapists")


def _is_indirect_source(source_url: str) -> bool:
    lowered_url = str(source_url or "").lower()
    return any(marker in lowered_url for marker in INDIRECT_SOURCE_PATH_MARKERS)


def _has_practice_signal(*, practice_name: str, source_url: str, evidence_quote: str) -> bool:
    haystack = " ".join([practice_name or "", source_url or "", evidence_quote or ""]).lower()
    return any(marker in haystack for marker in PRACTICE_SIGNAL_MARKERS)


def _source_summary(con: sqlite3.Connection, record_id: str, source_urls_json: str) -> SourceSummary:
    evidence_rows = con.execute(
        """
        SELECT field_name, source_url, quote, captured_at
        FROM field_evidence
        WHERE record_id=?
        ORDER BY captured_at DESC
        """,
        (record_id,),
    ).fetchall()
    urls = [str(item) for item in json.loads(source_urls_json or "[]") if str(item)]
    for row in evidence_rows:
        if row["source_url"] and row["source_url"] not in urls:
            urls.append(str(row["source_url"]))
    latest_source_url = str(evidence_rows[0]["source_url"] if evidence_rows else (urls[0] if urls else ""))
    latest_quote = str(evidence_rows[0]["quote"] if evidence_rows else "")
    critical_present = tuple(
        sorted(
            {
                str(row["field_name"])
                for row in evidence_rows
                if str(row["field_name"] or "") in CRITICAL_FIELDS and str(row["source_url"] or "") and str(row["quote"] or "")
            }
        )
    )
    domains = tuple(sorted({normalize_domain(url) for url in urls if normalize_domain(url)}))
    return SourceSummary(
        latest_source_url=latest_source_url,
        latest_quote=latest_quote,
        domains=domains,
        source_count=len(urls),
        evidence_count=len(evidence_rows),
        critical_fields_present=critical_present,
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


def _triage_decision(
    con: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    missing: list[str],
) -> TriageDecision | None:
    source = _source_summary(con, row["record_id"], row["source_urls_json"])
    provider_name = _normalize_spaces(row["provider_name_snapshot"])
    practice_name = _normalize_spaces(row["practice_name_snapshot"])
    provider_name_valid = _looks_like_person_name(provider_name)
    has_contact = _has_public_contact(phone=row["phone"], website=row["website"], intake_url=row["intake_url"])
    directory_source = _is_directory_source(practice_name=practice_name, source_url=source.latest_source_url)
    indirect_source = _is_indirect_source(source.latest_source_url)
    practice_signal = _has_practice_signal(
        practice_name=practice_name,
        source_url=source.latest_source_url,
        evidence_quote=source.latest_quote,
    )
    critical_count = len(source.critical_fields_present)
    confidence = float(row["record_confidence"] or 0.0)

    if directory_source and not provider_name_valid:
        reason = (
            "Directory sludge: generic snippet was extracted as a provider from a broad listing page; "
            "keep for parser review, not operator review."
        )
        return TriageDecision(
            review_type="directory_sludge",
            reason=reason,
            source_url=source.latest_source_url,
            evidence_quote=source.latest_quote,
            review_id_parts=[row["record_id"], source.latest_source_url, "directory_sludge"],
        )

    if not provider_name_valid and practice_signal:
        reason = (
            "Practice-only signal: service/evaluation evidence exists but no credible named clinician was verified; "
            "review as practice coverage, not provider outreach."
        )
        return TriageDecision(
            review_type="practice_only_signal",
            reason=reason,
            source_url=source.latest_source_url,
            evidence_quote=source.latest_quote,
            review_id_parts=[row["record_id"], source.latest_source_url, "practice_only_signal"],
        )

    if provider_name_valid and indirect_source and critical_count <= 1 and confidence < 0.7:
        reason = (
            "Indirect provider signal: named clinician was found on a webinar/FAQ/resource page without strong direct service evidence; "
            "needs corroboration before outreach."
        )
        return TriageDecision(
            review_type="indirect_provider_signal",
            reason=reason,
            source_url=source.latest_source_url,
            evidence_quote=source.latest_quote,
            review_id_parts=[row["record_id"], source.latest_source_url, "indirect_provider_signal"],
        )

    if not has_contact and critical_count == 0 and confidence < 0.55 and missing:
        reason = (
            "Weak provider signal: evidence is too thin to justify operator review yet; likely parser noise or unsupported mention."
        )
        return TriageDecision(
            review_type="weak_provider_signal",
            reason=reason,
            source_url=source.latest_source_url,
            evidence_quote=source.latest_quote,
            review_id_parts=[row["record_id"], source.latest_source_url, "weak_provider_signal"],
        )

    return None


def run_qa(con: sqlite3.Connection) -> dict[str, int]:
    now = utcnow_iso()
    rows = con.execute(
        """
        SELECT pr.record_id, pr.provider_name_snapshot, pr.practice_name_snapshot, pr.prescriptive_authority,
               pr.record_confidence, pr.diagnoses_asd, pr.diagnoses_adhd, pr.license_status, pr.outreach_fit_score,
               pr.source_urls_json,
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
    triage_stats = {
        "directory_sludge": 0,
        "practice_only_signal": 0,
        "indirect_provider_signal": 0,
        "weak_provider_signal": 0,
        "seed_retire_candidates": 0,
    }
    source_review_rollup: dict[str, dict[str, int]] = {}

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

        triage_row = dict(row)
        triage_row["record_confidence"] = record_confidence
        triage_decision = _triage_decision(con, triage_row, missing=missing)
        if triage_decision:
            _queue_review(
                con,
                review_id_parts=triage_decision.review_id_parts,
                record_id=row["record_id"],
                review_type=triage_decision.review_type,
                provider_name=row["provider_name_snapshot"],
                practice_name=row["practice_name_snapshot"],
                reason=triage_decision.reason,
                source_url=triage_decision.source_url,
                evidence_quote=triage_decision.evidence_quote,
                created_at=now,
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
                (triage_decision.reason, "; ".join(conflict_notes), record_confidence, now, row["record_id"]),
            )
            triage_stats[triage_decision.review_type] = triage_stats.get(triage_decision.review_type, 0) + 1
            rollup = source_review_rollup.setdefault(
                triage_decision.source_url,
                {"queued": 0, "directory_sludge": 0, "practice_only_signal": 0, "indirect_provider_signal": 0, "weak_provider_signal": 0},
            )
            rollup["queued"] += 1
            rollup[triage_decision.review_type] = rollup.get(triage_decision.review_type, 0) + 1
            queued += 1
            continue

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
            _queue_review(
                con,
                review_id_parts=[row["record_id"], reason_text],
                record_id=row["record_id"],
                review_type="record_review",
                provider_name=row["provider_name_snapshot"],
                practice_name=row["practice_name_snapshot"],
                reason=reason_text,
                source_url=str((evidence_row or {})["source_url"] if evidence_row else ""),
                evidence_quote=str((evidence_row or {})["quote"] if evidence_row else ""),
                created_at=now,
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

    for source_url, counts in source_review_rollup.items():
        if counts.get("queued", 0) < 5:
            continue
        if counts.get("directory_sludge", 0) != counts.get("queued", 0):
            continue
        domain = normalize_domain(source_url)
        reason = (
            "Seed retire candidate: this source produced repeated directory-sludge records without credible clinician-level evidence; "
            "retire or parser-block this source before the next clean run."
        )
        _queue_review(
            con,
            review_id_parts=[domain or source_url, source_url, "seed_retire_candidate"],
            record_id="",
            review_type="seed_retire_candidate",
            provider_name="",
            practice_name=domain or source_url,
            reason=reason,
            source_url=source_url,
            evidence_quote="",
            created_at=now,
        )
        triage_stats["seed_retire_candidates"] += 1

    con.commit()
    return {
        "approved_records": approved,
        "queued_records": queued,
        "contradictions": contradictions,
        "outreach_ready_records": outreach_ready,
        "triage_directory_sludge": triage_stats["directory_sludge"],
        "triage_practice_only_signal": triage_stats["practice_only_signal"],
        "triage_indirect_provider_signal": triage_stats["indirect_provider_signal"],
        "triage_weak_provider_signal": triage_stats["weak_provider_signal"],
        "seed_retire_candidates": triage_stats["seed_retire_candidates"],
    }
