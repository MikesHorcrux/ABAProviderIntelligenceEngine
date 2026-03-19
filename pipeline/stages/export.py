from __future__ import annotations

from collections import defaultdict
import csv
import html
import json
import re
import shutil
import sqlite3
import textwrap
from pathlib import Path
from urllib.parse import urlparse


def _safe_slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")
    return cleaned or "record"


def _bundle(con: sqlite3.Connection, record_id: str) -> dict[str, object]:
    record = con.execute(
        """
        SELECT pr.*, pl.city, pl.state AS location_state, pl.metro, pl.phone AS location_phone,
               p.provider_name, p.credentials, pt.website, pt.intake_url, pr.practice_name_snapshot AS practice_name
        FROM provider_practice_records pr
        INNER JOIN providers p ON p.provider_id = pr.provider_id
        INNER JOIN practices pt ON pt.practice_id = pr.practice_id
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        WHERE pr.record_id=?
        """,
        (record_id,),
    ).fetchone()
    evidence = [dict(row) for row in con.execute("SELECT * FROM field_evidence WHERE record_id=? ORDER BY field_name, captured_at", (record_id,))]
    contradictions = [dict(row) for row in con.execute("SELECT * FROM contradictions WHERE record_id=? ORDER BY field_name", (record_id,))]
    return {
        "record": dict(record) if record else {},
        "evidence": evidence,
        "contradictions": contradictions,
    }


def _diagnostic_focus(record: dict[str, object]) -> str:
    asd = str(record.get("diagnoses_asd") or "unclear")
    adhd = str(record.get("diagnoses_adhd") or "unclear")
    if asd == "yes" and adhd == "yes":
        return "ASD and ADHD diagnostic services"
    if asd == "yes":
        return "autism diagnostic services"
    if adhd == "yes":
        return "ADHD diagnostic services"
    return "developmental and behavioral evaluation services"


def _target_buyer(record: dict[str, object]) -> str:
    credentials = str(record.get("credentials") or "").lower()
    if "md" in credentials or "do" in credentials:
        return "medical director or practice owner"
    if "psyd" in credentials or "phd" in credentials:
        return "clinical director or practice owner"
    if "apn" in credentials or "np" in credentials or "pa" in credentials:
        return "clinical lead or practice administrator"
    return "practice administrator or intake lead"


def _outreach_angle(record: dict[str, object]) -> str:
    focus = _diagnostic_focus(record)
    telehealth = str(record.get("telehealth") or "unknown")
    referral = str(record.get("referral_requirements") or "").strip() or "unknown"
    if telehealth == "yes":
        return f"Lead with referral capture and intake efficiency for {focus}, including telehealth-enabled scheduling capacity."
    if referral != "unknown":
        return f"Lead with faster referral routing and intake conversion for {focus}, since the practice already publishes referral requirements."
    return f"Lead with diagnostic demand capture, intake conversion, and referral visibility for {focus}."


def _outreach_opener(record: dict[str, object]) -> str:
    practice = str(record.get("practice_name") or record.get("practice_name_snapshot") or "").strip()
    city = str(record.get("city") or "").strip()
    focus = _diagnostic_focus(record)
    locale = f" in {city}" if city else ""
    return f"{practice}{locale} publicly advertises {focus}; open with referral demand, intake throughput, and evaluation booking friction."


def _evidence_summary(bundle: dict[str, object]) -> str:
    evidence = list(bundle.get("evidence") or [])
    preferred_fields = ("diagnoses_asd", "diagnoses_adhd", "license_status", "prescriptive_authority")
    snippets: list[str] = []
    for field in preferred_fields:
        item = next((entry for entry in evidence if str(entry.get("field_name") or entry.get("field") or "") == field), None)
        if not item:
            continue
        quote = _meaningful_quote(item.get("quote") or "")
        if quote:
            snippets.append(quote)
    return " | ".join(snippets[:3])


def _sales_bundle_row(bundle: dict[str, object]) -> dict[str, object]:
    record = dict(bundle.get("record") or {})
    evidence = list(bundle.get("evidence") or [])
    source_urls = sorted({str(item.get("source_url") or "") for item in evidence if str(item.get("source_url") or "")})
    return {
        "record_id": record.get("record_id", ""),
        "provider_name": record.get("provider_name", ""),
        "credentials": record.get("credentials", ""),
        "practice_name": record.get("practice_name", ""),
        "city": record.get("city", ""),
        "state": record.get("location_state", ""),
        "metro": record.get("metro", ""),
        "phone": record.get("location_phone", ""),
        "website": record.get("website", ""),
        "intake_url": record.get("intake_url", ""),
        "diagnoses_asd": record.get("diagnoses_asd", "unclear"),
        "diagnoses_adhd": record.get("diagnoses_adhd", "unclear"),
        "license_status": record.get("license_status", "unknown"),
        "prescriptive_authority": record.get("prescriptive_authority", "unknown"),
        "record_confidence": record.get("record_confidence", 0.0),
        "outreach_fit_score": record.get("outreach_fit_score", 0.0),
        "target_buyer": _target_buyer(record),
        "outreach_angle": _outreach_angle(record),
        "opener": _outreach_opener(record),
        "evidence_summary": _evidence_summary(bundle),
        "source_urls": source_urls,
    }


def _sales_markdown(bundle: dict[str, object]) -> str:
    row = _sales_bundle_row(bundle)
    lines = [
        f"# Sales Brief - {row['provider_name']} / {row['practice_name']}",
        "",
        "## Target",
        f"- Buyer: {row['target_buyer']}",
        f"- Phone: {row['phone'] or 'unknown'}",
        f"- Website: {row['website'] or 'unknown'}",
        f"- Intake URL: {row['intake_url'] or 'unknown'}",
        "",
        "## Why This Record Matters",
        f"- Diagnostic focus: {_diagnostic_focus(dict(bundle.get('record') or {}))}",
        f"- License status: {row['license_status']}",
        f"- Prescribing capability: {row['prescriptive_authority']}",
        f"- Record confidence: {row['record_confidence']}",
        f"- Outreach fit score: {row['outreach_fit_score']}",
        "",
        "## Recommended Angle",
        f"- {row['outreach_angle']}",
        "",
        "## Suggested Opener",
        f"- {row['opener']}",
        "",
        "## Evidence Summary",
        f"- {row['evidence_summary'] or 'See cited evidence bundle.'}",
        "",
        "## Evidence Links",
    ]
    for url in row["source_urls"]:
        lines.append(f"- {url}")
    if not row["source_urls"]:
        lines.append("- No evidence links captured.")
    return "\n".join(lines).strip() + "\n"


def _markdown_profile(bundle: dict[str, object]) -> str:
    record = dict(bundle.get("record") or {})
    evidence = list(bundle.get("evidence") or [])
    contradictions = list(bundle.get("contradictions") or [])
    source_urls = sorted({str(item.get("source_url") or "") for item in evidence if str(item.get("source_url") or "")})
    caveats = record.get("blocked_reason") or record.get("conflict_note") or "No major caveats."
    lines = [
        f"# {record.get('provider_name', '')} - {record.get('practice_name', '')}",
        "",
        "## Summary",
        f"- Credentials: {record.get('credentials', 'unknown')}",
        f"- License: {record.get('license_type', 'unknown')} / {record.get('license_status', 'unknown')} / {record.get('license_state', 'unknown')}",
        f"- Record confidence: {record.get('record_confidence', 0.0)}",
        f"- Last verified: {record.get('last_verified_at', '')}",
        "",
        "## Diagnostic capability",
        f"- ASD diagnosis: {record.get('diagnoses_asd', 'unclear')}",
        f"- ADHD diagnosis: {record.get('diagnoses_adhd', 'unclear')}",
        "",
        "## Prescribing capability",
        f"- Authority: {record.get('prescriptive_authority', 'unknown')}",
        f"- Basis: {record.get('prescriptive_basis', '')}",
        "",
        "## Practice details",
        f"- Practice: {record.get('practice_name', '')}",
        f"- City/state: {record.get('city', '')}, {record.get('location_state', '')}",
        f"- Metro: {record.get('metro', '')}",
        f"- Phone: {record.get('location_phone', '')}",
        f"- Telehealth: {record.get('telehealth', 'unknown')}",
        f"- Age groups: {record.get('age_groups_json', '[]')}",
        f"- Insurance notes: {record.get('insurance_notes', '') or 'unknown'}",
        f"- Waitlist notes: {record.get('waitlist_notes', '') or 'unknown'}",
        f"- Referral requirements: {record.get('referral_requirements', '') or 'unknown'}",
        "",
        "## Evidence links",
    ]
    for url in source_urls:
        lines.append(f"- {url}")
    if not source_urls:
        lines.append("- No evidence links captured.")
    lines.extend(["", "## Confidence and caveats", f"- Caveats: {caveats}"])
    if contradictions:
        lines.append(f"- Contradictions: {len(contradictions)} source conflicts captured.")
    return "\n".join(lines).strip() + "\n"


def _parse_json_array(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if not isinstance(parsed, list):
        return [text]
    return [str(item).strip() for item in parsed if str(item).strip()]


def _unique_nonempty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _clean_contact_name(raw: str) -> str:
    tokens = [token.strip(" ,.;:-") for token in str(raw or "").replace("Dr. ", "").replace("Dr ", "").split() if token.strip(" ,.;:-")]
    if len(tokens) < 2:
        return ""
    bad_tokens = {
        "and",
        "faculty",
        "member",
        "recognized",
        "work",
        "in",
        "new",
        "jersey",
        "co",
        "director",
        "learn",
        "more",
        "licensed",
        "psychologist",
        "faculty",
        "stay",
    }
    cleaned: list[str] = []
    for token in tokens:
        normalized = token.strip("-")
        lowered = normalized.lower()
        if lowered in bad_tokens:
            break
        if len(normalized) == 1 and normalized.islower():
            continue
        cleaned.append(normalized)
        if len(cleaned) >= 3:
            break
    if len(cleaned) < 2:
        return ""
    if len(cleaned) >= 3 and cleaned[0] == cleaned[2]:
        cleaned = cleaned[:2]
    if len(cleaned) >= 3 and cleaned[1] == cleaned[2]:
        cleaned = cleaned[:2]
    return " ".join(cleaned[:3])


def _canonical_contact_name(name: str) -> str:
    parts = [part.strip(".").lower() for part in name.split() if part.strip(".")]
    if len(parts) >= 2:
        return f"{parts[0]} {parts[-1]}"
    return " ".join(parts)


def _candidate_contact_names(rows: list[dict[str, object]]) -> list[str]:
    canonical_to_name: dict[str, str] = {}
    for row in rows:
        cleaned = _clean_contact_name(row.get("provider_name") or row.get("provider_name_snapshot") or "")
        if not cleaned:
            continue
        canonical = _canonical_contact_name(cleaned)
        if not canonical:
            continue
        existing = canonical_to_name.get(canonical)
        if existing is None or len(cleaned) < len(existing):
            canonical_to_name[canonical] = cleaned
    return list(canonical_to_name.values())


def _named_contact_role(primary: dict[str, object]) -> str:
    credentials = str(primary.get("credentials") or "").lower()
    website = str(primary.get("website") or "").lower()
    practice_name = str(primary.get("practice_name") or primary.get("practice_name_snapshot") or "").lower()
    if "md" in credentials or "do" in credentials:
        return "medical director"
    if "psyd" in credentials or "phd" in credentials:
        return "clinical lead / psychologist"
    if "rutgers" in website or "psychology" in practice_name or "clinic" in practice_name or "autism" in practice_name:
        return "clinical lead / faculty clinician"
    return "practice lead"


def _normalized_source_url(value: object, fallback: object = "") -> str:
    urls = _parse_json_array(value)
    if urls:
        return urls[0]
    return str(fallback or "").strip()


def _meaningful_quote(value: object) -> str:
    text = html.unescape(" ".join(str(value or "").split()))
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"yes", "no", "unknown", "unclear"}:
        return ""
    if len(text) < 24:
        return ""
    if "&n" in text or text.endswith("&"):
        return ""
    if text[0].islower():
        return ""
    if any(marker in lowered for marker in ("clinics and services", "profiles, and achievements", "leading provider of compassionate, quality")):
        return ""
    if len(text) > 180:
        text = text[:177].rsplit(" ", 1)[0] + "..."
    return text


def _short_signal_text(value: object, *, max_length: int = 140) -> str:
    text = html.unescape(" ".join(str(value or "").split()))
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"yes", "no", "unknown", "unclear"}:
        return ""
    if text[0].islower():
        return ""
    if "&n" in text or text.endswith("&"):
        return ""
    if any(marker in lowered for marker in ("clinics and services", "profiles, and achievements", "leading provider of compassionate, quality")):
        return ""
    if len(text) > max_length:
        text = text[: max_length - 3].rsplit(" ", 1)[0] + "..."
    return text


def _format_phone(value: object) -> str:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw


def _display_qa_state(value: object) -> str:
    text = str(value or "").strip().replace("_", " ")
    if not text:
        return "unknown"
    return text[:1].upper() + text[1:]


def _source_briefs(con: sqlite3.Connection, urls: list[str]) -> list[dict[str, str]]:
    briefs: list[dict[str, str]] = []
    for url in _unique_nonempty(urls)[:10]:
        row = con.execute("SELECT content FROM source_documents WHERE source_url=? ORDER BY fetched_at DESC LIMIT 1", (url,)).fetchone()
        if not row:
            continue
        content = str(row[0] or "")
        meta_match = re.search(r"<meta[^>]+name=[\"']description[\"'][^>]+content=[\"']([^\"']+)", content, re.IGNORECASE)
        if not meta_match:
            meta_match = re.search(r"<meta[^>]+property=[\"']og:description[\"'][^>]+content=[\"']([^\"']+)", content, re.IGNORECASE)
        title_match = re.search(r"<title>(.*?)</title>", content, re.IGNORECASE | re.DOTALL)
        summary = ""
        if meta_match:
            summary = _short_signal_text(meta_match.group(1), max_length=170)
        if not summary and title_match:
            summary = _short_signal_text(title_match.group(1), max_length=120)
        if not summary:
            continue
        briefs.append({"url": url, "summary": summary})
    return briefs


def _humanize_review_note(note: object) -> str:
    text = " ".join(str(note or "").split()).strip()
    lowered = text.lower()
    if not text:
        return ""
    if "practice offers evaluations but no named clinician was verified" in lowered:
        return "Service-level evidence is present, but no named clinician is verified yet."
    if "practice-only signal" in lowered:
        return "Use this as account-level research only; a clinician-level outreach target is not verified yet."
    if "low_confidence" in lowered or "missing_critical:" in lowered:
        missing_fields: list[str] = []
        if "missing_critical:" in lowered:
            trailing = lowered.split("missing_critical:", 1)[1]
            missing_fields = [field.strip() for field in trailing.split(",") if field.strip()]
        display_fields = []
        field_labels = {
            "diagnoses_asd": "autism-diagnosis capability",
            "diagnoses_adhd": "ADHD-diagnosis capability",
            "license_status": "license status",
            "prescriptive_authority": "prescriptive authority",
        }
        for field in missing_fields:
            display_fields.append(field_labels.get(field, field.replace("_", " ")))
        if display_fields:
            return "Named clinician references were found, but verification is still missing for " + ", ".join(display_fields) + "."
        return "Named clinician references were found, but the record still needs verification before outreach."
    return text


def _clean_market_city(value: object) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""
    lowered = f" {text.lower()} "
    if any(marker in lowered for marker in (" road ", " avenue ", " ave ", " street ", " blvd ", " boulevard ", " drive ", " lane ")):
        parts = text.split()
        if len(parts) >= 2 and parts[-2].lower() in {"new", "west", "east", "north", "south"}:
            return " ".join(parts[-2:])
        return parts[-1]
    return text


def _review_signal_priority(practice_name: str, source_url: str) -> int:
    text = f"{practice_name} {source_url}".lower()
    positive_markers = (
        "evaluation",
        "evaluations",
        "assessment",
        "assessments",
        "autism",
        "adhd",
        "clinic",
        "clinics",
        "center",
        "services",
        "diagnostic",
        "developmental",
    )
    negative_markers = (
        "frequently asked questions",
        "faq",
        "healthier me",
        "approach-to-treatment",
        "approach to treatment",
        "before-your-assessment",
        "before your assessment",
        "billing",
        "insurance",
        "directions",
        "resources",
        "news",
        "blog",
    )
    score = 0
    for marker in positive_markers:
        if marker in text:
            score += 8
    for marker in negative_markers:
        if marker in text:
            score -= 12
    return score


def _review_account_key(source_url: str) -> str:
    parsed = urlparse(str(source_url or "").strip())
    domain = parsed.netloc.lower()
    segments = [part.strip().lower() for part in parsed.path.split("/") if part.strip()]
    if not segments:
        return domain or "review-only"
    if segments[0] == "treatment-care" and len(segments) >= 4:
        scope = segments[:4]
    elif segments[0] == "centers-clinics" and len(segments) >= 2:
        scope = segments[:2]
    else:
        scope = segments[: min(len(segments), 2)]
    return "::".join([domain, *scope])


def _review_only_bundle(row: dict[str, object]) -> dict[str, object]:
    source_url = _normalized_source_url(row.get("source_urls_json"), row.get("source_url"))
    evidence: list[dict[str, object]] = []
    for field in ("referral_requirements", "insurance_notes", "waitlist_notes"):
        quote = _meaningful_quote(row.get(field) or "")
        if not quote:
            continue
        evidence.append(
            {
                "field_name": field,
                "field_value": str(row.get(field) or "").strip(),
                "quote": quote,
                "source_url": source_url,
            }
        )
    return {
        "record": row,
        "evidence": evidence,
        "contradictions": [],
    }


def _qa_state(rows: list[dict[str, object]], review_types: set[str]) -> str:
    if any(str(row.get("export_status") or "") == "approved" for row in rows):
        if any(int(row.get("outreach_ready") or 0) == 1 for row in rows):
            return "approved_outreach_ready"
        return "approved"
    if "practice_only_signal" in review_types:
        return "practice_signal"
    if "indirect_provider_signal" in review_types:
        return "indirect_provider_signal"
    if any(str(row.get("review_status") or "") == "queued" for row in rows):
        return "under_review"
    return "candidate"


def _budget_band(rows: list[dict[str, object]]) -> str:
    practice_name = str(rows[0].get("practice_name") or "").lower()
    website = str(rows[0].get("website") or "").lower()
    provider_count = len(_candidate_contact_names(rows))
    location_count = len(
        {
            (
                str(row.get("city") or "").strip().lower(),
                str(row.get("location_state") or "").strip().lower(),
                str(row.get("phone") or row.get("location_phone") or "").strip(),
            )
            for row in rows
        }
    )
    if provider_count >= 4 or location_count >= 3 or any(token in practice_name for token in ("hospital", "health system", "barnabas")):
        return "Enterprise / multi-site"
    if any(token in f"{practice_name} {website}" for token in ("rutgers", "university", "college", "school of", "gsapp")):
        return "Institutional / specialty program"
    if provider_count >= 2 or location_count >= 2:
        return "Regional / growing group"
    return "Single-site / owner-led"


def _service_focus(rows: list[dict[str, object]]) -> str:
    text = " ".join(
        [
            str(rows[0].get("practice_name") or rows[0].get("practice_name_snapshot") or ""),
            " ".join(_parse_json_array(rows[0].get("source_urls_json"))),
        ]
    ).lower()
    has_asd = any(str(row.get("diagnoses_asd") or "unclear") == "yes" for row in rows)
    has_adhd = any(str(row.get("diagnoses_adhd") or "unclear") == "yes" for row in rows)
    if "autism" in text or "asd" in text:
        has_asd = True
    if "adhd" in text:
        has_adhd = True
    if has_asd and has_adhd:
        return "ASD and ADHD evaluations"
    if has_asd:
        return "Autism evaluations"
    if has_adhd:
        return "ADHD evaluations"
    return "Developmental and behavioral services"


def _operating_signals(rows: list[dict[str, object]]) -> list[str]:
    primary = rows[0]
    signals: list[str] = []
    if str(primary.get("website") or "").strip():
        signals.append("Public website available")
    if str(primary.get("intake_url") or "").strip():
        signals.append("Online intake or referral path published")
    if str(primary.get("telehealth") or "").strip() == "yes":
        signals.append("Telehealth or virtual visits called out")
    referral_notes = _short_signal_text(primary.get("referral_requirements") or "", max_length=130)
    insurance_notes = _short_signal_text(primary.get("insurance_notes") or "", max_length=130)
    waitlist_notes = _short_signal_text(primary.get("waitlist_notes") or "", max_length=130)
    if referral_notes:
        signals.append(f"Referral workflow noted: {referral_notes}")
    if insurance_notes:
        signals.append(f"Insurance signal: {insurance_notes}")
    if waitlist_notes:
        signals.append(f"Scheduling/waitlist signal: {waitlist_notes}")
    if str(primary.get("phone") or primary.get("location_phone") or "").strip():
        signals.append("Direct phone contact published")
    return _unique_nonempty(signals)


def _evidence_links(rows: list[dict[str, object]], primary_bundle: dict[str, object]) -> list[str]:
    urls: list[str] = []
    for row in rows:
        urls.extend(_parse_json_array(row.get("source_urls_json")))
    for item in list(primary_bundle.get("evidence") or []):
        candidate = str(item.get("source_url") or "").strip()
        if candidate:
            urls.append(candidate)
    return _unique_nonempty(urls)


def _evidence_quotes(primary_bundle: dict[str, object], source_briefs: list[dict[str, str]]) -> list[str]:
    quotes: list[str] = []
    for item in list(primary_bundle.get("evidence") or []):
        quote = _meaningful_quote(item.get("quote") or "")
        if quote:
            quotes.append(quote)
    for brief in source_briefs:
        summary = _short_signal_text(brief.get("summary") or "", max_length=170)
        if summary:
            quotes.append(summary)
    return _unique_nonempty(quotes)[:3]


def _why_this_lead_matters(
    rows: list[dict[str, object]],
    primary_bundle: dict[str, object],
    qa_state: str,
    source_briefs: list[dict[str, str]],
) -> list[str]:
    primary = rows[0]
    focus = _service_focus(rows)
    fit_score = round(float(primary.get("outreach_fit_score") or 0.0) * 100)
    confidence = round(float(primary.get("record_confidence") or 0.0) * 100)
    reasons = [
        f"Public evidence supports {focus.lower()} with fit {fit_score}/100 and confidence {confidence}/100.",
    ]
    if str(primary.get("license_status") or "").strip() == "active":
        reasons.append("Licensing status is active on the strongest available provider-backed record.")
    if str(primary.get("intake_url") or "").strip() or str(primary.get("referral_requirements") or "").strip():
        reasons.append("The account exposes a visible intake/referral workflow, which is a strong operations-entry signal.")
    if qa_state == "practice_signal":
        reasons.append("This is still a practice-signal lead, so messaging should focus on operations and referral workflow rather than clinician-specific claims.")
    evidence_summary = _evidence_summary(primary_bundle)
    if evidence_summary:
        reasons.append(f"Best evidence in hand: {evidence_summary}")
    elif source_briefs:
        reasons.append(f"Best public signal in hand: {source_briefs[0]['summary']}")
    return _unique_nonempty(reasons)[:4]


def _best_channel(role: str, primary: dict[str, object]) -> str:
    if "intake" in role.lower() or "administrator" in role.lower():
        if str(primary.get("phone") or primary.get("location_phone") or "").strip():
            return "Phone"
    if str(primary.get("intake_url") or "").strip():
        return "Website form"
    if str(primary.get("website") or "").strip():
        return "Website / contact form"
    if str(primary.get("phone") or primary.get("location_phone") or "").strip():
        return "Phone"
    return "Manual research"


def _contact_playbook(rows: list[dict[str, object]], qa_state: str) -> list[dict[str, str]]:
    primary = rows[0]
    focus = _service_focus(rows).lower()
    contacts: list[dict[str, str]] = []
    candidate_names = _candidate_contact_names(rows)
    provider_name = candidate_names[0] if candidate_names else ""
    credentials = str(primary.get("credentials") or "").strip()
    intake_signal = "the published intake/referral path" if str(primary.get("intake_url") or "").strip() else "the public service page"

    if provider_name:
        role = _named_contact_role(primary)
        contacts.append(
            {
                "name": f"{provider_name}{', ' + credentials if credentials else ''}",
                "role": role,
                "influence": "High",
                "what_they_care_about": f"Clinical quality, referral fit, and capacity for {focus}",
                "best_angle": "Referral-fit clarity and smoother intake handoff for the target service line.",
                "conversation_hook": f"Lead with the public service evidence and use {intake_signal} as proof of real workflow exposure.",
                "likely_objection": "May see outreach as generic or non-clinical.",
                "first_cta": "Ask for a 15-minute referral and intake workflow review.",
                "best_channel": _best_channel(role, primary),
            }
        )

    contacts.append(
        {
            "name": "Practice Administrator (inferred)",
            "role": "operations / admin lead",
            "influence": "High",
            "what_they_care_about": "Intake throughput, referral routing, scheduling friction, and staff utilization",
            "best_angle": "Workflow relief, faster reporting, and fewer manual handoffs.",
            "conversation_hook": f"Use {intake_signal} to open on scheduling friction and incomplete referrals.",
            "likely_objection": "Tool fatigue or concern about implementation overhead.",
            "first_cta": "Offer a workflow audit tied to referral conversion and faster scheduling.",
            "best_channel": _best_channel("operations / admin lead", primary),
        }
    )

    contacts.append(
        {
            "name": "Intake / Referral Coordinator (inferred)",
            "role": "front-line workflow owner",
            "influence": "Medium",
            "what_they_care_about": "Reducing back-and-forth, incomplete referrals, and dropped evaluations",
            "best_angle": "Fewer incomplete referrals and a cleaner intake path.",
            "conversation_hook": "Anchor on the practice's public referral and intake flow rather than product features.",
            "likely_objection": "May not control budget.",
            "first_cta": "Request 10 minutes to map the current intake steps and friction points.",
            "best_channel": _best_channel("Intake / Referral Coordinator", primary),
        }
    )

    if qa_state != "approved_outreach_ready":
        contacts.append(
            {
                "name": "Owner / Budget Holder (inferred)",
                "role": "economic buyer",
                "influence": "High",
                "what_they_care_about": "Evaluation demand capture, growth efficiency, and team leverage",
                "best_angle": "Operational leverage without adding administrative drag.",
                "conversation_hook": "Tie the pitch to public operating signals rather than unsupported provider claims.",
                "likely_objection": "Will ask for proof the workflow gap is real.",
                "first_cta": "Share a concise before/after workflow hypothesis and ask for validation.",
                "best_channel": _best_channel("Owner / Budget Holder", primary),
            }
        )

    deduped: list[dict[str, str]] = []
    seen_roles: set[str] = set()
    for contact in contacts:
        role = contact["role"]
        if role in seen_roles:
            continue
        seen_roles.add(role)
        deduped.append(contact)
    return deduped[:4]


def _recommended_sequence(primary: dict[str, object], contacts: list[dict[str, str]], qa_state: str) -> list[str]:
    practice_name = str(primary.get("practice_name") or primary.get("practice_name_snapshot") or "the account").strip()
    first_contact = contacts[0]["name"] if contacts else "the operations contact"
    first_channel = contacts[0]["best_channel"] if contacts else "best available channel"
    sequence = [
        f"Start with {first_contact} at {practice_name} via {first_channel}, opening on public intake/referral workflow friction rather than feature claims.",
        "Follow with the clinical lead using service-specific evidence and a workflow hypothesis tied to referrals, scheduling, and capacity.",
    ]
    if qa_state != "approved_outreach_ready":
        sequence.append("Keep claims narrow: position this as an account-intel lead until another provider-backed signal is confirmed.")
    else:
        sequence.append("Escalate to the economic buyer once the operational pain is validated and the clinical lead confirms relevance.")
    if contacts:
        sequence.append(f"Bring the economic buyer in only after the workflow gap is validated and the team agrees the pain is real.")
    return sequence[:4]


def _dossier_bundle(
    *,
    rows: list[dict[str, object]],
    reviews: list[dict[str, object]],
    primary_bundle: dict[str, object],
    source_briefs: list[dict[str, str]],
) -> dict[str, object]:
    primary = rows[0]
    review_types = {str(item.get("review_type") or "") for item in reviews if str(item.get("review_type") or "")}
    qa_state = _qa_state(rows, review_types)
    contacts = _contact_playbook(rows, qa_state)
    evidence_links = _evidence_links(rows, primary_bundle)
    evidence_quotes = _evidence_quotes(primary_bundle, source_briefs)
    provider_names = _unique_nonempty(
        _candidate_contact_names(rows)
    )
    location_labels = _unique_nonempty(
        [
            ", ".join(part for part in [str(row.get("city") or "").strip(), str(row.get("location_state") or "").strip()] if part)
            for row in rows
        ]
    )
    return {
        "practice_name": str(primary.get("practice_name") or primary.get("practice_name_snapshot") or "").strip(),
        "city": _clean_market_city(primary.get("city") or ""),
        "state": str(primary.get("location_state") or "").strip(),
        "metro": str(primary.get("metro") or "").strip(),
        "website": str(primary.get("website") or "").strip(),
        "intake_url": str(primary.get("intake_url") or "").strip(),
        "phone": _format_phone(primary.get("phone") or primary.get("location_phone") or ""),
        "fit_score": round(float(primary.get("outreach_fit_score") or 0.0) * 100),
        "confidence_score": round(float(primary.get("record_confidence") or 0.0) * 100),
        "qa_state": qa_state,
        "budget_band": _budget_band(rows),
        "service_focus": _service_focus(rows),
        "provider_count": len(provider_names),
        "named_providers": provider_names[:4],
        "location_count": len(location_labels),
        "locations": location_labels[:4],
        "operating_signals": _operating_signals(rows),
        "why_this_lead_matters": _why_this_lead_matters(rows, primary_bundle, qa_state, source_briefs),
        "contacts": contacts,
        "recommended_sequence": _recommended_sequence(primary, contacts, qa_state),
        "evidence_links": evidence_links[:10],
        "evidence_quotes": evidence_quotes,
        "source_briefs": source_briefs[:3],
        "review_types": sorted(review_types),
        "review_notes": _unique_nonempty([_humanize_review_note(item.get("reason") or "") for item in reviews])[:4],
    }


def _contact_profile_markdown(dossier: dict[str, object], contact: dict[str, str]) -> str:
    lines = [
        f"# {contact['name']}",
        "",
        "## Role",
        f"- Account: {dossier['practice_name']}",
        f"- Role: {contact['role']}",
        f"- Influence: {contact['influence']}",
        f"- Best channel: {contact['best_channel']}",
        "",
        "## What They Care About",
        f"- {contact['what_they_care_about']}",
        "",
        "## Best Angle",
        f"- {contact['best_angle']}",
        "",
        "## Conversation Hook",
        f"- {contact['conversation_hook']}",
        "",
        "## Likely Objection",
        f"- {contact['likely_objection']}",
        "",
        "## First CTA",
        f"- {contact['first_cta']}",
        "",
        "## Evidence In Hand",
    ]
    for quote in list(dossier.get("evidence_quotes") or []):
        lines.append(f"- {quote}")
    if not list(dossier.get("evidence_quotes") or []):
        lines.append("- Use the linked source pages to verify the service line and intake path before outreach.")
    lines.extend(["", "## Sources"])
    for url in list(dossier.get("evidence_links") or []):
        lines.append(f"- {url}")
    return "\n".join(lines).strip() + "\n"


def _lead_dossier_markdown(dossier: dict[str, object]) -> str:
    lines = [
        f"# Lead Intelligence Report - {dossier['practice_name']}",
        "",
        "## Report Details",
        f"- Status: {_display_qa_state(dossier['qa_state'])}",
        f"- Budget band: {dossier['budget_band']}",
        f"- Fit score: {dossier['fit_score']}/100",
        f"- Contacts mapped: {len(list(dossier.get('contacts') or []))}",
        "",
        "## Company Snapshot",
        f"- Account: {dossier['practice_name']}",
        f"- Research confidence: {dossier['confidence_score']}/100",
        f"- Primary market: {dossier['city']}, {dossier['state']} ({dossier['metro'] or 'metro unknown'})",
        f"- Service focus: {dossier['service_focus']}",
        f"- Provider count in hand: {dossier['provider_count']}",
        f"- Location count in hand: {dossier['location_count']}",
        "",
        "## Operating Environment",
    ]
    for signal in list(dossier.get("operating_signals") or []):
        lines.append(f"- {signal}")
    lines.extend(["", "## Why This Lead Stands Out"])
    for item in list(dossier.get("why_this_lead_matters") or []):
        lines.append(f"- {item}")
    lines.extend(["", "## Decision Network Matrix"])
    for contact in list(dossier.get("contacts") or []):
        lines.append(
            f"- {contact['role']} | {contact['name']} | Influence: {contact['influence']} | What they care about: {contact['what_they_care_about']} | Best angle: {contact['best_angle']} | Profile: {contact.get('profile_file', 'n/a')}"
        )
    lines.extend(["", "## Contact Playbook"])
    for contact in list(dossier.get("contacts") or []):
        lines.append(f"- {contact['name']}")
        lines.append(f"  Best angle: {contact['best_angle']}")
        lines.append(f"  Conversation hook: {contact['conversation_hook']}")
        lines.append(f"  Likely objection: {contact['likely_objection']}")
        lines.append(f"  First CTA: {contact['first_cta']}")
        lines.append(f"  Best channel: {contact['best_channel']}")
    lines.extend(["", "## Recommended Sequence"])
    for idx, step in enumerate(list(dossier.get("recommended_sequence") or []), start=1):
        lines.append(f"- {idx}. {step}")
    lines.extend(["", "## Evidence In Hand"])
    for quote in list(dossier.get("evidence_quotes") or []):
        lines.append(f"- {quote}")
    if not list(dossier.get("evidence_quotes") or []):
        lines.append("- See linked evidence URLs and profile files.")
    lines.extend(["", "## Method & Files"])
    lines.append("- This executive report stays concise on purpose; the deeper contact workups live in the linked markdown profiles.")
    for contact in list(dossier.get("contacts") or []):
        if contact.get("profile_file"):
            lines.append(f"- {contact['name']}: {contact['profile_file']}")
    if list(dossier.get("review_notes") or []):
        lines.extend(["", "## Verification Notes"])
        for note in list(dossier.get("review_notes") or []):
            lines.append(f"- {note}")
    lines.extend(["", "## Evidence Links"])
    for url in list(dossier.get("evidence_links") or []):
        lines.append(f"- {url}")
    if not list(dossier.get("evidence_links") or []):
        lines.append("- No evidence links captured.")
    return "\n".join(lines).strip() + "\n"


def _internal_review_summary_markdown(summary: dict[str, object]) -> str:
    lines = [
        f"# Internal Review Account Summary - {summary['practice_name']}",
        "",
        "## Review Status",
        f"- State: {_display_qa_state(summary['qa_state'])}",
        f"- Confidence: {summary['confidence_score']}/100",
        f"- Service focus: {summary['service_focus']}",
        f"- Named providers verified: {summary['provider_count']}",
        "",
        "## Why This Account Is Still Internal",
    ]
    for note in list(summary.get("review_notes") or []):
        lines.append(f"- {note}")
    if not list(summary.get("review_notes") or []):
        lines.append("- Public service signals exist, but this account has not crossed the export gate.")
    lines.extend(["", "## Public Signals"])
    for signal in list(summary.get("operating_signals") or []):
        lines.append(f"- {signal}")
    if not list(summary.get("operating_signals") or []):
        lines.append("- No operating signals captured.")
    lines.extend(["", "## Evidence In Hand"])
    for quote in list(summary.get("evidence_quotes") or []):
        lines.append(f"- {quote}")
    if not list(summary.get("evidence_quotes") or []):
        lines.append("- Review the linked source pages directly.")
    lines.extend(["", "## Evidence Links"])
    for url in list(summary.get("evidence_links") or []):
        lines.append(f"- {url}")
    if not list(summary.get("evidence_links") or []):
        lines.append("- No evidence links captured.")
    return "\n".join(lines).strip() + "\n"


def _approved_dossier_candidates(con: sqlite3.Connection, *, limit: int) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in con.execute(
            """
            SELECT pr.record_id, pr.practice_id, pr.location_id, pr.provider_name_snapshot, pr.practice_name_snapshot,
                   pr.license_state, pr.license_type, pr.license_status, pr.diagnoses_asd, pr.diagnoses_adhd,
                   pr.prescriptive_authority, pr.prescriptive_basis, pr.telehealth, pr.insurance_notes,
                   pr.waitlist_notes, pr.referral_requirements, pr.source_urls_json, pr.field_confidence_json,
                   pr.record_confidence, pr.outreach_fit_score, pr.outreach_ready, pr.review_status, pr.export_status,
                   pr.blocked_reason, pr.conflict_note, pr.last_verified_at,
                   p.provider_name, p.credentials,
                   pt.website, pt.intake_url, COALESCE(pl.phone, pt.phone, '') AS phone,
                   pl.city, pl.state AS location_state, pl.metro
            FROM provider_practice_records pr
            LEFT JOIN providers p ON p.provider_id = pr.provider_id
            INNER JOIN practices pt ON pt.practice_id = pr.practice_id
            INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
            WHERE pr.export_status='approved'
            ORDER BY pr.outreach_fit_score DESC, pr.record_confidence DESC, pr.practice_name_snapshot ASC
            LIMIT ?
            """,
            (limit,),
        )
    ]


def _internal_review_groups(
    con: sqlite3.Connection,
    *,
    review_rows: list[dict[str, object]],
    limit: int,
) -> dict[str, list[dict[str, object]]]:
    grouped_candidates: dict[str, list[dict[str, object]]] = defaultdict(list)
    seen_group_sources: set[tuple[str, str]] = set()
    for review_row in review_rows[: max(limit * 4, 20)]:
        review_type = str(review_row.get("review_type") or "").strip()
        practice_name = str(review_row.get("practice_name") or "").strip()
        source_url = str(review_row.get("source_url") or "").strip()
        if review_type not in {"missing_provider", "practice_only_signal"} or not practice_name or not source_url:
            continue
        review_priority = _review_signal_priority(practice_name, source_url)
        if review_priority < 8:
            continue
        review_group_key = f"review-only:{_safe_slug(_review_account_key(source_url))}"
        if (review_group_key, source_url) in seen_group_sources:
            continue
        extracted_matches = [
            dict(row)
            for row in con.execute(
                """
                SELECT practice_name, source_url, phone, intake_url, city, state, metro, diagnoses_asd, diagnoses_adhd,
                       referral_requirements, insurance_notes
                FROM extracted_records
                WHERE source_url=?
                """,
                (source_url,),
            )
        ]
        if not extracted_matches:
            continue
        seen_group_sources.add((review_group_key, source_url))
        for match in extracted_matches:
            grouped_candidates[review_group_key].append(
                {
                    "record_id": "",
                    "practice_id": review_group_key,
                    "provider_name": "",
                    "provider_name_snapshot": "",
                    "practice_name": practice_name,
                    "practice_name_snapshot": practice_name,
                    "website": source_url,
                    "source_url": source_url,
                    "intake_url": str(match.get("intake_url") or "").strip(),
                    "phone": str(match.get("phone") or "").strip(),
                    "city": str(match.get("city") or "").strip(),
                    "location_state": str(match.get("state") or "").strip(),
                    "metro": str(match.get("metro") or "").strip(),
                    "diagnoses_asd": str(match.get("diagnoses_asd") or "unclear"),
                    "diagnoses_adhd": str(match.get("diagnoses_adhd") or "unclear"),
                    "referral_requirements": str(match.get("referral_requirements") or "").strip(),
                    "insurance_notes": str(match.get("insurance_notes") or "").strip(),
                    "telehealth": "unknown",
                    "record_confidence": 0.18 if review_type == "practice_only_signal" else 0.22,
                    "outreach_fit_score": 0.24 if str(match.get("diagnoses_asd") or "unclear") == "yes" else 0.16,
                    "outreach_ready": 0,
                    "review_status": "queued",
                    "export_status": "review_only",
                    "blocked_reason": str(review_row.get("reason") or "").strip(),
                    "source_urls_json": json.dumps([source_url]),
                    "credentials": "",
                    "_review_priority": review_priority,
                }
            )
    return grouped_candidates


def _markdown_to_html(markdown: str) -> str:
    html_lines = [
        "<html><head><meta charset='utf-8'><style>",
        "body{font-family:Helvetica,Arial,sans-serif;margin:40px;color:#1f2933;}",
        "h1,h2{color:#0f172a;} ul{padding-left:20px;} li{margin:4px 0;} p{line-height:1.5;}",
        "</style></head><body>",
    ]
    in_list = False
    for raw in markdown.splitlines():
        line = raw.strip()
        if not line:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            continue
        if line.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h1>{html.escape(line[2:])}</h1>")
            continue
        if line.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{html.escape(line[3:])}</h2>")
            continue
        if line.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{html.escape(line[2:])}</li>")
            continue
        if in_list:
            html_lines.append("</ul>")
            in_list = False
        html_lines.append(f"<p>{html.escape(line)}</p>")
    if in_list:
        html_lines.append("</ul>")
    html_lines.append("</body></html>")
    return "\n".join(html_lines)


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _fallback_pdf_bytes(markdown: str) -> bytes:
    lines = []
    for raw in markdown.splitlines():
        if not raw.strip():
            lines.append("")
            continue
        lines.extend(textwrap.wrap(raw, width=88) or [""])
    content_lines = ["BT", "/F1 10 Tf", "50 760 Td", "14 TL"]
    for line in lines[:48]:
        content_lines.append(f"({_pdf_escape(line)}) Tj")
        content_lines.append("T*")
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="ignore")

    objects = [
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n",
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n",
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n",
        f"4 0 obj << /Length {len(stream)} >> stream\n".encode("latin-1") + stream + b"\nendstream endobj\n",
        b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n",
    ]

    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(output))
        output.extend(obj)
    xref_start = len(output)
    output.extend(f"xref\n0 {len(offsets)}\n".encode("latin-1"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
    output.extend(
        f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode("latin-1")
    )
    return bytes(output)


def _write_pdf(markdown: str, pdf_path: Path) -> None:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "LeadTitle",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=20,
            leading=24,
            textColor=colors.HexColor("#0f172a"),
            spaceAfter=12,
        )
        heading_style = ParagraphStyle(
            "LeadHeading",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=16,
            textColor=colors.HexColor("#1d4ed8"),
            spaceBefore=10,
            spaceAfter=6,
        )
        body_style = ParagraphStyle(
            "LeadBody",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=5,
        )

        story: list[object] = []
        bullet_buffer: list[str] = []

        def flush_bullets() -> None:
            nonlocal bullet_buffer
            if not bullet_buffer:
                return
            story.append(
                ListFlowable(
                    [ListItem(Paragraph(html.escape(item), body_style)) for item in bullet_buffer],
                    bulletType="bullet",
                    leftIndent=14,
                )
            )
            story.append(Spacer(1, 6))
            bullet_buffer = []

        for raw in markdown.splitlines():
            line = raw.rstrip()
            stripped = line.strip()
            if not stripped:
                flush_bullets()
                story.append(Spacer(1, 4))
                continue
            if stripped.startswith("# "):
                flush_bullets()
                story.append(Paragraph(html.escape(stripped[2:]), title_style))
                continue
            if stripped.startswith("## "):
                flush_bullets()
                story.append(Paragraph(html.escape(stripped[3:]), heading_style))
                continue
            if stripped.startswith("- "):
                bullet_buffer.append(stripped[2:])
                continue
            flush_bullets()
            story.append(Paragraph(html.escape(stripped), body_style))

        flush_bullets()
        doc = SimpleDocTemplate(
            str(pdf_path),
            pagesize=LETTER,
            leftMargin=42,
            rightMargin=42,
            topMargin=44,
            bottomMargin=40,
        )
        doc.build(story)
        return
    except Exception:
        pdf_path.write_bytes(_fallback_pdf_bytes(markdown))


def export_provider_intel(con: sqlite3.Connection, out_dir: Path, run_id: str, limit: int = 100) -> dict[str, object]:
    root = out_dir / "provider_intel"
    root.mkdir(parents=True, exist_ok=True)
    records_path = root / f"provider_records_{run_id}.csv"
    json_path = root / f"provider_records_{run_id}.json"
    review_path = root / f"review_queue_{run_id}.csv"
    sales_path = root / f"sales_report_{run_id}.csv"
    dossiers_csv_path = root / f"lead_intelligence_{run_id}.csv"
    dossiers_json_path = root / f"lead_intelligence_{run_id}.json"
    internal_review_csv_path = root / f"internal_review_accounts_{run_id}.csv"
    internal_review_json_path = root / f"internal_review_accounts_{run_id}.json"
    profiles_dir = root / "profiles"
    evidence_dir = root / "evidence"
    outreach_dir = root / "outreach"
    dossiers_dir = root / "dossiers" / run_id
    internal_review_dir = root / "internal_review" / run_id
    profiles_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    outreach_dir.mkdir(parents=True, exist_ok=True)
    if dossiers_dir.exists():
        shutil.rmtree(dossiers_dir)
    dossiers_dir.mkdir(parents=True, exist_ok=True)
    if internal_review_dir.exists():
        shutil.rmtree(internal_review_dir)
    internal_review_dir.mkdir(parents=True, exist_ok=True)

    approved_rows = con.execute(
        """
        SELECT pr.record_id, pr.provider_id, p.provider_name, p.credentials, p.npi, pr.license_state, pr.license_type, pr.license_status,
               pr.practice_name_snapshot AS practice_name, pl.city, pl.state, pl.metro, pl.phone, pt.website, pt.intake_url,
               pr.diagnoses_asd, pr.diagnoses_adhd, pr.prescriptive_authority, pr.prescriptive_basis, pr.age_groups_json,
               pr.telehealth, pr.insurance_notes, pr.waitlist_notes, pr.referral_requirements, pr.source_urls_json,
               pr.field_confidence_json, pr.record_confidence, pr.outreach_fit_score, pr.outreach_ready, pr.outreach_reasons_json,
               pr.last_verified_at
        FROM provider_practice_records pr
        INNER JOIN providers p ON p.provider_id = pr.provider_id
        INNER JOIN practices pt ON pt.practice_id = pr.practice_id
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        WHERE pr.export_status='approved'
        ORDER BY pr.outreach_fit_score DESC, pr.record_confidence DESC, p.provider_name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    fieldnames = [
        "record_id",
        "provider_id",
        "provider_name",
        "credentials",
        "npi",
        "license_state",
        "license_type",
        "license_status",
        "practice_name",
        "city",
        "state",
        "metro",
        "phone",
        "website",
        "intake_url",
        "diagnoses_asd",
        "diagnoses_adhd",
        "prescriptive_authority",
        "prescriptive_basis",
        "age_groups",
        "telehealth",
        "insurance_notes",
        "waitlist_notes",
        "referral_requirements",
        "source_urls",
        "field_confidence",
        "record_confidence",
        "outreach_fit_score",
        "outreach_ready",
        "outreach_reasons",
        "last_verified_at",
    ]
    export_rows: list[dict[str, object]] = []
    for row in approved_rows:
        source_urls = json.loads(row["source_urls_json"] or "[]")
        field_confidence = json.loads(row["field_confidence_json"] or "{}")
        outreach_reasons = json.loads(row["outreach_reasons_json"] or "[]")
        export_rows.append(
            {
                "record_id": row["record_id"],
                "provider_id": row["provider_id"],
                "provider_name": row["provider_name"],
                "credentials": row["credentials"],
                "npi": row["npi"],
                "license_state": row["license_state"],
                "license_type": row["license_type"],
                "license_status": row["license_status"],
                "practice_name": row["practice_name"],
                "city": row["city"],
                "state": row["state"],
                "metro": row["metro"],
                "phone": row["phone"],
                "website": row["website"],
                "intake_url": row["intake_url"],
                "diagnoses_asd": row["diagnoses_asd"],
                "diagnoses_adhd": row["diagnoses_adhd"],
                "prescriptive_authority": row["prescriptive_authority"],
                "prescriptive_basis": row["prescriptive_basis"],
                "age_groups": json.loads(row["age_groups_json"] or "[]"),
                "telehealth": row["telehealth"],
                "insurance_notes": row["insurance_notes"] or "unknown",
                "waitlist_notes": row["waitlist_notes"] or "unknown",
                "referral_requirements": row["referral_requirements"] or "unknown",
                "source_urls": source_urls,
                "field_confidence": field_confidence,
                "record_confidence": row["record_confidence"],
                "outreach_fit_score": row["outreach_fit_score"],
                "outreach_ready": int(row["outreach_ready"] or 0),
                "outreach_reasons": outreach_reasons,
                "last_verified_at": row["last_verified_at"],
            }
        )

    with records_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in export_rows:
            flattened = dict(row)
            flattened["age_groups"] = json.dumps(flattened["age_groups"])
            flattened["source_urls"] = json.dumps(flattened["source_urls"])
            flattened["field_confidence"] = json.dumps(flattened["field_confidence"], sort_keys=True)
            flattened["outreach_reasons"] = json.dumps(flattened["outreach_reasons"], sort_keys=True)
            writer.writerow(flattened)

    json_path.write_text(json.dumps(export_rows, indent=2, default=str), encoding="utf-8")

    sales_rows: list[dict[str, object]] = []
    for row in approved_rows:
        bundle = _bundle(con, row["record_id"])
        slug = _safe_slug(f"{row['provider_name']}-{row['practice_name']}")
        record_dir = profiles_dir / f"{row['record_id']}-{slug}"
        record_dir.mkdir(parents=True, exist_ok=True)
        evidence_path = evidence_dir / f"{row['record_id']}.json"
        markdown_path = record_dir / "profile.md"
        pdf_path = record_dir / "profile.pdf"
        markdown = _markdown_profile(bundle)
        markdown_path.write_text(markdown, encoding="utf-8")
        evidence_path.write_text(json.dumps(bundle, indent=2, default=str), encoding="utf-8")
        _write_pdf(markdown, pdf_path)
        if int(row["outreach_ready"] or 0):
            sales_row = _sales_bundle_row(bundle)
            sales_rows.append(sales_row)
            outreach_record_dir = outreach_dir / f"{row['record_id']}-{slug}"
            outreach_record_dir.mkdir(parents=True, exist_ok=True)
            sales_markdown_path = outreach_record_dir / "sales_brief.md"
            sales_pdf_path = outreach_record_dir / "sales_brief.pdf"
            sales_markdown = _sales_markdown(bundle)
            sales_markdown_path.write_text(sales_markdown, encoding="utf-8")
            _write_pdf(sales_markdown, sales_pdf_path)

    sales_fieldnames = [
        "record_id",
        "provider_name",
        "credentials",
        "practice_name",
        "city",
        "state",
        "metro",
        "phone",
        "website",
        "intake_url",
        "diagnoses_asd",
        "diagnoses_adhd",
        "license_status",
        "prescriptive_authority",
        "record_confidence",
        "outreach_fit_score",
        "target_buyer",
        "outreach_angle",
        "opener",
        "evidence_summary",
        "source_urls",
    ]
    with sales_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sales_fieldnames)
        writer.writeheader()
        for row in sales_rows:
            flattened = dict(row)
            flattened["source_urls"] = json.dumps(flattened["source_urls"])
            writer.writerow(flattened)

    review_rows = [
        dict(row)
        for row in con.execute(
            """
            SELECT review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at
            FROM review_queue
            ORDER BY created_at DESC, review_id ASC
            """
        )
    ]
    with review_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["review_id", "record_id", "review_type", "provider_name", "practice_name", "reason", "source_url", "evidence_quote", "status", "created_at"],
        )
        writer.writeheader()
        writer.writerows(review_rows)

    dossier_candidate_rows = _approved_dossier_candidates(con, limit=max(limit * 3, 15))
    grouped_candidates: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in dossier_candidate_rows:
        grouped_candidates[str(row.get("practice_id") or row.get("practice_name_snapshot") or row.get("record_id") or "")].append(row)

    dossier_rows: list[dict[str, object]] = []
    for group_rows in grouped_candidates.values():
        group_rows.sort(
            key=lambda row: (
                -int(row.get("_review_priority") or 0),
                -float(row.get("outreach_fit_score") or 0.0),
                -float(row.get("record_confidence") or 0.0),
                str(row.get("practice_name") or row.get("practice_name_snapshot") or ""),
            )
        )
        primary_row = group_rows[0]
        primary_bundle = _bundle(con, str(primary_row.get("record_id") or ""))
        source_briefs = _source_briefs(con, _evidence_links(group_rows, primary_bundle))
        dossier = _dossier_bundle(rows=group_rows, reviews=[], primary_bundle=primary_bundle, source_briefs=source_briefs)
        dossier_id = f"{str(primary_row.get('practice_id') or primary_row.get('record_id') or '').strip()}-{_safe_slug(str(dossier['practice_name']))}"
        dossier_dir = dossiers_dir / dossier_id
        dossier_dir.mkdir(parents=True, exist_ok=True)
        contact_profiles_dir = dossier_dir / "profiles"
        contact_profiles_dir.mkdir(parents=True, exist_ok=True)
        for contact in list(dossier.get("contacts") or []):
            profile_name = _safe_slug(str(contact.get("name") or "contact")) + ".md"
            profile_path = contact_profiles_dir / profile_name
            contact["profile_file"] = f"profiles/{profile_name}"
            profile_path.write_text(_contact_profile_markdown(dossier, contact), encoding="utf-8")
        dossier_markdown = _lead_dossier_markdown(dossier)
        dossier_md_path = dossier_dir / "lead_intelligence.md"
        dossier_pdf_path = dossier_dir / "lead_intelligence.pdf"
        dossier_json_path = dossier_dir / "lead_intelligence.json"
        dossier_md_path.write_text(dossier_markdown, encoding="utf-8")
        _write_pdf(dossier_markdown, dossier_pdf_path)
        dossier_json_path.write_text(json.dumps(dossier, indent=2, default=str), encoding="utf-8")
        dossier_rows.append(
            {
                "dossier_id": dossier_id,
                "practice_name": dossier["practice_name"],
                "city": dossier["city"],
                "state": dossier["state"],
                "metro": dossier["metro"],
                "qa_state": dossier["qa_state"],
                "fit_score": dossier["fit_score"],
                "confidence_score": dossier["confidence_score"],
                "budget_band": dossier["budget_band"],
                "service_focus": dossier["service_focus"],
                "provider_count": dossier["provider_count"],
                "location_count": dossier["location_count"],
                "primary_contact": (list(dossier.get("contacts") or [{}])[0]).get("name", ""),
                "best_channel": (list(dossier.get("contacts") or [{}])[0]).get("best_channel", ""),
                "website": dossier["website"],
                "intake_url": dossier["intake_url"],
                "phone": dossier["phone"],
                "dossier_markdown": str(dossier_md_path),
                "dossier_pdf": str(dossier_pdf_path),
                "dossier_json": str(dossier_json_path),
                "profiles_dir": str(contact_profiles_dir),
                "source_urls": list(dossier.get("evidence_links") or []),
            }
        )

    dossier_fieldnames = [
        "dossier_id",
        "practice_name",
        "city",
        "state",
        "metro",
        "qa_state",
        "fit_score",
        "confidence_score",
        "budget_band",
        "service_focus",
        "provider_count",
        "location_count",
        "primary_contact",
        "best_channel",
        "website",
        "intake_url",
        "phone",
        "dossier_markdown",
        "dossier_pdf",
        "dossier_json",
        "profiles_dir",
        "source_urls",
    ]
    with dossiers_csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=dossier_fieldnames)
        writer.writeheader()
        for row in dossier_rows:
            flattened = dict(row)
            flattened["source_urls"] = json.dumps(flattened["source_urls"])
            writer.writerow(flattened)
    dossiers_json_path.write_text(json.dumps(dossier_rows, indent=2, default=str), encoding="utf-8")

    reviews_by_source: dict[str, list[dict[str, object]]] = defaultdict(list)
    for review_row in review_rows:
        source_url = str(review_row.get("source_url") or "").strip()
        if source_url:
            reviews_by_source[source_url].append(review_row)

    internal_review_rows: list[dict[str, object]] = []
    for group_rows in _internal_review_groups(con, review_rows=review_rows, limit=limit).values():
        group_rows.sort(
            key=lambda row: (
                -int(row.get("_review_priority") or 0),
                -float(row.get("outreach_fit_score") or 0.0),
                -float(row.get("record_confidence") or 0.0),
                str(row.get("practice_name") or row.get("practice_name_snapshot") or ""),
            )
        )
        group_reviews: list[dict[str, object]] = []
        for row in group_rows:
            source_url = _normalized_source_url(row.get("source_urls_json"), row.get("source_url"))
            group_reviews.extend(reviews_by_source.get(source_url, []))
        deduped_reviews: dict[str, dict[str, object]] = {}
        for review in group_reviews:
            review_id = str(review.get("review_id") or "")
            deduped_reviews[review_id or json.dumps(review, sort_keys=True, default=str)] = review
        group_reviews = list(deduped_reviews.values())
        if not group_reviews:
            continue
        primary_row = group_rows[0]
        primary_bundle = _review_only_bundle(primary_row)
        source_briefs = _source_briefs(con, _evidence_links(group_rows, primary_bundle))
        summary = _dossier_bundle(rows=group_rows, reviews=group_reviews, primary_bundle=primary_bundle, source_briefs=source_briefs)
        summary_id = f"{str(primary_row.get('practice_id') or primary_row.get('record_id') or '').strip()}-{_safe_slug(str(summary['practice_name']))}"
        summary_dir = internal_review_dir / summary_id
        summary_dir.mkdir(parents=True, exist_ok=True)
        summary_markdown = _internal_review_summary_markdown(summary)
        summary_md_path = summary_dir / "internal_review_summary.md"
        summary_pdf_path = summary_dir / "internal_review_summary.pdf"
        summary_json_path = summary_dir / "internal_review_summary.json"
        summary_md_path.write_text(summary_markdown, encoding="utf-8")
        _write_pdf(summary_markdown, summary_pdf_path)
        summary_json_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        internal_review_rows.append(
            {
                "account_id": summary_id,
                "practice_name": summary["practice_name"],
                "city": summary["city"],
                "state": summary["state"],
                "metro": summary["metro"],
                "qa_state": summary["qa_state"],
                "confidence_score": summary["confidence_score"],
                "service_focus": summary["service_focus"],
                "website": summary["website"],
                "intake_url": summary["intake_url"],
                "phone": summary["phone"],
                "summary_markdown": str(summary_md_path),
                "summary_pdf": str(summary_pdf_path),
                "summary_json": str(summary_json_path),
                "source_urls": list(summary.get("evidence_links") or []),
            }
        )

    internal_review_fieldnames = [
        "account_id",
        "practice_name",
        "city",
        "state",
        "metro",
        "qa_state",
        "confidence_score",
        "service_focus",
        "website",
        "intake_url",
        "phone",
        "summary_markdown",
        "summary_pdf",
        "summary_json",
        "source_urls",
    ]
    with internal_review_csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=internal_review_fieldnames)
        writer.writeheader()
        for row in internal_review_rows:
            flattened = dict(row)
            flattened["source_urls"] = json.dumps(flattened["source_urls"])
            writer.writerow(flattened)
    internal_review_json_path.write_text(json.dumps(internal_review_rows, indent=2, default=str), encoding="utf-8")

    return {
        "records_csv": str(records_path),
        "records_json": str(json_path),
        "review_queue_csv": str(review_path),
        "sales_report_csv": str(sales_path),
        "dossiers_csv": str(dossiers_csv_path),
        "dossiers_json": str(dossiers_json_path),
        "internal_review_csv": str(internal_review_csv_path),
        "internal_review_json": str(internal_review_json_path),
        "profiles_dir": str(profiles_dir),
        "evidence_dir": str(evidence_dir),
        "outreach_dir": str(outreach_dir),
        "dossiers_dir": str(dossiers_dir),
        "internal_review_dir": str(internal_review_dir),
        "record_count": len(export_rows),
        "sales_count": len(sales_rows),
        "review_count": len(review_rows),
        "dossier_count": len(dossier_rows),
        "internal_review_count": len(internal_review_rows),
    }
