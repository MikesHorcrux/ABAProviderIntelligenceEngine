from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from pipeline.fetch_backends.common import FetchResult
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.parse import extract_links, extract_title
from pipeline.utils import PHONE_RE, extract_snippet, normalize_text, strip_html


NPI_RE = re.compile(r"\bNPI(?:\s*(?:Number|#|:))?\s*(\d{10})\b", re.I)
FAX_RE = re.compile(r"\bFax[:\s]+((?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", re.I)
PHONE_CAPTURE_RE = PHONE_RE
PROVIDER_RE = re.compile(
    r"\b(?:Dr\.\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,3})\s*,\s*(MD|DO|PsyD|PhD|NP|APN|PA|LCSW)\b"
)
CITY_STATE_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,\s*(NJ)\b")
LICENSE_ACTIVE_RE = re.compile(r"\b(active|inactive|suspended|revoked)\b", re.I)
INSURANCE_RE = re.compile(r"\b(insurance|accept(?:s|ing)?\s+insurance|in-network)\b", re.I)
WAITLIST_RE = re.compile(r"\b(waitlist|wait list|currently scheduling|next available)\b", re.I)
REFERRAL_RE = re.compile(r"\b(referral|required referral|physician referral|intake form)\b", re.I)
TELEHEALTH_RE = re.compile(r"\b(telehealth|virtual visits?|video visits?|remote appointments?)\b", re.I)
CHILD_RE = re.compile(r"\b(children|child|pediatric|adolescent|teen)\b", re.I)
ADULT_RE = re.compile(r"\b(adult|adults)\b", re.I)
ASD_EXPLICIT_RE = re.compile(
    r"\b(autism|ASD).{0,40}\b(diagnostic evaluation|diagnostic evaluations|assessment|assessments|testing|testing services|evaluation|evaluations)\b",
    re.I,
)
ADHD_EXPLICIT_RE = re.compile(
    r"\b(ADHD).{0,40}\b(diagnostic evaluation|diagnostic evaluations|assessment|assessments|testing|evaluation|evaluations)\b",
    re.I,
)
ASD_AMBIGUOUS_RE = re.compile(r"\b(autism|ASD)\b", re.I)
ADHD_AMBIGUOUS_RE = re.compile(r"\bADHD\b", re.I)
PRACTICE_NAME_RE = re.compile(r"\b(?:Center|Clinic|Associates|Psychology|Behavioral|Pediatrics|Hospital)\b", re.I)


@dataclass(frozen=True)
class EvidenceItem:
    field: str
    value: str
    quote: str
    source_url: str


@dataclass(frozen=True)
class ExtractedRecord:
    provider_name: str
    credentials: str
    npi: str
    practice_name: str
    intake_url: str
    phone: str
    fax: str
    address_1: str
    city: str
    state: str
    zip_code: str
    metro: str
    license_state: str
    license_type: str
    license_status: str
    diagnoses_asd: str
    diagnoses_adhd: str
    age_groups: list[str]
    telehealth: str
    insurance_notes: str
    waitlist_notes: str
    referral_requirements: str
    evidence: list[EvidenceItem]
    source_tier: str
    source_type: str
    extraction_profile: str
    source_url: str


def _match_evidence(pattern: re.Pattern[str], text: str, field: str, value: str, source_url: str) -> EvidenceItem | None:
    match = pattern.search(text)
    if not match:
        return None
    return EvidenceItem(field=field, value=value, quote=extract_snippet(text, match.start(), match.end()), source_url=source_url)


def _first_phone(text: str) -> str:
    match = PHONE_CAPTURE_RE.search(text)
    return normalize_text(match.group(0)) if match else ""


def _first_fax(text: str) -> str:
    match = FAX_RE.search(text)
    return normalize_text(match.group(1)) if match else ""


def _first_city_state(text: str) -> tuple[str, str]:
    match = CITY_STATE_RE.search(text)
    if not match:
        return "", ""
    return normalize_text(match.group(1)), normalize_text(match.group(2))


def _age_groups(text: str) -> list[str]:
    groups: list[str] = []
    if CHILD_RE.search(text):
        groups.extend(["child", "adolescent"])
    if ADULT_RE.search(text):
        groups.append("adult")
    if not groups:
        return ["unknown"]
    return list(dict.fromkeys(groups))


def _license_type(credentials: str, source_type: str) -> str:
    lowered = credentials.lower()
    if "md" in lowered or "do" in lowered:
        return "physician"
    if "psyd" in lowered or "phd" in lowered:
        return "psychologist"
    if "apn" in lowered or "np" in lowered:
        return "advanced_practice_nurse"
    if lowered.strip() == "pa":
        return "physician_assistant"
    if "lcsw" in lowered:
        return "clinical_social_worker"
    if source_type == "licensing_board":
        return "board_listing"
    return "unknown"


def _practice_name(text: str, html: str, fallback: str) -> str:
    title = normalize_text(extract_title(html))
    if title and PRACTICE_NAME_RE.search(title):
        return title
    if fallback:
        return fallback
    lines = [normalize_text(chunk) for chunk in text.split("  ") if normalize_text(chunk)]
    for line in lines[:6]:
        if PRACTICE_NAME_RE.search(line):
            return line
    return title or "Unknown Practice"


def _clean_provider_name(raw: str) -> str:
    tokens = normalize_text(raw).replace("Dr. ", "").split()
    noise = {"center", "clinic", "psychology", "behavioral", "hospital", "associates", "group", "state", "garden"}
    while tokens and tokens[0].lower().strip(".") in noise:
        tokens.pop(0)
    if len(tokens) > 3:
        tokens = tokens[-3:]
    return " ".join(tokens)


def _intake_url(base: str, html: str) -> str:
    for link in extract_links(base, html):
        lowered = link.lower()
        if any(token in lowered for token in ("intake", "referral", "appointment", "new-patient", "contact")):
            if link.startswith("http"):
                return link
            return f"{base.rstrip('/')}/{link.lstrip('/')}"
    return ""


def _line_snippet(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(text)
    return extract_snippet(text, match.start(), match.end()) if match else ""


def extract_records(
    item: FetchResult,
    seed: DiscoverySeed,
    metro_lookup: dict[str, str],
) -> list[ExtractedRecord]:
    text = strip_html(item.content or "")
    if not text:
        return []

    practice_name = _practice_name(text, item.content, seed.name)
    city, state = _first_city_state(text)
    state = state or seed.state or "NJ"
    metro = metro_lookup.get(city.lower(), seed.market or "unknown") if city else (seed.market or "unknown")
    phone = _first_phone(text)
    fax = _first_fax(text)
    intake_url = _intake_url(item.target_url, item.content or "")
    telehealth = "yes" if TELEHEALTH_RE.search(text) else "unknown"
    insurance_notes = _line_snippet(INSURANCE_RE, text)
    waitlist_notes = _line_snippet(WAITLIST_RE, text)
    referral_requirements = _line_snippet(REFERRAL_RE, text)
    age_groups = _age_groups(text)
    license_status_match = LICENSE_ACTIVE_RE.search(text)
    license_status = license_status_match.group(1).lower() if license_status_match else "unknown"
    npi_match = NPI_RE.search(text)
    npi = npi_match.group(1) if npi_match else ""

    asd_value = "unclear"
    asd_evidence = _match_evidence(ASD_EXPLICIT_RE, text, "diagnoses_asd", "yes", item.target_url)
    if asd_evidence:
        asd_value = "yes"
    elif ASD_AMBIGUOUS_RE.search(text):
        asd_value = "unclear"

    adhd_value = "unclear"
    adhd_evidence = _match_evidence(ADHD_EXPLICIT_RE, text, "diagnoses_adhd", "yes", item.target_url)
    if adhd_evidence:
        adhd_value = "yes"
    elif ADHD_AMBIGUOUS_RE.search(text):
        adhd_value = "unclear"

    provider_matches = list(PROVIDER_RE.finditer(text))
    if not provider_matches:
        evidence: list[EvidenceItem] = []
        if asd_evidence:
            evidence.append(asd_evidence)
        if adhd_evidence:
            evidence.append(adhd_evidence)
        if license_status != "unknown":
            evidence.append(
                EvidenceItem(
                    field="license_status",
                    value=license_status,
                    quote=_line_snippet(LICENSE_ACTIVE_RE, text),
                    source_url=item.target_url,
                )
            )
        return [
            ExtractedRecord(
                provider_name="",
                credentials="",
                npi=npi,
                practice_name=practice_name,
                intake_url=intake_url,
                phone=phone,
                fax=fax,
                address_1="",
                city=city,
                state=state,
                zip_code="",
                metro=metro,
                license_state=state,
                license_type="unknown",
                license_status=license_status,
                diagnoses_asd=asd_value,
                diagnoses_adhd=adhd_value,
                age_groups=age_groups,
                telehealth=telehealth,
                insurance_notes=insurance_notes,
                waitlist_notes=waitlist_notes,
                referral_requirements=referral_requirements,
                evidence=evidence,
                source_tier=seed.tier,
                source_type=seed.source_type,
                extraction_profile=seed.extraction_profile,
                source_url=item.target_url,
            )
        ]

    records: list[ExtractedRecord] = []
    for match in provider_matches:
        provider_name = _clean_provider_name(match.group(1))
        credentials = normalize_text(match.group(2))
        evidence = [
            EvidenceItem(
                field="provider_name",
                value=provider_name,
                quote=extract_snippet(text, match.start(), match.end()),
                source_url=item.target_url,
            ),
            EvidenceItem(
                field="credentials",
                value=credentials,
                quote=extract_snippet(text, match.start(), match.end()),
                source_url=item.target_url,
            ),
        ]
        if asd_evidence:
            evidence.append(asd_evidence)
        if adhd_evidence:
            evidence.append(adhd_evidence)
        if license_status != "unknown":
            evidence.append(
                EvidenceItem(
                    field="license_status",
                    value=license_status,
                    quote=_line_snippet(LICENSE_ACTIVE_RE, text),
                    source_url=item.target_url,
                )
            )
        if npi:
            evidence.append(
                EvidenceItem(
                    field="npi",
                    value=npi,
                    quote=_line_snippet(NPI_RE, text),
                    source_url=item.target_url,
                )
            )
        records.append(
            ExtractedRecord(
                provider_name=provider_name,
                credentials=credentials,
                npi=npi,
                practice_name=practice_name,
                intake_url=intake_url,
                phone=phone,
                fax=fax,
                address_1="",
                city=city,
                state=state,
                zip_code="",
                metro=metro,
                license_state=state,
                license_type=_license_type(credentials, seed.source_type),
                license_status=license_status,
                diagnoses_asd=asd_value,
                diagnoses_adhd=adhd_value,
                age_groups=age_groups,
                telehealth=telehealth,
                insurance_notes=insurance_notes,
                waitlist_notes=waitlist_notes,
                referral_requirements=referral_requirements,
                evidence=evidence,
                source_tier=seed.tier,
                source_type=seed.source_type,
                extraction_profile=seed.extraction_profile,
                source_url=item.target_url,
            )
        )
    return records


def evidence_to_json(evidence: list[EvidenceItem]) -> str:
    return json.dumps([item.__dict__ for item in evidence], sort_keys=True)
