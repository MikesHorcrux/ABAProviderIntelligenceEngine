from __future__ import annotations

import json
import re
from dataclasses import dataclass

from pipeline.fetch_backends.common import FetchResult, detect_block_signal
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.parse import extract_links, extract_title
from pipeline.utils import PHONE_RE, extract_snippet, normalize_text, resolve_link, strip_html


CREDENTIAL_TOKEN = r"(?:MD|DO|PsyD|PhD|NP|APN|PA|LCSW|EdD|BCBA-D|BCBA)"
NPI_RE = re.compile(r"\bNPI(?:\s*(?:Number|#|:))?\s*(\d{10})\b", re.I)
FAX_RE = re.compile(r"\bFax[:\s]+((?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", re.I)
PHONE_CAPTURE_RE = PHONE_RE
PROVIDER_WITH_CREDENTIAL_RE = re.compile(
    rf"\b(?:Dr\.\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){{1,3}})\s*,\s*({CREDENTIAL_TOKEN}(?:\s*,\s*{CREDENTIAL_TOKEN})*)\b"
)
ROLE_PROVIDER_RE = re.compile(
    rf"\b(?:Directed by|Led by|Director,|Director:|Medical Director:?|Clinical Director:?|Program Director:?|Psychiatrist:?|Psychologist:?|Physician:?|Provider:)\s+(?:Dr\.\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){{1,3}})(?:\s*,\s*({CREDENTIAL_TOKEN}(?:\s*,\s*{CREDENTIAL_TOKEN})*))?\b",
    re.I,
)
DR_PROVIDER_RE = re.compile(r"\bDr\.\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,3})\b")
CITY_STATE_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,\s*(NJ)\b")
LICENSE_STATUS_PATTERNS = (
    re.compile(r"\blicense status[:\s]+(active|inactive|suspended|revoked)\b", re.I),
    re.compile(r"\bstatus[:\s]+(active|inactive|suspended|revoked)\b", re.I),
    re.compile(r"\b(active|inactive|suspended|revoked)\s+license\b", re.I),
)
BOARD_PROVIDER_PATTERNS = (
    re.compile(
        r"\b(?:licensee name|provider name|practitioner name|physician name|psychologist name|licensee)\s*[:\-]\s*(?:dr\.\s+)?([A-Z][a-z.'-]+(?:\s+[A-Z][a-z.'-]+){1,3}?)(?=\s+(?:profession|license|status|state)\b|$)",
        re.I,
    ),
    re.compile(r"\bverification\s+for\s+(?:dr\.\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,3})\b", re.I),
)
BOARD_LICENSE_TYPE_PATTERNS = (
    re.compile(r"\blicense type[:\s]+([A-Za-z][A-Za-z /-]{2,60})\b", re.I),
    re.compile(r"\bprofession[:\s]+([A-Za-z][A-Za-z /-]{2,60})\b", re.I),
)
INSURANCE_RE = re.compile(r"\b(insurance|accept(?:s|ing)?\s+insurance|in-network)\b", re.I)
WAITLIST_RE = re.compile(r"\b(waitlist|wait list|currently scheduling|next available)\b", re.I)
REFERRAL_RE = re.compile(r"\b(referral|required referral|physician referral|intake form)\b", re.I)
TELEHEALTH_RE = re.compile(r"\b(telehealth|virtual visits?|video visits?|remote appointments?)\b", re.I)
CHILD_RE = re.compile(r"\b(children|child|pediatric|adolescent|teen)\b", re.I)
ADULT_RE = re.compile(r"\b(adult|adults)\b", re.I)
PAGE_RELEVANCE_RE = re.compile(r"\b(autism|asd|adhd|developmental|diagnostic|evaluation|assessment|neurodevelopment)\b", re.I)
ASD_EXPLICIT_PATTERNS = (
    re.compile(
        r"\b(?:autism|asd|autism spectrum disorder).{0,80}\b(?:diagnostic evaluations?|diagnostic testing|assessment(?:s)?|testing(?: services?)?|evaluations?)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:diagnostic evaluations?|diagnostic testing|assessment(?:s)?|testing(?: services?)?|evaluations?).{0,80}\b(?:autism|asd|autism spectrum disorder)\b",
        re.I,
    ),
    re.compile(r"\bassessment\s*,\s*diagnosis\s+and\s+management\s+of.{0,60}\b(?:autism|asd|autism spectrum disorder)\b", re.I),
)
ADHD_EXPLICIT_PATTERNS = (
    re.compile(
        r"\b(?:adhd|attention deficit hyperactivity disorder).{0,80}\b(?:diagnostic evaluations?|diagnostic testing|assessment(?:s)?|testing(?: services?)?|evaluations?)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:diagnostic evaluations?|diagnostic testing|assessment(?:s)?|testing(?: services?)?|evaluations?).{0,80}\b(?:adhd|attention deficit hyperactivity disorder)\b",
        re.I,
    ),
    re.compile(r"\bassessment\s*,\s*diagnosis\s+and\s+management\s+of.{0,60}\b(?:adhd|attention deficit hyperactivity disorder)\b", re.I),
)
ASD_AMBIGUOUS_RE = re.compile(r"\b(autism|ASD|autism spectrum disorder)\b", re.I)
ADHD_AMBIGUOUS_RE = re.compile(r"\bADHD\b", re.I)
PRACTICE_NAME_RE = re.compile(r"\b(?:Center|Clinic|Associates|Psychology|Behavioral|Pediatrics|Hospital|Health|Services|Evaluations)\b", re.I)
CONTENT_SECTION_RES = (
    re.compile(r"<main\b[^>]*>([\s\S]*?)</main>", re.I),
    re.compile(r"<article\b[^>]*>([\s\S]*?)</article>", re.I),
    re.compile(r"<div\b[^>]*id=[\"']main-content[\"'][^>]*>([\s\S]*?)</div>", re.I),
    re.compile(r"<section\b[^>]*data-content=[\"']true[\"'][^>]*>([\s\S]*?)</section>", re.I),
)
BLOCK_MARKERS = (
    "_Incapsula_Resource",
    "Request unsuccessful",
    "Incapsula incident ID",
    "access to this page has been denied",
    "pardon our interruption",
)


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


@dataclass(frozen=True)
class ProviderCandidate:
    provider_name: str
    credentials: str
    start: int
    end: int


def _match_evidence(pattern: re.Pattern[str], text: str, field: str, value: str, source_url: str) -> EvidenceItem | None:
    match = pattern.search(text)
    if not match:
        return None
    return EvidenceItem(field=field, value=value, quote=extract_snippet(text, match.start(), match.end()), source_url=source_url)


def _match_first_evidence(
    patterns: tuple[re.Pattern[str], ...],
    text: str,
    field: str,
    value: str,
    source_url: str,
) -> EvidenceItem | None:
    for pattern in patterns:
        evidence = _match_evidence(pattern, text, field, value, source_url)
        if evidence:
            return evidence
    return None


def _semantic_html(html: str) -> str:
    for pattern in CONTENT_SECTION_RES:
        match = pattern.search(html or "")
        if not match:
            continue
        candidate = match.group(1)
        if len(strip_html(candidate)) >= 80:
            return candidate
    return html


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
    tail_noise = {
        "director",
        "associate",
        "assistant",
        "professor",
        "teaching",
        "clinical",
        "training",
        "research",
        "program",
        "school",
        "initiatives",
        "department",
        "and",
        "at",
        "dr",
    }
    while tokens and tokens[0].lower().strip(".") in noise:
        tokens.pop(0)
    for index, token in enumerate(tokens):
        if index >= 2 and token.lower().strip(".,") in tail_noise:
            tokens = tokens[:index]
            break
    if len(tokens) > 4:
        tokens = tokens[:4]
    return " ".join(tokens)


def _clean_credentials(raw: str) -> str:
    cleaned = normalize_text(raw)
    if not cleaned:
        return ""
    parts = [normalize_text(part) for part in cleaned.split(",") if normalize_text(part)]
    return ", ".join(parts[:3])


def _intake_url(base: str, html: str) -> str:
    for link in extract_links(base, html):
        lowered = link.lower()
        if any(token in lowered for token in ("intake", "referral", "appointment", "new-patient", "newpatient", "contact")):
            return resolve_link(base, link)
    return ""


def _line_snippet(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(text)
    return extract_snippet(text, match.start(), match.end()) if match else ""


def _match_first_group(patterns: tuple[re.Pattern[str], ...], text: str) -> tuple[str, str]:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return normalize_text(match.group(1)).lower(), extract_snippet(text, match.start(), match.end())
    return "", ""


def _diagnosis_signal(
    *,
    text: str,
    field: str,
    source_url: str,
    explicit_patterns: tuple[re.Pattern[str], ...],
    ambiguous_pattern: re.Pattern[str],
) -> tuple[str, EvidenceItem | None]:
    explicit = _match_first_evidence(explicit_patterns, text, field, "yes", source_url)
    if explicit:
        return "yes", explicit
    ambiguous = ambiguous_pattern.search(text)
    if ambiguous:
        return (
            "unclear",
            EvidenceItem(
                field=field,
                value="unclear",
                quote=extract_snippet(text, ambiguous.start(), ambiguous.end()),
                source_url=source_url,
            ),
        )
    return "unclear", None


def _provider_candidates(text: str, seed: DiscoverySeed) -> list[ProviderCandidate]:
    candidates_by_name: dict[str, ProviderCandidate] = {}
    patterns: list[re.Pattern[str]] = []
    if seed.source_type != "licensing_board":
        patterns.extend([PROVIDER_WITH_CREDENTIAL_RE, ROLE_PROVIDER_RE])
    if seed.source_type in {"hospital_directory", "university_directory"}:
        patterns.append(DR_PROVIDER_RE)

    for pattern in patterns:
        for match in pattern.finditer(text):
            provider_name = _clean_provider_name(match.group(1))
            if not provider_name:
                continue
            credentials = _clean_credentials(match.group(2)) if match.lastindex and match.lastindex > 1 else ""
            key = provider_name.lower()
            candidate = ProviderCandidate(provider_name=provider_name, credentials=credentials, start=match.start(), end=match.end())
            existing = candidates_by_name.get(key)
            if existing is None or (candidate.credentials and not existing.credentials):
                candidates_by_name[key] = candidate

    if seed.source_type == "licensing_board":
        for pattern in BOARD_PROVIDER_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue
            provider_name = _clean_provider_name(match.group(1))
            if not provider_name:
                continue
            key = provider_name.lower()
            if key not in candidates_by_name:
                candidates_by_name[key] = ProviderCandidate(provider_name=provider_name, credentials="", start=match.start(), end=match.end())
            break
        if not candidates_by_name:
            head = text[:600]
            for pattern in (PROVIDER_WITH_CREDENTIAL_RE, DR_PROVIDER_RE):
                match = pattern.search(head)
                if not match:
                    continue
                provider_name = _clean_provider_name(match.group(1))
                if not provider_name:
                    continue
                credentials = _clean_credentials(match.group(2)) if match.lastindex and match.lastindex > 1 else ""
                candidates_by_name[provider_name.lower()] = ProviderCandidate(
                    provider_name=provider_name,
                    credentials=credentials,
                    start=match.start(),
                    end=match.end(),
                )
                break

    return sorted(candidates_by_name.values(), key=lambda item: (item.start, item.provider_name.lower()))


def _board_license_type(seed: DiscoverySeed, text: str) -> str:
    detected, _ = _match_first_group(BOARD_LICENSE_TYPE_PATTERNS, text)
    haystack = f"{seed.name} {detected}".lower()
    if "psych" in haystack:
        return "psychologist"
    if "nurs" in haystack or "apn" in haystack or "nurse practitioner" in haystack:
        return "advanced_practice_nurse"
    if "assistant" in haystack or re.search(r"\bpa\b", haystack):
        return "physician_assistant"
    if "physician" in haystack or "medical" in haystack or "bme" in haystack:
        return "physician"
    return "unknown"


def _is_relevant_page(
    *,
    seed: DiscoverySeed,
    page_title: str,
    source_url: str,
    asd_value: str,
    adhd_value: str,
) -> bool:
    if seed.source_type == "licensing_board":
        return True
    if asd_value == "yes" or adhd_value == "yes":
        return True
    haystack = f"{page_title} {source_url}"
    return bool(PAGE_RELEVANCE_RE.search(haystack))


def extract_records(
    item: FetchResult,
    seed: DiscoverySeed,
    metro_lookup: dict[str, str],
) -> list[ExtractedRecord]:
    html = item.content or ""
    if not html:
        return []

    if detect_block_signal(status_code=item.status_code, content=html, extra_patterns=BLOCK_MARKERS).triggered:
        return []

    semantic_html = _semantic_html(html)
    semantic_text = strip_html(semantic_html)
    full_text = strip_html(html)
    text = semantic_text or full_text
    if not text:
        return []

    page_title = normalize_text(extract_title(html))
    practice_name = _practice_name(text, html, seed.name)
    city, state = _first_city_state(full_text)
    state = state or seed.state or "NJ"
    metro = metro_lookup.get(city.lower(), seed.market or "unknown") if city else (seed.market or "unknown")
    phone = _first_phone(full_text)
    fax = _first_fax(full_text)
    intake_url = _intake_url(item.target_url, html)
    telehealth = "yes" if TELEHEALTH_RE.search(full_text) else "unknown"
    insurance_notes = _line_snippet(INSURANCE_RE, full_text)
    waitlist_notes = _line_snippet(WAITLIST_RE, full_text)
    referral_requirements = _line_snippet(REFERRAL_RE, full_text)
    age_groups = _age_groups(text)
    license_status, license_status_quote = _match_first_group(LICENSE_STATUS_PATTERNS, text)
    license_status = license_status or "unknown"
    npi_match = NPI_RE.search(full_text)
    npi = npi_match.group(1) if npi_match else ""
    license_type = _board_license_type(seed, full_text) if seed.source_type == "licensing_board" else ""

    asd_value, asd_evidence = _diagnosis_signal(
        text=text,
        field="diagnoses_asd",
        source_url=item.target_url,
        explicit_patterns=ASD_EXPLICIT_PATTERNS,
        ambiguous_pattern=ASD_AMBIGUOUS_RE,
    )
    adhd_value, adhd_evidence = _diagnosis_signal(
        text=text,
        field="diagnoses_adhd",
        source_url=item.target_url,
        explicit_patterns=ADHD_EXPLICIT_PATTERNS,
        ambiguous_pattern=ADHD_AMBIGUOUS_RE,
    )
    if not _is_relevant_page(
        seed=seed,
        page_title=page_title,
        source_url=item.target_url,
        asd_value=asd_value,
        adhd_value=adhd_value,
    ):
        return []

    provider_matches = _provider_candidates(text, seed)
    if not provider_matches:
        evidence: list[EvidenceItem] = []
        reviewable_practice_signal = False
        if asd_evidence:
            evidence.append(asd_evidence)
            reviewable_practice_signal = reviewable_practice_signal or asd_evidence.value == "yes"
        if adhd_evidence:
            evidence.append(adhd_evidence)
            reviewable_practice_signal = reviewable_practice_signal or adhd_evidence.value == "yes"
        if license_status != "unknown":
            evidence.append(
                EvidenceItem(
                    field="license_status",
                    value=license_status,
                    quote=license_status_quote,
                    source_url=item.target_url,
                )
            )
            reviewable_practice_signal = True
        if npi:
            evidence.append(
                EvidenceItem(
                    field="npi",
                    value=npi,
                    quote=_line_snippet(NPI_RE, full_text),
                    source_url=item.target_url,
                )
            )
        if not reviewable_practice_signal:
            return []
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
                license_type=license_type or "unknown",
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
        provider_name = match.provider_name
        credentials = match.credentials
        evidence = [
            EvidenceItem(
                field="provider_name",
                value=provider_name,
                quote=extract_snippet(text, match.start, match.end),
                source_url=item.target_url,
            ),
        ]
        if credentials:
            evidence.append(
                EvidenceItem(
                    field="credentials",
                    value=credentials,
                    quote=extract_snippet(text, match.start, match.end),
                    source_url=item.target_url,
                )
            )
        if asd_evidence:
            evidence.append(asd_evidence)
        if adhd_evidence:
            evidence.append(adhd_evidence)
        if license_status != "unknown":
            evidence.append(
                EvidenceItem(
                    field="license_status",
                    value=license_status,
                    quote=license_status_quote,
                    source_url=item.target_url,
                )
            )
        if npi:
            evidence.append(
                EvidenceItem(
                    field="npi",
                    value=npi,
                    quote=_line_snippet(NPI_RE, full_text),
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
                license_type=license_type or _license_type(credentials, seed.source_type),
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
