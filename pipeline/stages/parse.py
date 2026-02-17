from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Iterable

from pipeline.utils import EMAIL_RE, PHONE_RE, CandidateSignal, extract_snippet, strip_html


EMAIL_CANDIDATE_RE = re.compile(EMAIL_RE.pattern, re.I)
PHONE_CANDIDATE_RE = re.compile(PHONE_RE.pattern)
HREF_RE = re.compile(r"href=[\"']([^\"'#]+)", re.I)
SRCSET_RE = re.compile(r'https?://[^\s"\'<>]+')

SCHEMA_ORG_RE = re.compile(r"\"@type\"\s*:\s*\"?LocalBusiness\"?.{0,4000}?\"?address\"\s*:\s*{", re.I | re.S)
MENU_PROVIDER_PATTERNS = {
    "dutchie": re.compile(r"dutchie", re.I),
    "weedmaps": re.compile(r"weedmaps", re.I),
    "jane": re.compile(r"\bJane\b", re.I),
    "greenbits": re.compile(r"greenbits", re.I),
    "flowhub": re.compile(r"flowhub", re.I),
}
ROLE_KW_RE = re.compile(r"\b(owner|gm|general manager|buyer|purchasing|inventory manager|chief operating officer|operations|director of operations)\b", re.I)
NAME_ROLE_RE = re.compile(r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,3})[\s\S]{0,80}\b(owner|gm|general manager|buyer|purchasing|inventory manager)\b", re.I)


@dataclass(frozen=True)
class ParsedPage:
    url: str
    html: str
    text: str
    emails: list[CandidateSignal]
    phones: list[CandidateSignal]
    contact_people: list[tuple[str, str, str]]
    social_urls: list[str]
    schema_local_business: dict
    menu_providers: list[str]
    links: list[str]


def extract_links(url: str, html: str) -> list[str]:
    return list({h.lower() for h in HREF_RE.findall(html) if h.strip()})


def _extract_contacts(text: str, page_url: str) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    for m in NAME_ROLE_RE.finditer(text):
        name = m.group(1).strip()
        role = m.group(2).strip().lower()
        if len(name.split()) < 2:
            continue
        out.append((name, role, extract_snippet(text, m.start(), m.end())))
    return out


def _extract_social_links(html: str) -> list[str]:
    out = set[str]()
    lowered = html.lower()
    for domain in ("instagram.com", "facebook.com", "x.com", "twitter.com", "tiktok.com", "youtube.com"):
        start = 0
        token = domain
        while True:
            idx = lowered.find(token, start)
            if idx < 0:
                break
            left = max(0, idx - 90)
            right = min(len(html), idx + 180)
            context = html[left:right]
            m = SRCSET_RE.search(context)
            if m and m.group(0).startswith("http"):
                out.add(m.group(0))
            start = idx + 1
    return sorted(out)


def _extract_schema_org_address(text: str) -> dict:
    if not SCHEMA_ORG_RE.search(text):
        return {}
    # cheap fallback: attempt to parse application/ld+json blocks if available
    for block in re.findall(r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", text, flags=re.I | re.S):
        try:
            payload = json.loads(block.strip())
            if isinstance(payload, dict) and payload.get("@type") in {"LocalBusiness", "Store", "CannabisStore"}:
                return payload
        except Exception:
            continue
    return {}


def parse_page(url: str, html: str) -> ParsedPage:
    text = strip_html(html or "")
    emails: list[CandidateSignal] = []
    phones: list[CandidateSignal] = []
    for m in EMAIL_CANDIDATE_RE.finditer(text):
        value = m.group(0).lower()
        if not value:
            continue
        emails.append(
            CandidateSignal(
                url=url,
                field_name="email",
                value=value,
                confidence=0.78,
                snippet=extract_snippet(text, m.start(), m.end()),
                source="first_party",
            )
        )

    for m in PHONE_CANDIDATE_RE.finditer(text):
        value = m.group(0)
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) < 10:
            continue
        phones.append(
            CandidateSignal(
                url=url,
                field_name="phone",
                value=value,
                confidence=0.7,
                snippet=extract_snippet(text, m.start(), m.end()),
                source="first_party",
            )
        )

    contacts = _extract_contacts(text, url)
    social_urls = _extract_social_links(html)
    schema_org = _extract_schema_org_address(text)
    links = extract_links(url, html)
    providers: list[str] = []
    for name, pat in MENU_PROVIDER_PATTERNS.items():
        if pat.search(text):
            providers.append(name)

    return ParsedPage(
        url=url,
        html=html,
        text=text,
        emails=emails,
        phones=phones,
        contact_people=[(name, role, snippet) for name, role, snippet in contacts],
        social_urls=social_urls,
        schema_local_business=schema_org,
        menu_providers=providers,
        links=[l for l in links],
    )


def dedupe_signals(values: Iterable[CandidateSignal]) -> list[CandidateSignal]:
    seen: set[tuple[str, str]] = set()
    out: list[CandidateSignal] = []
    for v in values:
        key = (v.field_name, v.value.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out
