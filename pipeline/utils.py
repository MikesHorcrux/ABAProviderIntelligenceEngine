from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qsl


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
TRIM_WS_RE = re.compile(r"\s+")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(value: str | None) -> str:
    return TRIM_WS_RE.sub(" ", (value or "").strip()).strip()


def normalize_domain(url_or_domain: str) -> str:
    v = normalize_text(url_or_domain).lower()
    if not v:
        return ""
    if "://" not in v:
        v = f"https://{v}"
    p = urlparse(v)
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def normalize_url(raw: str) -> str:
    try:
        p = urlparse(normalize_text(raw))
        if not p.scheme:
            p = urlparse(f"https://{normalize_text(raw)}")
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = p.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=False)]
        return urlunparse((p.scheme.lower(), host, path, "", urlencode(sorted(query)), ""))
    except Exception:
        return normalize_text(raw).lower()


def same_domain(a: str, b: str) -> bool:
    return normalize_domain(a) == normalize_domain(b)


def make_pk(prefix: str, parts: list[str]) -> str:
    base = "|".join([normalize_text(p).lower() for p in parts])
    digest = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def extract_snippet(text: str, start: int, end: int, max_len: int = 180) -> str:
    if not text:
        return ""
    start = max(start - 40, 0)
    end = min(end + 40, len(text))
    snippet = text[start:end].strip().replace("\n", " ")
    return snippet[:max_len]


def is_valid_email(value: str) -> bool:
    return bool(EMAIL_RE.fullmatch((value or "").strip().lower()))


def is_valid_phone(value: str) -> bool:
    digits = re.sub(r"\D", "", value or "")
    return 10 <= len(digits) <= 15


def strip_html(text: str) -> str:
    if not text:
        return ""
    no_script = re.sub(r"<script[\\s\\S]*?</script>", " ", text, flags=re.I)
    no_style = re.sub(r"<style[\\s\\S]*?</style>", " ", no_script, flags=re.I)
    no_tags = re.sub(r"<[^>]+>", " ", no_style)
    return TRIM_WS_RE.sub(" ", no_tags).strip()


def resolve_link(base_url: str, raw: str) -> str:
    return normalize_url(urljoin(base_url, raw))


def parse_page_text(text: str) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    if not text:
        return out
    for match in EMAIL_RE.finditer(text):
        out.append((match.group(0), match.start(), match.end()))
    return out


@dataclass(frozen=True)
class CandidateSignal:
    url: str
    field_name: str
    value: str
    confidence: float
    snippet: str
    source: str = ""
