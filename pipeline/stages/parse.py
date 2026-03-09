from __future__ import annotations

import re
from urllib.parse import urlparse

from pipeline.utils import strip_html


HREF_RE = re.compile(r"""href=["']([^"'#]+)""", re.I)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)


def extract_links(url: str, html: str) -> list[str]:
    del url
    return list({href.strip() for href in HREF_RE.findall(html or "") if href.strip()})


def extract_title(html: str) -> str:
    for regex in (TITLE_RE, H1_RE):
        match = regex.search(html or "")
        if match:
            return strip_html(match.group(1))
    return ""


def extract_domain(url: str) -> str:
    parsed = urlparse(url or "")
    return (parsed.netloc or "").lower()
