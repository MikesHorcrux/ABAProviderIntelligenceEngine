from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pipeline.utils import normalize_domain


ALLOWED_POLICY_MODES = {"http_then_browser_on_block", "browser", "http_only"}


@dataclass(frozen=True)
class DomainPolicy:
    mode: str = "http_then_browser_on_block"
    wait_for_selector: str = ""
    extra_block_patterns: tuple[str, ...] = ()
    max_pages_per_domain: int | None = None
    max_depth: int | None = None
    browser_on_block: bool = True


@dataclass(frozen=True)
class DomainPolicySet:
    default: DomainPolicy
    domains: dict[str, DomainPolicy]
    source_path: Path

    def resolve(self, url_or_domain: str) -> DomainPolicy:
        domain = normalize_domain(url_or_domain)
        if not domain:
            return self.default
        return self.domains.get(domain, self.default)


def _coerce_policy(payload: object, fallback: DomainPolicy) -> DomainPolicy:
    if not isinstance(payload, dict):
        return fallback

    mode = str(payload.get("mode", fallback.mode)).strip() or fallback.mode
    if mode not in ALLOWED_POLICY_MODES:
        mode = fallback.mode

    wait_for_selector = str(payload.get("waitForSelector", fallback.wait_for_selector)).strip()
    extra_patterns = tuple(
        str(item).strip()
        for item in payload.get("extraBlockPatterns", fallback.extra_block_patterns)
        if str(item).strip()
    )
    max_pages = payload.get("maxPagesPerDomain", fallback.max_pages_per_domain)
    max_depth = payload.get("maxDepth", fallback.max_depth)

    return DomainPolicy(
        mode=mode,
        wait_for_selector=wait_for_selector,
        extra_block_patterns=extra_patterns,
        max_pages_per_domain=int(max_pages) if max_pages not in (None, "") else fallback.max_pages_per_domain,
        max_depth=int(max_depth) if max_depth not in (None, "") else fallback.max_depth,
        browser_on_block=bool(payload.get("browserOnBlock", fallback.browser_on_block)),
    )


def load_domain_policies(path: str | Path) -> DomainPolicySet:
    resolved = Path(path).resolve()
    default = DomainPolicy()

    if not resolved.exists():
        return DomainPolicySet(default=default, domains={}, source_path=resolved)

    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception:
        return DomainPolicySet(default=default, domains={}, source_path=resolved)

    default_policy = _coerce_policy(payload.get("default"), default) if isinstance(payload, dict) else default
    raw_domains = payload.get("domains") if isinstance(payload, dict) else {}
    domains: dict[str, DomainPolicy] = {}
    if isinstance(raw_domains, dict):
        for raw_domain, raw_policy in raw_domains.items():
            normalized = normalize_domain(raw_domain)
            if not normalized or "*" in str(raw_domain):
                continue
            domains[normalized] = _coerce_policy(raw_policy, default_policy)

    return DomainPolicySet(default=default_policy, domains=domains, source_path=resolved)
