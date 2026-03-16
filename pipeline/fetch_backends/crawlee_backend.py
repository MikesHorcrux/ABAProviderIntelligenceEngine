from __future__ import annotations

import asyncio
import atexit
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urlparse

import sqlite3

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import HttpCrawler, PlaywrightCrawler
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee.storages import RequestQueue

from pipeline.config import CrawlConfig
from pipeline.fetch_backends.common import (
    BlockSignal,
    FetchResult,
    QueueItem,
    SeedRunRecorder,
    already_fetched_recently,
    detect_block_signal,
    first_positive_status_code,
    is_html_content_type,
    status_code_from_error_text,
)
from pipeline.fetch_backends.domain_policy import DomainPolicy, load_domain_policies
from pipeline.run_control import (
    append_intervention,
    domain_control_record,
    domain_runtime_record,
    ensure_run_control,
    load_run_control,
    save_run_control,
)
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.parse import extract_links
from pipeline.utils import normalize_domain, normalize_url, resolve_link, same_domain, utcnow_iso


try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover
    PlaywrightTimeoutError = TimeoutError


_CRAWLEE_LOOP: asyncio.AbstractEventLoop | None = None

AUTO_SUPPRESS_PREFIX_FAILURES = 3
AUTO_STOP_DNS_FAILURES = 1
AUTO_STOP_BLOCKED_FAILURES = 3
AUTO_STOP_NOT_FOUND_FAILURES = 8
CONTROL_REFRESH_INTERVAL_SECONDS = 0.75
RUNTIME_PERSIST_INTERVAL_SECONDS = 1.0
BLOCK_REASON_STATUS_HINT = 403

STATIC_PATH_PREFIXES = (
    "/_next/",
    "/wp-content/",
    "/wp-includes/",
    "/assets/",
    "/static/",
    "/images/",
    "/image/",
    "/img/",
    "/fonts/",
    "/css/",
    "/js/",
)
LOW_VALUE_PATH_PREFIXES = (
    "/privacy",
    "/terms",
    "/policy",
    "/policies",
    "/xmlrpc.php",
    "/feed",
    "/author/",
    "/tag/",
    "/category/",
)
STATIC_FILE_EXTENSIONS = {
    ".css",
    ".js",
    ".mjs",
    ".map",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".eot",
    ".pdf",
    ".zip",
    ".mp4",
    ".mp3",
    ".webm",
}
SITEMAP_PATHS = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
)
RESEARCH_PATH_BONUS = 64
ROOT_PAGE_BONUS = 52
SITEMAP_FETCH_LIMIT = 4
URLSET_LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>", re.I)
POSITIVE_RESEARCH_HINTS = {
    "provider": 22,
    "providers": 22,
    "doctor": 18,
    "doctors": 18,
    "therapist": 18,
    "therapists": 18,
    "psychologist": 22,
    "psychologists": 22,
    "psychiatrist": 22,
    "directory": 20,
    "search": 20,
    "find": 14,
    "license": 24,
    "verify": 24,
    "lookup": 18,
    "results": 16,
    "listing": 14,
    "staff": 14,
    "team": 14,
    "evaluation": 18,
    "evaluations": 18,
    "assessment": 18,
    "diagnostic": 18,
    "autism": 18,
    "adhd": 18,
    "telehealth": 10,
    "contact": 8,
}
NEGATIVE_RESEARCH_HINTS = {
    "privacy": -100,
    "terms": -100,
    "career": -90,
    "careers": -90,
    "job": -90,
    "jobs": -90,
    "news": -72,
    "blog": -72,
    "article": -72,
    "articles": -72,
    "event": -56,
    "events": -56,
    "press": -56,
    "donate": -64,
    "foundation": -64,
}
GENERIC_PATH_SEGMENTS = {
    "application",
    "applications",
    "default",
    "default.aspx",
    "default.html",
    "index",
    "index.aspx",
    "index.html",
    "page",
    "pages",
}
LICENSE_DISCOVERY_PATHS = {
    "/verify-a-license",
    "/license-verification",
    "/license-lookup",
    "/physician-search",
    "/find-a-doctor",
}


def _path_lower(normalized_url: str) -> str:
    return (urlparse(normalized_url).path or "/").lower()


def _path_prefix(normalized_url: str) -> str:
    path = _path_lower(normalized_url)
    if path == "/":
        return "/"
    for prefix in (*STATIC_PATH_PREFIXES, *LOW_VALUE_PATH_PREFIXES):
        normalized_prefix = prefix.rstrip("/") if prefix != "/" else prefix
        if path == normalized_prefix or path.startswith(prefix):
            return prefix
    parts = [part for part in path.split("/") if part]
    if not parts:
        return "/"
    first = parts[0]
    if "." in first:
        return f"/{first}"
    return f"/{first}/"


def _site_root_url(url: str) -> str:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc:
        return normalized
    return normalize_url(f"{parsed.scheme}://{parsed.netloc}/")


def _seed_anchor_segments(url: str) -> list[str]:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    parts = [part for part in (parsed.path or "/").split("/") if part]
    if parts and "." in parts[-1]:
        parts = parts[:-1]
    while parts and parts[-1].lower() in GENERIC_PATH_SEGMENTS:
        parts = parts[:-1]
    return parts


def _seed_looks_like_detail_page(seed: DiscoverySeed) -> bool:
    anchor_segments = _seed_anchor_segments(seed.website)
    if not anchor_segments:
        return False
    leaf = anchor_segments[-1].lower()
    if any(token in leaf for token in ("provider", "team", "staff", "directory", "search", "license", "lookup", "verify", "location")):
        return False
    if seed.source_type in {"hospital_directory", "university_directory", "practice_site", "state_registry"}:
        return len(leaf) >= 18 or leaf.count("-") >= 2 or len(anchor_segments) >= 2
    return False


def _seed_research_base_urls(seed: DiscoverySeed) -> list[str]:
    root_url = _site_root_url(seed.website)
    anchor_segments = _seed_anchor_segments(seed.website)
    if _seed_looks_like_detail_page(seed) and len(anchor_segments) > 1:
        anchor_segments = anchor_segments[:-1]
    bases: list[str] = []
    seen: set[str] = set()

    def add_base(parts: list[str]) -> None:
        if not parts:
            return
        candidate = normalize_url(f"{root_url.rstrip('/')}/{'/'.join(parts)}/")
        if candidate and candidate not in seen:
            seen.add(candidate)
            bases.append(candidate)

    for depth in range(len(anchor_segments), max(len(anchor_segments) - 3, 0), -1):
        add_base(anchor_segments[:depth])
    if len(anchor_segments) == 1:
        add_base(anchor_segments)
    return bases


def _agent_research_paths_for_seed(seed: DiscoverySeed, cfg: CrawlConfig) -> list[str]:
    if seed.source_type != "licensing_board" and _seed_looks_like_detail_page(seed):
        return []
    if seed.browser_required and seed.source_type in {"insurer_directory", "professional_directory"}:
        return []
    paths: list[str] = []
    seen: set[str] = set()
    for raw in [*cfg.agent_research_paths, *cfg.extra_paths]:
        path = str(raw).strip().lower()
        if not path or not path.startswith("/") or path in seen:
            continue
        if seed.source_type != "licensing_board" and path in LICENSE_DISCOVERY_PATHS:
            continue
        seen.add(path)
        paths.append(path)
    return paths


def _seed_path_proximity_bonus(url: str, seed: DiscoverySeed) -> int:
    candidate_segments = _seed_anchor_segments(url)
    seed_segments = _seed_anchor_segments(seed.website)
    if not candidate_segments or not seed_segments:
        return 0
    shared = 0
    for candidate_part, seed_part in zip(candidate_segments, seed_segments):
        if candidate_part.lower() != seed_part.lower():
            break
        shared += 1
    if shared == 0:
        return 0
    bonus = shared * 18
    if candidate_segments[: len(seed_segments)] == seed_segments[: len(candidate_segments)]:
        bonus += 12
    return bonus


def _fetch_seed_research_document(url: str, *, user_agent: str, timeout_seconds: float) -> tuple[int, str, str]:
    req = request.Request(url, headers={"User-Agent": user_agent})
    timeout = max(2.0, min(float(timeout_seconds or 8.0), 15.0))
    try:
        with request.urlopen(req, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="ignore")
            return int(getattr(response, "status", 200) or 200), str(response.headers.get("Content-Type") or ""), payload
    except error.HTTPError as exc:
        try:
            payload = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            payload = ""
        return int(exc.code or 0), str(exc.headers.get("Content-Type") or ""), payload
    except Exception:
        return 0, "", ""


def _parse_robots_sitemaps(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for line in (text or "").splitlines():
        stripped = line.strip()
        if not stripped or ":" not in stripped:
            continue
        label, value = stripped.split(":", 1)
        if label.strip().lower() != "sitemap":
            continue
        candidate = normalize_url(value.strip())
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        urls.append(candidate)
    return urls


def _parse_sitemap_urls(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for raw in URLSET_LOC_RE.findall(text or ""):
        candidate = normalize_url(raw)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        urls.append(candidate)
    return urls


def _score_seed_research_candidate(url: str, *, seed: DiscoverySeed, cfg: CrawlConfig) -> int:
    normalized = normalize_url(url)
    if not normalized or not same_domain(seed.website, normalized):
        return -1000
    parsed = urlparse(normalized)
    path = (parsed.path or "/").lower()
    score = 0
    if path in {"", "/"}:
        score += ROOT_PAGE_BONUS
    score += _seed_path_proximity_bonus(normalized, seed)
    configured_paths = {
        str(item).strip().lower()
        for item in _agent_research_paths_for_seed(seed, cfg)
        if str(item).strip()
    }
    if any(path == configured_path or path.endswith(configured_path) for configured_path in configured_paths):
        score += RESEARCH_PATH_BONUS
    for token, weight in POSITIVE_RESEARCH_HINTS.items():
        if token in path:
            score += weight
    for token, weight in NEGATIVE_RESEARCH_HINTS.items():
        if token in path:
            score += weight
    if seed.source_type == "licensing_board":
        if any(token in path for token in ("license", "verify", "lookup", "search", "results", "physician", "psychologist")):
            score += 18
    if path.endswith(".xml"):
        score -= 80
    return score


def _discover_seed_research_urls(seed: DiscoverySeed, cfg: CrawlConfig) -> list[str]:
    if not cfg.agent_research_enabled:
        return []

    root_url = _site_root_url(seed.website)
    candidate_urls: list[str] = []
    seen_candidates: set[str] = set()

    def add_candidate(url: str) -> None:
        normalized = normalize_url(url)
        if not normalized or normalized in seen_candidates or not same_domain(seed.website, normalized):
            return
        seen_candidates.add(normalized)
        candidate_urls.append(normalized)

    if normalize_url(root_url) != normalize_url(seed.website):
        add_candidate(root_url)

    research_bases = _seed_research_base_urls(seed)
    research_paths = _agent_research_paths_for_seed(seed, cfg)
    if not research_bases:
        research_bases = [root_url]
    for base_url in research_bases:
        if normalize_url(base_url) != normalize_url(seed.website):
            add_candidate(base_url)
        for path in research_paths:
            add_candidate(f"{base_url.rstrip('/')}{path}")

    sitemap_queue: list[str] = []
    sitemap_seen: set[str] = set()
    for sitemap_url in _parse_robots_sitemaps(
        _fetch_seed_research_document(f"{root_url.rstrip('/')}/robots.txt", user_agent=cfg.user_agent, timeout_seconds=cfg.timeout_seconds)[2]
    ):
        if same_domain(seed.website, sitemap_url) and sitemap_url not in sitemap_seen:
            sitemap_seen.add(sitemap_url)
            sitemap_queue.append(sitemap_url)
    if not sitemap_queue:
        for path in SITEMAP_PATHS:
            sitemap_url = normalize_url(f"{root_url.rstrip('/')}{path}")
            if sitemap_url not in sitemap_seen:
                sitemap_seen.add(sitemap_url)
                sitemap_queue.append(sitemap_url)

    fetched_sitemaps = 0
    while sitemap_queue and fetched_sitemaps < SITEMAP_FETCH_LIMIT:
        sitemap_url = sitemap_queue.pop(0)
        status_code, _, body = _fetch_seed_research_document(
            sitemap_url,
            user_agent=cfg.user_agent,
            timeout_seconds=cfg.timeout_seconds,
        )
        if status_code != 200 or not body:
            continue
        fetched_sitemaps += 1
        for discovered_url in _parse_sitemap_urls(body):
            if not same_domain(seed.website, discovered_url):
                continue
            path = (urlparse(discovered_url).path or "").lower()
            if path.endswith(".xml") and "sitemap" in path and discovered_url not in sitemap_seen and len(sitemap_seen) < SITEMAP_FETCH_LIMIT * 3:
                sitemap_seen.add(discovered_url)
                sitemap_queue.append(discovered_url)
                continue
            add_candidate(discovered_url)

    ranked = sorted(
        (
            candidate
            for candidate in candidate_urls
            if _score_seed_research_candidate(candidate, seed=seed, cfg=cfg) >= int(cfg.agent_research_min_score or 0)
        ),
        key=lambda item: (
            -_score_seed_research_candidate(item, seed=seed, cfg=cfg),
            len(urlparse(item).path or "/"),
            item,
        ),
    )
    limit = max(1, int(cfg.agent_research_limit or 1))
    return ranked[:limit]


def _is_valid_seed_domain(seed: DiscoverySeed) -> tuple[bool, str]:
    domain = normalize_domain(seed.website)
    if not domain:
        return False, "missing_domain"
    if " " in domain:
        return False, "invalid_domain_whitespace"
    if any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789.-:" for ch in domain):
        return False, "invalid_domain_chars"
    if "." not in domain and not domain.startswith("localhost") and not domain.startswith("127.0.0.1"):
        return False, "invalid_domain_format"
    return True, domain


def _failure_kind(status_code: int, error_message: str) -> str:
    lowered = (error_message or "").lower()
    if "dns error" in lowered or "lookup address" in lowered or "nodename nor servname" in lowered:
        return "dns"
    if status_code in {401, 403, 429, 503}:
        return "blocked"
    if status_code == 404:
        return "not_found"
    if "failed to connect" in lowered or "connecterror" in lowered or "connection" in lowered:
        return "connect"
    return "error"


def _status_code_from_block_reason(reason: str) -> int:
    if not reason:
        return 0
    if reason.startswith("status:"):
        try:
            return int(reason.split(":", 1)[1])
        except ValueError:
            return 0
    if reason.startswith("marker:"):
        return BLOCK_REASON_STATUS_HINT
    return 0


@dataclass
class SeedCrawlState:
    con: sqlite3.Connection
    seed: DiscoverySeed
    cfg: CrawlConfig
    policy: DomainPolicy
    metrics: Any
    logger: Any
    job_id: str
    denylist: set[str]
    recorder: SeedRunRecorder
    crawl_pages: int
    total_page_limit: int
    crawl_depth: int
    browser_page_limit: int
    run_state_dir: str | Path | None = None
    seen_urls: set[str] = field(default_factory=set)
    queued_requests: list[QueueItem] = field(default_factory=list)
    processed_urls: set[str] = field(default_factory=set)
    browser_escalation_requested: bool = False
    browser_escalation_reason: str = ""
    filtered_urls: int = 0
    discovery_enabled: bool = True
    stop_requested: bool = False
    seed_quarantined: bool = False
    seed_quarantine_reason: str = ""
    manual_suppressed_path_prefixes: set[str] = field(default_factory=set)
    auto_suppressed_path_prefixes: set[str] = field(default_factory=set)
    failure_counts: dict[str, int] = field(default_factory=dict)
    prefix_failure_counts: dict[str, int] = field(default_factory=dict)
    current_error: str = ""
    seed_exception_reason: str = ""
    recorded_action_keys: set[str] = field(default_factory=set)
    control_last_refreshed_at: float = 0.0
    runtime_last_persisted_at: float = 0.0
    base_crawl_pages: int = 0
    base_total_page_limit: int = 0
    base_browser_page_limit: int = 0

    def __post_init__(self) -> None:
        self.base_crawl_pages = int(self.crawl_pages)
        self.base_total_page_limit = int(self.total_page_limit)
        self.base_browser_page_limit = int(self.browser_page_limit)
        ensure_run_control(self.job_id, self.run_state_dir)
        self.refresh_controls(force=True)
        self.persist_runtime(force=True, status="pending")

    @property
    def domain(self) -> str:
        return normalize_domain(self.seed.website)

    @property
    def allowed_schemes(self) -> set[str]:
        return self.cfg.merged_schemes()

    @property
    def block_patterns(self) -> tuple[str, ...]:
        return tuple(self.cfg.crawlee_extra_block_patterns) + tuple(self.policy.extra_block_patterns)

    @property
    def remaining_total_budget(self) -> int:
        return max(0, self.total_page_limit - self.recorder.run_pages_fetched)

    @property
    def remaining_browser_budget(self) -> int:
        return max(0, min(self.browser_page_limit, self.remaining_total_budget))

    def can_escalate_to_browser(self) -> bool:
        if self.policy.mode == "http_only":
            return False
        if not self.policy.browser_on_block:
            return False
        return self.remaining_browser_budget > 0

    @property
    def suppressed_path_prefixes(self) -> set[str]:
        return set(self.manual_suppressed_path_prefixes) | set(self.auto_suppressed_path_prefixes)

    def _control_state(self) -> dict[str, Any]:
        return load_run_control(self.job_id, self.run_state_dir)

    def persist_runtime(self, *, force: bool = False, status: str | None = None) -> None:
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self.runtime_last_persisted_at < RUNTIME_PERSIST_INTERVAL_SECONDS:
            return
        state = self._control_state()
        runtime = domain_runtime_record(state, self.domain)
        if status:
            runtime["status"] = status
        runtime["processed_urls"] = len(self.processed_urls)
        runtime["success_pages"] = int(self.recorder.run_success_pages)
        runtime["failure_pages"] = int(self.recorder.run_failure_pages)
        runtime["filtered_urls"] = int(self.filtered_urls)
        runtime["last_status_code"] = int(self.recorder.last_status_code or 0)
        runtime["last_error"] = self.current_error[:240]
        runtime["discovery_enabled"] = bool(self.discovery_enabled)
        runtime["browser_escalated"] = bool(self.browser_escalation_requested)
        runtime["updated_at"] = utcnow_iso()
        state.setdefault("runtime", {})["current_seed_domain"] = self.domain if runtime["status"] == "running" else ""
        save_run_control(state, self.run_state_dir)
        self.runtime_last_persisted_at = now_monotonic

    def _persist_intervention(
        self,
        *,
        key: str,
        action: str,
        reason: str,
        source: str,
        details: dict[str, Any] | None = None,
    ) -> bool:
        if key in self.recorded_action_keys:
            return False
        self.recorded_action_keys.add(key)
        state = self._control_state()
        append_intervention(
            state,
            domain=self.domain,
            action=action,
            reason=reason,
            source=source,
            details=details,
        )
        runtime = domain_runtime_record(state, self.domain)
        runtime["last_error"] = reason
        runtime["updated_at"] = utcnow_iso()
        save_run_control(state, self.run_state_dir)
        self.logger.warning(
            "fetch_intervention",
            extra={
                "job_id": self.job_id,
                "stage": "fetch",
                "domain": self.domain,
                "action": action,
                "reason": reason,
                "details": details or {},
            },
        )
        self.runtime_last_persisted_at = 0.0
        return True

    def refresh_controls(self, *, force: bool = False) -> None:
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self.control_last_refreshed_at < CONTROL_REFRESH_INTERVAL_SECONDS:
            return

        state = self._control_state()
        control = dict((((state.get("agent_controls") or {}).get("domains") or {}).get(self.domain)) or {})
        self.manual_suppressed_path_prefixes = {
            str(item).strip().lower()
            for item in control.get("suppressed_path_prefixes", [])
            if str(item).strip()
        }
        self.seed_quarantined = bool(control.get("quarantined"))
        self.seed_quarantine_reason = str(control.get("quarantine_reason") or "")
        self.stop_requested = bool(control.get("stop_requested")) or self.seed_quarantined
        override = control.get("max_pages_per_domain")
        if override not in (None, ""):
            max_pages = max(1, int(override))
            self.crawl_pages = min(self.base_crawl_pages, max_pages)
            self.total_page_limit = min(self.base_total_page_limit, max_pages)
            self.browser_page_limit = min(self.base_browser_page_limit, max_pages)
        else:
            self.crawl_pages = self.base_crawl_pages
            self.total_page_limit = self.base_total_page_limit
            self.browser_page_limit = self.base_browser_page_limit
        self.control_last_refreshed_at = now_monotonic

    def _update_control_record(
        self,
        *,
        quarantined: bool | None = None,
        quarantine_reason: str | None = None,
        stop_requested: bool | None = None,
        add_suppressed_prefix: str | None = None,
    ) -> dict[str, Any]:
        state = self._control_state()
        record = domain_control_record(state, self.domain)
        if quarantined is not None:
            record["quarantined"] = bool(quarantined)
        if quarantine_reason is not None:
            record["quarantine_reason"] = str(quarantine_reason)
        elif quarantined is False:
            record["quarantine_reason"] = ""
        if stop_requested is not None:
            record["stop_requested"] = bool(stop_requested)
        if add_suppressed_prefix:
            prefixes = {
                str(item).strip().lower()
                for item in record.get("suppressed_path_prefixes", [])
                if str(item).strip()
            }
            prefixes.add(str(add_suppressed_prefix).strip().lower())
            record["suppressed_path_prefixes"] = sorted(prefixes)
        record["updated_at"] = utcnow_iso()
        save_run_control(state, self.run_state_dir)
        self.refresh_controls(force=True)
        return record

    def mark_processed(self, normalized_url: str) -> None:
        self.processed_urls.add(normalized_url)
        self.persist_runtime()

    def request_browser_escalation(self, reason: str) -> None:
        if self.browser_escalation_requested or not self.can_escalate_to_browser():
            return
        self.browser_escalation_requested = True
        self.browser_escalation_reason = reason
        self.metrics.inc("browser_escalations")
        self._persist_intervention(
            key=f"browser:{reason}",
            action="browser_escalation",
            reason=reason,
            source="auto",
            details={"remaining_browser_budget": self.remaining_browser_budget},
        )
        self.persist_runtime(force=True)

    def _rejection_reason(self, normalized_url: str) -> str:
        path = _path_lower(normalized_url)
        for prefix in self.suppressed_path_prefixes:
            if path == prefix.rstrip("/") or path.startswith(prefix):
                return "suppressed_prefix"
        if any(path.startswith(prefix) for prefix in STATIC_PATH_PREFIXES):
            return "static_path"
        for prefix in LOW_VALUE_PATH_PREFIXES:
            normalized_prefix = prefix.rstrip("/") if prefix != "/" else prefix
            if path == normalized_prefix or path.startswith(prefix):
                return "low_value_path"
        if any(path.endswith(ext) for ext in STATIC_FILE_EXTENSIONS):
            return "static_extension"
        return ""

    def observe_success(self) -> None:
        self.current_error = ""
        self.persist_runtime()

    def observe_failure(self, *, normalized_url: str, status_code: int, error_message: str) -> str | None:
        failure_kind = _failure_kind(status_code, error_message)
        self.failure_counts[failure_kind] = int(self.failure_counts.get(failure_kind, 0)) + 1
        self.current_error = (error_message or failure_kind)[:240]
        prefix = _path_prefix(normalized_url)
        if prefix != "/":
            self.prefix_failure_counts[prefix] = int(self.prefix_failure_counts.get(prefix, 0)) + 1
            if (
                self.prefix_failure_counts[prefix] >= AUTO_SUPPRESS_PREFIX_FAILURES
                and prefix not in self.suppressed_path_prefixes
            ):
                state = self._control_state()
                self._update_control_record(add_suppressed_prefix=prefix)
                state = self._control_state()
                append_intervention(
                    state,
                    domain=self.domain,
                    action="auto_suppress_prefix",
                    reason=f"{failure_kind}_storm",
                    source="auto",
                    details={"prefix": prefix, "failures": self.prefix_failure_counts[prefix]},
                )
                save_run_control(state, self.run_state_dir)
                self.recorded_action_keys.add(f"prefix:{prefix}")
                self.logger.warning(
                    "fetch_intervention",
                    extra={
                        "job_id": self.job_id,
                        "stage": "fetch",
                        "domain": self.domain,
                        "action": "auto_suppress_prefix",
                        "reason": f"{failure_kind}_storm",
                        "details": {"prefix": prefix, "failures": self.prefix_failure_counts[prefix]},
                    },
                )

        if failure_kind == "dns" and self.failure_counts[failure_kind] >= AUTO_STOP_DNS_FAILURES:
            self.seed_quarantined = True
            self.seed_quarantine_reason = "dns_failure"
            self.stop_requested = True
            self.discovery_enabled = False
            self._update_control_record(
                quarantined=True,
                quarantine_reason=self.seed_quarantine_reason,
                stop_requested=True,
            )
            self._persist_intervention(
                key="stop:dns",
                action="auto_quarantine_seed",
                reason="dns_failure",
                source="auto",
                details={"error": error_message[:200]},
            )
            self.persist_runtime(force=True)
            return "dns_failure"

        if failure_kind == "blocked" and self.failure_counts[failure_kind] >= AUTO_STOP_BLOCKED_FAILURES:
            self.stop_requested = True
            self.discovery_enabled = False
            self._update_control_record(stop_requested=True)
            self._persist_intervention(
                key="stop:blocked",
                action="auto_stop_domain",
                reason="blocked_storm",
                source="auto",
                details={"blocked_failures": self.failure_counts[failure_kind]},
            )
            self.persist_runtime(force=True)
            return "blocked_storm"

        if failure_kind == "not_found" and self.failure_counts[failure_kind] >= AUTO_STOP_NOT_FOUND_FAILURES:
            self.discovery_enabled = False
            self.stop_requested = self.recorder.run_success_pages == 0
            if self.stop_requested:
                self._update_control_record(stop_requested=True)
            self._persist_intervention(
                key="stop:not_found",
                action="auto_disable_discovery",
                reason="not_found_storm",
                source="auto",
                details={"not_found_failures": self.failure_counts[failure_kind]},
            )
            self.persist_runtime(force=True)
            return "not_found_storm" if self.stop_requested else None

        self.persist_runtime()
        return None

    def should_accept_url(self, normalized_url: str, depth: int) -> bool:
        self.refresh_controls(force=True)
        if self.stop_requested or self.seed_quarantined:
            return False
        parsed = urlparse(normalized_url)
        if parsed.scheme.lower() not in self.allowed_schemes:
            return False
        domain = normalize_domain(normalized_url)
        if not domain or domain in self.denylist:
            return False
        if not same_domain(self.seed.website, normalized_url):
            return False
        if depth > 0 and not self.discovery_enabled:
            self.filtered_urls += 1
            self.metrics.inc("pages_filtered")
            self.persist_runtime()
            return False
        rejection_reason = self._rejection_reason(normalized_url)
        if rejection_reason:
            self.filtered_urls += 1
            self.metrics.inc("pages_filtered")
            self.metrics.inc(f"pages_filtered_{rejection_reason}")
            self.persist_runtime()
            return False
        return True

    def queue_url(self, url: str, depth: int) -> QueueItem | None:
        normalized = normalize_url(url)
        if not normalized or normalized in self.seen_urls:
            return None
        self.seen_urls.add(normalized)

        if not self.should_accept_url(normalized, depth):
            return None
        if len(self.seen_urls) > self.crawl_pages * 2:
            return None
        if already_fetched_recently(self.con, normalized, self.cfg):
            self.metrics.inc("pages_cached")
            return None

        item = QueueItem(requested_url=normalized, normalized_url=normalized, depth=depth)
        self.queued_requests.append(item)
        return item

    def seed_initial_requests(self) -> list[QueueItem]:
        self.refresh_controls(force=True)
        if self.seed_quarantined or self.stop_requested:
            self.persist_runtime(force=True, status="quarantined" if self.seed_quarantined else "stopped")
            return []
        items: list[QueueItem] = []
        seed_url = self.queue_url(self.seed.website, 0)
        if seed_url is not None:
            items.append(seed_url)
        research_candidates = _discover_seed_research_urls(self.seed, self.cfg)
        for candidate in research_candidates:
            item = self.queue_url(candidate, 0)
            if item is not None:
                items.append(item)
        return items

    def enqueue_links_from_html(self, base_url: str, html: str, depth: int) -> list[QueueItem]:
        if depth >= self.crawl_depth or not self.discovery_enabled or self.stop_requested:
            return []
        queued: list[QueueItem] = []
        links = list(extract_links(base_url, html))
        links.sort(
            key=lambda raw: (
                -_score_seed_research_candidate(
                    normalize_url(raw) if raw.startswith(("http://", "https://")) else resolve_link(base_url, raw),
                    seed=self.seed,
                    cfg=self.cfg,
                ),
                len(raw),
                raw,
            )
        )
        for link in links:
            if link.startswith("http://") or link.startswith("https://"):
                next_url = normalize_url(link)
            else:
                next_url = resolve_link(base_url, link)
            item = self.queue_url(next_url, depth + 1)
            if item is not None:
                queued.append(item)
        return queued

    def remaining_queue_for_browser(self) -> list[QueueItem]:
        if self.remaining_browser_budget <= 0:
            return []
        pending = [item for item in self.queued_requests if item.normalized_url not in self.processed_urls]
        return pending[: self.remaining_browser_budget]


def _close_crawlee_loop() -> None:
    global _CRAWLEE_LOOP
    if _CRAWLEE_LOOP is None or _CRAWLEE_LOOP.is_closed():
        return
    _CRAWLEE_LOOP.close()
    _CRAWLEE_LOOP = None


atexit.register(_close_crawlee_loop)


def _concurrency_settings(cfg: CrawlConfig) -> ConcurrencySettings:
    max_concurrency = max(1, int(cfg.max_concurrency))
    desired = 1 if max_concurrency == 1 else min(max_concurrency, 2)
    max_interval = max(cfg.crawl_delay_seconds, cfg.per_domain_min_interval_seconds, 0.0)
    max_tasks_per_minute = float("inf")
    if max_interval > 0:
        max_tasks_per_minute = max(1.0, 60.0 / max_interval)
    return ConcurrencySettings(
        min_concurrency=1,
        max_concurrency=max_concurrency,
        desired_concurrency=desired,
        max_tasks_per_minute=max_tasks_per_minute,
    )


def _proxy_configuration(cfg: CrawlConfig) -> ProxyConfiguration | None:
    if not cfg.crawlee_proxy_urls:
        return None
    return ProxyConfiguration(proxy_urls=list(cfg.crawlee_proxy_urls))


def _request_from_queue(item: QueueItem, cfg: CrawlConfig) -> Request:
    request = Request.from_url(
        item.normalized_url,
        unique_key=item.normalized_url,
        max_retries=cfg.max_retries,
        headers={"User-Agent": cfg.user_agent},
        user_data={"requested_url": item.requested_url, "depth": item.depth},
    )
    request.crawl_depth = item.depth
    return request


def _storage_name(prefix: str, value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "-" for ch in value.lower()).strip("-")
    cleaned = cleaned or "seed"
    return f"{prefix}-{cleaned}"


def _run_crawlee(coro):
    global _CRAWLEE_LOOP

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop is not None:
        raise RuntimeError("run_fetch cannot be called from an active asyncio event loop.")

    if _CRAWLEE_LOOP is None or _CRAWLEE_LOOP.is_closed():
        _CRAWLEE_LOOP = asyncio.new_event_loop()

    asyncio.set_event_loop(_CRAWLEE_LOOP)
    return _CRAWLEE_LOOP.run_until_complete(coro)


def _effective_crawl_pages(
    cfg: CrawlConfig,
    policy: DomainPolicy,
    override: int | None,
) -> int:
    candidate = override if override and override > 0 else cfg.max_pages_per_domain
    if candidate <= 0:
        candidate = 30
    if policy.max_pages_per_domain is not None and policy.max_pages_per_domain > 0:
        return policy.max_pages_per_domain
    return candidate


def _effective_total_pages(cfg: CrawlConfig, override: int | None, crawl_pages: int) -> int:
    candidate = override if override and override > 0 else cfg.max_total_pages
    if not candidate or candidate <= 0:
        candidate = crawl_pages
    return max(1, min(int(candidate), crawl_pages))


def _effective_crawl_depth(cfg: CrawlConfig, policy: DomainPolicy, override: int | None) -> int:
    candidate = override if override is not None and override >= 0 else cfg.max_depth
    if policy.max_depth is not None and policy.max_depth >= 0:
        candidate = policy.max_depth
    return min(max(int(candidate), 0), 10)


def _header_value(headers: Any, name: str) -> str:
    lowered = name.lower()
    try:
        for key, value in dict(headers).items():
            if str(key).lower() == lowered:
                return str(value)
    except Exception:
        return ""
    return ""


def _decode_html_bytes(payload: bytes, *, content_type: str | None) -> str:
    if not is_html_content_type(content_type):
        return ""
    return (payload or b"").decode("utf-8", errors="ignore")


async def _read_http_body(http_response: Any, *, content_type: str | None) -> str:
    if not is_html_content_type(content_type):
        return ""
    try:
        return (await http_response.read()).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _usable_content(status_code: int, html: str, block_signal: BlockSignal) -> bool:
    return status_code == 200 and bool(html) and not block_signal.triggered


def _resolved_browser_isolation(cfg: CrawlConfig) -> str:
    mode = str(getattr(cfg, "crawlee_browser_isolation", "") or "").strip().lower()
    if mode in {"inline", "subprocess"}:
        return mode
    return "subprocess" if sys.platform == "darwin" else "inline"


def _browser_worker_payload(state: SeedCrawlState, initial_requests: list[QueueItem]) -> dict[str, Any]:
    return {
        "seed": {
            "name": state.seed.name,
            "website": state.seed.website,
            "state": state.seed.state,
            "market": state.seed.market,
            "source": state.seed.source,
            "priority": state.seed.priority,
        },
        "config": {
            "user_agent": state.cfg.user_agent,
            "timeout_seconds": state.cfg.timeout_seconds,
            "max_retries": state.cfg.max_retries,
            "crawl_delay_seconds": state.cfg.crawl_delay_seconds,
            "respect_robots": state.cfg.respect_robots,
            "max_concurrency": state.cfg.max_concurrency,
            "crawlee_headless": state.cfg.crawlee_headless,
            "crawlee_browser_type": state.cfg.crawlee_browser_type,
            "crawlee_proxy_urls": list(state.cfg.crawlee_proxy_urls),
            "crawlee_use_session_pool": state.cfg.crawlee_use_session_pool,
            "crawlee_retry_on_blocked": state.cfg.crawlee_retry_on_blocked,
            "crawlee_max_session_rotations": state.cfg.crawlee_max_session_rotations,
            "crawlee_viewport_width": state.cfg.crawlee_viewport_width,
            "crawlee_viewport_height": state.cfg.crawlee_viewport_height,
            "allowed_schemes": list(state.allowed_schemes),
        },
        "policy": {
            "wait_for_selector": state.policy.wait_for_selector,
        },
        "limits": {
            "crawl_depth": state.crawl_depth,
            "browser_budget": state.remaining_browser_budget,
        },
        "block_patterns": list(state.block_patterns),
        "denylist": sorted(state.denylist),
        "seen_urls": sorted(state.seen_urls),
        "processed_urls": sorted(state.processed_urls),
        "suppressed_path_prefixes": sorted(state.suppressed_path_prefixes),
        "initial_requests": [
            {
                "requested_url": item.requested_url,
                "normalized_url": item.normalized_url,
                "depth": item.depth,
            }
            for item in initial_requests[: state.remaining_browser_budget]
        ],
    }


def _spawn_browser_worker(payload_path: Path, result_path: Path) -> tuple[int, str]:
    argv = [
        sys.executable,
        "-m",
        "pipeline.fetch_backends.browser_worker",
        str(payload_path),
        str(result_path),
    ]
    env = os.environ.copy()
    repo_root = str(Path(__file__).resolve().parents[2])
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = repo_root if not existing_pythonpath else f"{repo_root}{os.pathsep}{existing_pythonpath}"

    if hasattr(os, "posix_spawn"):
        pid = os.posix_spawn(sys.executable, argv, env)
        _, status = os.waitpid(pid, 0)
        return os.waitstatus_to_exitcode(status), "posix_spawn"

    import subprocess

    completed = subprocess.run(argv, env=env, check=False)
    return int(completed.returncode), "subprocess"


def _run_browser_worker_subprocess(state: SeedCrawlState, initial_requests: list[QueueItem]) -> dict[str, Any]:
    payload = _browser_worker_payload(state, initial_requests)
    with tempfile.TemporaryDirectory(prefix="provider-intel-browser-worker-") as td:
        payload_path = Path(td) / "payload.json"
        result_path = Path(td) / "result.json"
        payload_path.write_text(json.dumps(payload), encoding="utf-8")
        exit_code, launch_mode = _spawn_browser_worker(payload_path, result_path)
        if not result_path.exists():
            raise RuntimeError(f"browser worker failed before writing results (exit_code={exit_code}, launch_mode={launch_mode})")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            raise RuntimeError(f"browser worker returned malformed payload (exit_code={exit_code}, launch_mode={launch_mode})")
        result.setdefault("launch_mode", launch_mode)
        result.setdefault("exit_code", exit_code)
        if exit_code != 0 and not result.get("ok", False):
            raise RuntimeError(
                str(result.get("error") or f"browser worker exited with code {exit_code} via {launch_mode}")
            )
        return result


def _apply_browser_worker_result(state: SeedCrawlState, payload: dict[str, Any]) -> None:
    status_hint = first_positive_status_code(payload.get("status_code_hint"), payload.get("last_status_code"))
    if status_hint > 0:
        state.recorder.note_status_hint(status_hint)

    for reason, count in dict(payload.get("filtered_counts") or {}).items():
        metric_key = f"pages_filtered_{str(reason).strip()}"
        if int(count or 0) > 0:
            state.metrics.inc(metric_key, int(count))
    filtered_total = int(payload.get("filtered_urls") or 0)
    if filtered_total > 0:
        state.filtered_urls += filtered_total
        state.metrics.inc("pages_filtered", filtered_total)

    processed = {
        normalize_url(str(url))
        for url in payload.get("processed_urls", [])
        if normalize_url(str(url))
    }
    for item in payload.get("results", []):
        if not isinstance(item, dict):
            continue
        normalized_url = normalize_url(str(item.get("normalized_url") or ""))
        requested_url = str(item.get("requested_url") or normalized_url or "")
        if normalized_url:
            processed.add(normalized_url)
        emit_result = bool(item.get("emit_result"))
        count_as_success = bool(item.get("count_as_success"))
        result = state.recorder.record_result(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=int(item.get("status_code") or 0),
            content=str(item.get("content") or ""),
            error_message=str(item.get("error_message") or ""),
            attempt_count=int(item.get("attempt_count") or 1),
            emit_result=emit_result,
            count_as_success=count_as_success,
            used_browser=True,
        )
        if count_as_success and emit_result and result is not None:
            state.observe_success()
        else:
            state.observe_failure(
                normalized_url=normalized_url or requested_url,
                status_code=int(item.get("status_code") or 0),
                error_message=str(item.get("error_message") or ""),
            )

    for normalized_url in sorted(processed):
        if normalized_url not in state.processed_urls:
            state.mark_processed(normalized_url)

    if payload.get("error"):
        raise RuntimeError(str(payload.get("error")))


def _run_browser_crawl_dispatch(state: SeedCrawlState, initial_requests: list[QueueItem]) -> None:
    if not initial_requests or state.remaining_browser_budget <= 0:
        return
    mode = _resolved_browser_isolation(state.cfg)
    state.persist_runtime(force=True, status="running")
    if mode == "subprocess":
        payload = _run_browser_worker_subprocess(state, initial_requests)
        _apply_browser_worker_result(state, payload)
        return
    _run_crawlee(_run_browser_crawl(state, initial_requests))


def _seed_final_status(state: SeedCrawlState) -> str:
    if state.seed_quarantined:
        return "quarantined"
    if state.seed_exception_reason and not state.recorder.has_success and state.recorder.run_pages_fetched == 0:
        return "failed"
    if state.stop_requested and not state.recorder.has_success and state.recorder.run_pages_fetched == 0:
        return "stopped"
    if state.recorder.has_success:
        return "partial" if state.stop_requested or state.seed_exception_reason else "completed"
    if state.recorder.run_pages_fetched:
        return "partial"
    return "empty"


def _handle_seed_crawl_exception(state: SeedCrawlState, exc: Exception) -> None:
    error_message = (str(exc) or exc.__class__.__name__).strip()
    status_hint = first_positive_status_code(
        status_code_from_error_text(exc),
        _status_code_from_block_reason(state.browser_escalation_reason),
        state.recorder.last_status_code,
    )
    if status_hint > 0:
        state.recorder.note_status_hint(status_hint)

    state.seed_exception_reason = error_message[:240]
    state.current_error = state.seed_exception_reason
    state.stop_requested = True
    state.discovery_enabled = False
    state._update_control_record(stop_requested=True)

    action = "auto_stop_domain"
    reason = "crawler_exception"
    if state.browser_escalation_requested:
        action = "auto_stop_domain"
        reason = "browser_crawl_exception"

    state._persist_intervention(
        key=f"fatal:{action}:{reason}",
        action=action,
        reason=reason,
        source="auto",
        details={
            "error": error_message[:200],
            "status_code": status_hint,
            "browser_reason": state.browser_escalation_reason,
        },
    )
    state.metrics.inc("seed_crawl_exceptions")
    state.logger.error(
        "seed_crawl_exception",
        extra={
            "job_id": state.job_id,
            "stage": "fetch",
            "domain": state.domain,
            "error": error_message[:240],
            "status_code": status_hint,
            "browser_reason": state.browser_escalation_reason,
        },
    )


async def _run_http_crawl(state: SeedCrawlState, initial_requests: list[QueueItem]) -> None:
    if not initial_requests or state.remaining_total_budget <= 0:
        return

    state.persist_runtime(force=True, status="running")
    request_queue = await RequestQueue.open(name=_storage_name("provider-intel-http", state.recorder.job_pk))
    crawler = HttpCrawler(
        request_manager=request_queue,
        max_requests_per_crawl=min(state.remaining_total_budget, len(initial_requests) + (state.crawl_pages * 2)),
        max_request_retries=state.cfg.max_retries,
        max_session_rotations=state.cfg.crawlee_max_session_rotations,
        use_session_pool=state.cfg.crawlee_use_session_pool,
        retry_on_blocked=state.cfg.crawlee_retry_on_blocked,
        respect_robots_txt_file=state.cfg.respect_robots,
        concurrency_settings=_concurrency_settings(state.cfg),
        proxy_configuration=_proxy_configuration(state.cfg),
        request_handler_timeout=timedelta(seconds=max(30, int(state.cfg.timeout_seconds * 4))),
        navigation_timeout=timedelta(seconds=max(1, int(state.cfg.timeout_seconds))),
        configure_logging=False,
    )

    async def on_skipped_request(url: str, reason: str) -> None:
        if reason == "robots_txt":
            state.recorder.note_status_hint(403)
            state.logger.warning(
                "Robots blocked",
                extra={"job_id": state.job_id, "stage": "fetch", "url": url},
            )

    crawler.on_skipped_request(on_skipped_request)

    @crawler.failed_request_handler
    async def failed_request_handler(context, error: Exception) -> None:
        state.refresh_controls(force=True)
        requested_url = str(context.request.user_data.get("requested_url") or context.request.url)
        normalized_url = normalize_url(context.request.url)
        content_type = None
        html = ""
        if hasattr(context, "http_response"):
            try:
                content_type = _header_value(context.http_response.headers, "content-type")
            except Exception:
                content_type = None
            html = await _read_http_body(context.http_response, content_type=content_type)
        status_code = first_positive_status_code(
            getattr(error, "status_code", 0),
            getattr(getattr(context, "http_response", None), "status_code", 0),
            status_code_from_error_text(error),
        )

        block_signal = detect_block_signal(
            status_code=status_code,
            content=html,
            extra_patterns=state.block_patterns,
        )
        state.mark_processed(normalized_url)
        state.recorder.record_result(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content=html,
            error_message=str(error),
            attempt_count=context.request.retry_count + 1,
            emit_result=False,
            count_as_success=False,
        )
        stop_reason = state.observe_failure(
            normalized_url=normalized_url,
            status_code=status_code,
            error_message=str(error),
        )

        if block_signal.triggered and state.can_escalate_to_browser():
            state.observe_failure(
                normalized_url=normalized_url,
                status_code=status_code,
                error_message=block_signal.reason,
            )
            state.request_browser_escalation(block_signal.reason)
            crawler.stop(f"http blocked: {block_signal.reason}")
            return
        if state.stop_requested or stop_reason:
            crawler.stop(stop_reason or "domain_stop_requested")

    @crawler.router.default_handler
    async def request_handler(context) -> None:
        state.refresh_controls(force=True)
        if state.stop_requested or state.seed_quarantined:
            crawler.stop(state.seed_quarantine_reason or "domain_stop_requested")
            return
        requested_url = str(context.request.user_data.get("requested_url") or context.request.url)
        normalized_url = normalize_url(context.request.url)
        status_code = int(context.http_response.status_code)
        content_type = _header_value(context.http_response.headers, "content-type")
        html = _decode_html_bytes(context.parsed_content, content_type=content_type)
        block_signal = detect_block_signal(
            status_code=status_code,
            content=html,
            extra_patterns=state.block_patterns,
        )
        state.mark_processed(normalized_url)
        result = state.recorder.record_result(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content=html,
            error_message="" if _usable_content(status_code, html, block_signal) else "blocked_or_non_html",
            attempt_count=context.request.retry_count + 1,
            emit_result=_usable_content(status_code, html, block_signal),
            count_as_success=_usable_content(status_code, html, block_signal),
        )

        if block_signal.triggered and state.can_escalate_to_browser():
            state.request_browser_escalation(block_signal.reason)
            crawler.stop(f"http blocked: {block_signal.reason}")
            return

        if result is None:
            stop_reason = state.observe_failure(
                normalized_url=normalized_url,
                status_code=status_code,
                error_message="blocked_or_non_html",
            )
            if state.stop_requested or stop_reason:
                crawler.stop(stop_reason or "domain_stop_requested")
            return

        state.observe_success()
        if result is None or state.remaining_total_budget <= 0:
            return

        next_items = state.enqueue_links_from_html(normalized_url, html, int(context.request.crawl_depth))
        if next_items:
            await context.add_requests([_request_from_queue(item, state.cfg) for item in next_items])

    try:
        await crawler.run([_request_from_queue(item, state.cfg) for item in initial_requests])
    finally:
        await request_queue.drop()


async def _run_browser_crawl(state: SeedCrawlState, initial_requests: list[QueueItem]) -> None:
    browser_budget = min(state.remaining_browser_budget, len(initial_requests))
    if not initial_requests or browser_budget <= 0:
        return

    state.persist_runtime(force=True, status="running")
    request_queue = await RequestQueue.open(name=_storage_name("provider-intel-browser", state.recorder.job_pk))
    crawler = PlaywrightCrawler(
        request_manager=request_queue,
        browser_type=state.cfg.crawlee_browser_type,
        headless=state.cfg.crawlee_headless,
        max_requests_per_crawl=browser_budget,
        max_request_retries=state.cfg.max_retries,
        max_session_rotations=state.cfg.crawlee_max_session_rotations,
        use_session_pool=state.cfg.crawlee_use_session_pool,
        retry_on_blocked=state.cfg.crawlee_retry_on_blocked,
        respect_robots_txt_file=state.cfg.respect_robots,
        concurrency_settings=_concurrency_settings(state.cfg),
        proxy_configuration=_proxy_configuration(state.cfg),
        request_handler_timeout=timedelta(seconds=max(45, int(state.cfg.timeout_seconds * 6))),
        navigation_timeout=timedelta(seconds=max(3, int(state.cfg.timeout_seconds * 2))),
        browser_new_context_options={
            "viewport": {
                "width": int(state.cfg.crawlee_viewport_width),
                "height": int(state.cfg.crawlee_viewport_height),
            },
            "user_agent": state.cfg.user_agent,
        },
        configure_logging=False,
    )

    async def on_skipped_request(url: str, reason: str) -> None:
        if reason == "robots_txt":
            state.recorder.note_status_hint(403)
            state.logger.warning(
                "Robots blocked",
                extra={"job_id": state.job_id, "stage": "fetch", "url": url},
            )

    crawler.on_skipped_request(on_skipped_request)

    async def pre_navigation(context) -> None:
        await context.block_requests(extra_url_patterns=list(state.block_patterns))

    crawler.pre_navigation_hook(pre_navigation)

    @crawler.failed_request_handler
    async def failed_request_handler(context, error: Exception) -> None:
        state.refresh_controls(force=True)
        requested_url = str(context.request.user_data.get("requested_url") or context.request.url)
        normalized_url = normalize_url(context.request.url)
        status_code = first_positive_status_code(
            getattr(error, "status_code", 0),
            getattr(getattr(context, "response", None), "status", 0),
            status_code_from_error_text(error),
        )
        state.mark_processed(normalized_url)
        state.recorder.record_result(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content="",
            error_message=str(error),
            attempt_count=context.request.retry_count + 1,
            emit_result=False,
            count_as_success=False,
            used_browser=True,
        )
        stop_reason = state.observe_failure(
            normalized_url=normalized_url,
            status_code=status_code,
            error_message=str(error),
        )
        if state.stop_requested or stop_reason:
            crawler.stop(stop_reason or "domain_stop_requested")

    @crawler.router.default_handler
    async def request_handler(context) -> None:
        state.refresh_controls(force=True)
        if state.stop_requested or state.seed_quarantined:
            crawler.stop(state.seed_quarantine_reason or "domain_stop_requested")
            return
        requested_url = str(context.request.user_data.get("requested_url") or context.request.url)
        normalized_url = normalize_url(context.request.url)

        if state.policy.wait_for_selector:
            try:
                await context.page.wait_for_selector(
                    state.policy.wait_for_selector,
                    timeout=int(state.cfg.timeout_seconds * 1000),
                )
            except PlaywrightTimeoutError:
                pass

        status_code = int(context.response.status) if context.response else 0
        content_type = ""
        if context.response is not None:
            try:
                content_type = await context.response.header_value("content-type") or ""
            except Exception:
                content_type = ""
        html = ""
        if is_html_content_type(content_type) or status_code == 200:
            try:
                html = await context.page.content()
            except Exception:
                html = ""

        block_signal = detect_block_signal(
            status_code=status_code,
            content=html,
            extra_patterns=state.block_patterns,
        )
        state.mark_processed(normalized_url)
        result = state.recorder.record_result(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content=html,
            error_message="" if _usable_content(status_code, html, block_signal) else "browser_non_html_or_blocked",
            attempt_count=context.request.retry_count + 1,
            emit_result=_usable_content(status_code, html, block_signal),
            count_as_success=_usable_content(status_code, html, block_signal),
            used_browser=True,
        )

        if result is None:
            stop_reason = state.observe_failure(
                normalized_url=normalized_url,
                status_code=status_code,
                error_message="browser_non_html_or_blocked",
            )
            if state.stop_requested or stop_reason:
                crawler.stop(stop_reason or "domain_stop_requested")
            return

        state.observe_success()
        if result is None or state.remaining_total_budget <= 0:
            return

        next_items = state.enqueue_links_from_html(normalized_url, html, int(context.request.crawl_depth))
        if next_items:
            budget = max(0, min(state.remaining_browser_budget, len(next_items)))
            if budget > 0:
                await context.add_requests([_request_from_queue(item, state.cfg) for item in next_items[:budget]])

    try:
        await crawler.run([_request_from_queue(item, state.cfg) for item in initial_requests[:browser_budget]])
    finally:
        await request_queue.drop()


async def _run_http_seed_fetch_async(
    *,
    state: SeedCrawlState,
    initial_requests: list[QueueItem],
) -> None:
    await _run_http_crawl(state, initial_requests)


def run_fetch(
    con: sqlite3.Connection,
    seeds: list[DiscoverySeed],
    cfg: CrawlConfig,
    logger,
    metrics,
    job_id: str,
    max_pages_per_domain: int | None = None,
    max_total_pages: int | None = None,
    max_depth: int | None = None,
    run_state_dir: str | Path | None = None,
) -> list[FetchResult]:
    denylist = {d for d in cfg.merged_denylist() if d}
    policy_set = load_domain_policies(cfg.resolved_crawlee_domain_policies_path())
    ensure_run_control(job_id, run_state_dir)
    discovered: list[FetchResult] = []

    for seed in seeds:
        valid_seed, seed_domain_or_reason = _is_valid_seed_domain(seed)
        if not valid_seed:
            invalid_identity = seed_domain_or_reason if "." in seed_domain_or_reason else normalize_url(seed.website)
            state = load_run_control(job_id, run_state_dir)
            append_intervention(
                state,
                domain=invalid_identity,
                action="auto_quarantine_seed",
                reason=f"invalid_seed:{seed_domain_or_reason}",
                source="auto",
                details={"seed_name": seed.name, "website": seed.website},
            )
            save_run_control(state, run_state_dir)
            logger.warning(
                "Skipping invalid seed",
                extra={
                    "job_id": job_id,
                    "stage": "fetch",
                    "seed_name": seed.name,
                    "website": seed.website,
                    "reason": seed_domain_or_reason,
                },
            )
            metrics.inc("seeds_quarantined")
            continue
        seed_domain = seed_domain_or_reason
        if seed_domain in denylist:
            logger.warning(
                "Skipping denylisted domain",
                extra={"job_id": job_id, "stage": "fetch", "domain": seed_domain},
            )
            continue

        policy = policy_set.resolve(seed.website)
        if seed.browser_required and policy.mode == "http_then_browser_on_block":
            policy = replace(policy, mode="browser")
        crawl_pages = _effective_crawl_pages(cfg, policy, max_pages_per_domain)
        total_limit = _effective_total_pages(cfg, max_total_pages, crawl_pages)
        crawl_depth_limit = _effective_crawl_depth(cfg, policy, max_depth)
        browser_limit = (
            policy.max_pages_per_domain
            if policy.max_pages_per_domain is not None and policy.max_pages_per_domain > 0
            else cfg.crawlee_max_browser_pages_per_domain
        )
        browser_limit = max(1, int(browser_limit))

        recorder = SeedRunRecorder(
            con=con,
            seed=seed,
            seed_domain=seed_domain,
            job_id=job_id,
            metrics=metrics,
        )
        recorder.start()

        state = SeedCrawlState(
            con=con,
            seed=seed,
            cfg=cfg,
            policy=policy,
            metrics=metrics,
            logger=logger,
            job_id=job_id,
            denylist=denylist,
            recorder=recorder,
            crawl_pages=crawl_pages,
            total_page_limit=total_limit,
            crawl_depth=crawl_depth_limit,
            browser_page_limit=browser_limit,
            run_state_dir=run_state_dir,
        )
        try:
            initial_requests = state.seed_initial_requests()
            if initial_requests:
                try:
                    if state.policy.mode == "browser":
                        _run_browser_crawl_dispatch(state, initial_requests)
                    else:
                        _run_crawlee(_run_http_seed_fetch_async(state=state, initial_requests=initial_requests))
                        if state.browser_escalation_requested:
                            _run_browser_crawl_dispatch(state, state.remaining_queue_for_browser())
                except Exception as exc:
                    _close_crawlee_loop()
                    if isinstance(exc, sqlite3.Error):
                        raise
                    _handle_seed_crawl_exception(state, exc)
        finally:
            final_status = _seed_final_status(state)
            recorder.finalize(final_status=final_status)
            state.persist_runtime(force=True, status=final_status)
            discovered.extend(recorder.results)

    con.commit()
    return discovered
