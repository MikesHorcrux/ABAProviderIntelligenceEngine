from __future__ import annotations

import asyncio
import atexit
import time
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any
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
                domains = state.setdefault("agent_controls", {}).setdefault("domains", {})
                record = dict(domains.get(self.domain) or {})
                prefixes = {
                    str(item).strip().lower()
                    for item in record.get("suppressed_path_prefixes", [])
                    if str(item).strip()
                }
                prefixes.add(prefix)
                record["suppressed_path_prefixes"] = sorted(prefixes)
                record["updated_at"] = utcnow_iso()
                domains[self.domain] = record
                append_intervention(
                    state,
                    domain=self.domain,
                    action="auto_suppress_prefix",
                    reason=f"{failure_kind}_storm",
                    source="auto",
                    details={"prefix": prefix, "failures": self.prefix_failure_counts[prefix]},
                )
                save_run_control(state, self.run_state_dir)
                self.manual_suppressed_path_prefixes = prefixes
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
        for path in self.cfg.extra_paths:
            candidate = normalize_url(f"{self.seed.website.rstrip('/')}{path}")
            item = self.queue_url(candidate, 0)
            if item is not None:
                items.append(item)
        return items

    def enqueue_links_from_html(self, base_url: str, html: str, depth: int) -> list[QueueItem]:
        if depth >= self.crawl_depth or not self.discovery_enabled or self.stop_requested:
            return []
        queued: list[QueueItem] = []
        for link in extract_links(base_url, html):
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


async def _run_http_crawl(state: SeedCrawlState, initial_requests: list[QueueItem]) -> None:
    if not initial_requests or state.remaining_total_budget <= 0:
        return

    state.persist_runtime(force=True, status="running")
    request_queue = await RequestQueue.open(name=_storage_name("cannaradar-http", state.recorder.job_pk))
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
    request_queue = await RequestQueue.open(name=_storage_name("cannaradar-browser", state.recorder.job_pk))
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


async def _run_seed_fetch_async(
    *,
    state: SeedCrawlState,
    initial_requests: list[QueueItem],
) -> None:
    if state.policy.mode == "browser":
        await _run_browser_crawl(state, initial_requests)
        return

    await _run_http_crawl(state, initial_requests)
    if state.browser_escalation_requested:
        await _run_browser_crawl(state, state.remaining_queue_for_browser())


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
        initial_requests = state.seed_initial_requests()

        if initial_requests:
            _run_crawlee(_run_seed_fetch_async(state=state, initial_requests=initial_requests))

        recorder.finalize()
        final_status = "completed" if recorder.has_success else ("quarantined" if state.seed_quarantined else ("stopped" if state.stop_requested else ("partial" if recorder.run_pages_fetched else "empty")))
        state.persist_runtime(force=True, status=final_status)
        discovered.extend(recorder.results)

    con.commit()
    return discovered
