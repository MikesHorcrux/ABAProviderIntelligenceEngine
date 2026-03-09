from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

from crawlee.crawlers import PlaywrightCrawler
from crawlee.storages import RequestQueue

from pipeline.config import CrawlConfig
from pipeline.fetch_backends.common import (
    detect_block_signal,
    first_positive_status_code,
    is_html_content_type,
    status_code_from_error_text,
)
from pipeline.fetch_backends.crawlee_backend import (
    LOW_VALUE_PATH_PREFIXES,
    STATIC_FILE_EXTENSIONS,
    STATIC_PATH_PREFIXES,
    _concurrency_settings,
    _header_value,
    _path_lower,
    _proxy_configuration,
    _request_from_queue,
    _storage_name,
    _usable_content,
)
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.parse import extract_links
from pipeline.utils import normalize_domain, normalize_url, resolve_link, same_domain

try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover
    PlaywrightTimeoutError = TimeoutError


@dataclass
class BrowserWorkerState:
    seed: DiscoverySeed
    cfg: CrawlConfig
    wait_for_selector: str
    block_patterns: tuple[str, ...]
    denylist: set[str]
    crawl_depth: int
    browser_budget: int
    seen_urls: set[str] = field(default_factory=set)
    processed_urls: set[str] = field(default_factory=set)
    suppressed_path_prefixes: set[str] = field(default_factory=set)
    filtered_urls: int = 0
    filtered_counts: dict[str, int] = field(default_factory=dict)
    status_hint: int = 0
    last_status_code: int = 0
    results: list[dict[str, Any]] = field(default_factory=list)

    @property
    def allowed_schemes(self) -> set[str]:
        return self.cfg.merged_schemes()

    def _reject(self, reason: str) -> None:
        self.filtered_urls += 1
        self.filtered_counts[reason] = int(self.filtered_counts.get(reason, 0)) + 1

    def _rejection_reason(self, normalized_url: str) -> str:
        path = _path_lower(normalized_url)
        for prefix in self.suppressed_path_prefixes:
            normalized_prefix = prefix.rstrip("/") if prefix != "/" else prefix
            if path == normalized_prefix or path.startswith(prefix):
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

    def queue_url(self, url: str, depth: int) -> dict[str, Any] | None:
        normalized = normalize_url(url)
        if not normalized or normalized in self.seen_urls:
            return None
        self.seen_urls.add(normalized)
        parsed = urlparse(normalized)
        if parsed.scheme.lower() not in self.allowed_schemes:
            return None
        domain = normalize_domain(normalized)
        if not domain or domain in self.denylist or not same_domain(self.seed.website, normalized):
            return None
        if depth > self.crawl_depth:
            self._reject("depth_limit")
            return None
        rejection_reason = self._rejection_reason(normalized)
        if rejection_reason:
            self._reject(rejection_reason)
            return None
        if len(self.seen_urls) > max(self.browser_budget * 2, self.browser_budget + 6):
            return None
        return {
            "requested_url": normalized,
            "normalized_url": normalized,
            "depth": depth,
        }

    def enqueue_links_from_html(self, base_url: str, html: str, depth: int) -> list[dict[str, Any]]:
        if depth >= self.crawl_depth:
            return []
        queued: list[dict[str, Any]] = []
        for link in extract_links(base_url, html):
            next_url = normalize_url(link) if link.startswith(("http://", "https://")) else resolve_link(base_url, link)
            item = self.queue_url(next_url, depth + 1)
            if item is not None:
                queued.append(item)
        return queued

    def note_status(self, status_code: int) -> None:
        code = int(status_code or 0)
        if code > 0:
            self.last_status_code = code
        if self.status_hint <= 0 and code in {401, 403, 429, 503}:
            self.status_hint = code

    def record(
        self,
        *,
        requested_url: str,
        normalized_url: str,
        status_code: int,
        content: str,
        error_message: str,
        attempt_count: int,
        emit_result: bool,
        count_as_success: bool,
    ) -> None:
        self.note_status(status_code)
        self.processed_urls.add(normalized_url)
        self.results.append(
            {
                "requested_url": requested_url,
                "normalized_url": normalized_url,
                "status_code": int(status_code or 0),
                "content": content,
                "error_message": error_message,
                "attempt_count": int(attempt_count or 1),
                "emit_result": bool(emit_result),
                "count_as_success": bool(count_as_success),
                "used_browser": True,
            }
        )


def _load_payload(path: str | Path) -> tuple[BrowserWorkerState, list[dict[str, Any]]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    state = BrowserWorkerState(
        seed=DiscoverySeed(**dict(payload.get("seed") or {})),
        cfg=CrawlConfig(**dict(payload.get("config") or {})),
        wait_for_selector=str(((payload.get("policy") or {}).get("wait_for_selector")) or ""),
        block_patterns=tuple(str(item).strip() for item in payload.get("block_patterns", []) if str(item).strip()),
        denylist={str(item).strip().lower() for item in payload.get("denylist", []) if str(item).strip()},
        crawl_depth=max(0, int(((payload.get("limits") or {}).get("crawl_depth")) or 0)),
        browser_budget=max(1, int(((payload.get("limits") or {}).get("browser_budget")) or 1)),
        seen_urls={normalize_url(str(url)) for url in payload.get("seen_urls", []) if normalize_url(str(url))},
        processed_urls={normalize_url(str(url)) for url in payload.get("processed_urls", []) if normalize_url(str(url))},
        suppressed_path_prefixes={
            str(item).strip().lower() for item in payload.get("suppressed_path_prefixes", []) if str(item).strip()
        },
    )
    initial_requests = []
    for item in payload.get("initial_requests", [])[: state.browser_budget]:
        if not isinstance(item, dict):
            continue
        initial_requests.append(
            {
                "requested_url": str(item.get("requested_url") or item.get("normalized_url") or ""),
                "normalized_url": str(item.get("normalized_url") or item.get("requested_url") or ""),
                "depth": int(item.get("depth") or 0),
            }
        )
    return state, initial_requests


async def _run_browser_worker(state: BrowserWorkerState, initial_requests: list[dict[str, Any]]) -> dict[str, Any]:
    if not initial_requests or state.browser_budget <= 0:
        return {
            "ok": True,
            "error": "",
            "status_code_hint": int(state.status_hint or 0),
            "last_status_code": int(state.last_status_code or 0),
            "filtered_urls": state.filtered_urls,
            "filtered_counts": state.filtered_counts,
            "processed_urls": sorted(state.processed_urls),
            "results": state.results,
        }

    queue_name = _storage_name("provider-intel-browser-worker", f"{normalize_domain(state.seed.website)}-{os.getpid()}")
    request_queue = await RequestQueue.open(name=queue_name)
    crawler = PlaywrightCrawler(
        request_manager=request_queue,
        browser_type=state.cfg.crawlee_browser_type,
        headless=state.cfg.crawlee_headless,
        max_requests_per_crawl=state.browser_budget,
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
            state.status_hint = 403
            normalized_url = normalize_url(url)
            if normalized_url:
                state.processed_urls.add(normalized_url)

    crawler.on_skipped_request(on_skipped_request)

    async def pre_navigation(context) -> None:
        await context.block_requests(extra_url_patterns=list(state.block_patterns))

    crawler.pre_navigation_hook(pre_navigation)

    @crawler.failed_request_handler
    async def failed_request_handler(context, error: Exception) -> None:
        requested_url = str(context.request.user_data.get("requested_url") or context.request.url)
        normalized_url = normalize_url(context.request.url)
        status_code = first_positive_status_code(
            getattr(error, "status_code", 0),
            getattr(getattr(context, "response", None), "status", 0),
            status_code_from_error_text(error),
        )
        state.record(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content="",
            error_message=str(error),
            attempt_count=context.request.retry_count + 1,
            emit_result=False,
            count_as_success=False,
        )

    @crawler.router.default_handler
    async def request_handler(context) -> None:
        requested_url = str(context.request.user_data.get("requested_url") or context.request.url)
        normalized_url = normalize_url(context.request.url)

        if state.wait_for_selector:
            try:
                await context.page.wait_for_selector(
                    state.wait_for_selector,
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
        usable = _usable_content(status_code, html, block_signal)
        state.record(
            requested_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content=html,
            error_message="" if usable else "browser_non_html_or_blocked",
            attempt_count=context.request.retry_count + 1,
            emit_result=usable,
            count_as_success=usable,
        )
        if not usable:
            return

        next_items = state.enqueue_links_from_html(normalized_url, html, int(context.request.crawl_depth))
        remaining_budget = max(0, state.browser_budget - len(state.results))
        if next_items and remaining_budget > 0:
            await context.add_requests(
                [_request_from_queue(SimpleNamespace(**item), state.cfg) for item in next_items[:remaining_budget]]
            )

    try:
        await crawler.run(
            [_request_from_queue(SimpleNamespace(**item), state.cfg) for item in initial_requests[: state.browser_budget]]
        )
        return {
            "ok": True,
            "error": "",
            "status_code_hint": int(state.status_hint or 0),
            "last_status_code": int(state.last_status_code or 0),
            "filtered_urls": state.filtered_urls,
            "filtered_counts": state.filtered_counts,
            "processed_urls": sorted(state.processed_urls),
            "results": state.results,
        }
    finally:
        await request_queue.drop()


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    if len(args) != 2:
        raise SystemExit("usage: python -m pipeline.fetch_backends.browser_worker <payload.json> <result.json>")
    payload_path = Path(args[0]).resolve()
    result_path = Path(args[1]).resolve()
    result_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        state, initial_requests = _load_payload(payload_path)
        result = asyncio.run(_run_browser_worker(state, initial_requests))
        result_path.write_text(json.dumps(result), encoding="utf-8")
        return 0
    except Exception as exc:
        error_payload = {
            "ok": False,
            "error": str(exc),
            "status_code_hint": 0,
            "last_status_code": 0,
            "filtered_urls": 0,
            "filtered_counts": {},
            "processed_urls": [],
            "results": [],
        }
        result_path.write_text(json.dumps(error_payload), encoding="utf-8")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
