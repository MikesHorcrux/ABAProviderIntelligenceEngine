from __future__ import annotations

import hashlib
import html
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from pipeline.config import CrawlConfig
from pipeline.stages.discovery import DiscoverySeed
from pipeline.utils import make_pk, utcnow_iso


BLOCKED_STATUS_CODES = frozenset({401, 403, 429, 503})
DEFAULT_BLOCK_MARKERS = (
    "captcha",
    "verify you are human",
    "access denied",
    "attention required",
    "cloudflare",
    "akamai",
)
HTML_CONTENT_TYPES = ("text/html", "application/xhtml+xml")
_HTML_COMMENT_RE = re.compile(r"(?is)<!--.*?-->")
_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style|noscript)\b[^>]*>.*?</\1>")
_HTML_TAG_RE = re.compile(r"(?is)<[^>]+>")
_STATUS_CODE_RE = re.compile(r"\bstatus code[: ]+(\d{3})\b", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class FetchResult:
    job_pk: str
    seed_name: str
    seed_state: str
    seed_market: str
    seed_website: str
    target_url: str
    normalized_url: str
    status_code: int
    content: str
    content_hash: str
    fetched_at: str


@dataclass(frozen=True)
class QueueItem:
    requested_url: str
    normalized_url: str
    depth: int


@dataclass(frozen=True)
class BlockSignal:
    triggered: bool
    reason: str = ""


def hash_content(html: str) -> str:
    return hashlib.sha256((html or "").encode("utf-8", errors="ignore")).hexdigest()


def status_class(status: int | None) -> str:
    if not status:
        return "network_error"
    if 200 <= status < 300:
        return "success"
    if status == 429:
        return "throttle"
    if 300 <= status <= 399:
        return "redirect"
    if 500 <= status <= 599:
        return "server_error"
    if 400 <= status <= 499:
        return "client_error"
    return "unknown_error"


def is_html_content_type(content_type: str | None) -> bool:
    lowered = (content_type or "").lower()
    return any(marker in lowered for marker in HTML_CONTENT_TYPES)


def first_positive_status_code(*values: object) -> int:
    for value in values:
        try:
            candidate = int(value or 0)
        except (TypeError, ValueError):
            continue
        if candidate > 0:
            return candidate
    return 0


def status_code_from_error_text(error: object) -> int:
    match = _STATUS_CODE_RE.search(str(error or ""))
    if not match:
        return 0
    return int(match.group(1))


def block_detection_text(content: str) -> str:
    text = content or ""
    if "<" in text and ">" in text:
        text = _HTML_COMMENT_RE.sub(" ", text)
        text = _SCRIPT_STYLE_RE.sub(" ", text)
        text = _HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return _WHITESPACE_RE.sub(" ", text).strip().lower()


def detect_block_signal(
    *,
    status_code: int,
    content: str,
    extra_patterns: tuple[str, ...] | list[str] | None = None,
) -> BlockSignal:
    if status_code in BLOCKED_STATUS_CODES:
        return BlockSignal(True, f"status:{status_code}")

    lowered = block_detection_text(content)
    for marker in (*DEFAULT_BLOCK_MARKERS, *(extra_patterns or ())):
        needle = str(marker).strip().lower()
        if needle and needle in lowered:
            return BlockSignal(True, f"marker:{needle}")

    return BlockSignal(False, "")


def already_fetched_recently(con: sqlite3.Connection, normalized_url: str, cfg: CrawlConfig) -> bool:
    if cfg.cache_ttl_hours <= 0:
        return False
    cutoff = (datetime.utcnow() - timedelta(hours=cfg.cache_ttl_hours)).isoformat()
    row = con.execute(
        """
        SELECT fetched_at
        FROM crawl_results
        WHERE target_url = ? AND fetched_at >= ?
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (normalized_url, cutoff),
    ).fetchone()
    return row is not None


def _upsert_seed_telemetry(
    con: sqlite3.Connection,
    *,
    seed_domain: str,
    seed_name: str,
    run_job_pk: str,
    run_started_at: str,
    run_completed_at: str,
    run_status: str,
    last_status_code: int,
    run_attempts: int,
    run_success_pages: int,
    run_failure_pages: int,
    run_pages_fetched: int,
) -> None:
    con.execute(
        """
        INSERT INTO seed_telemetry (
            seed_domain, seed_name, attempts, successes, failures,
            success_runs, failure_runs, consecutive_failures, last_status_code,
            last_success_at, last_failure_at, last_run_started_at, last_run_completed_at,
            last_run_status, last_run_pages_fetched, last_run_success_pages, last_run_failure_pages,
            last_run_job_pk, created_at, updated_at, deleted_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(seed_domain) DO UPDATE SET
            seed_name = excluded.seed_name,
            attempts = attempts + excluded.attempts,
            successes = successes + excluded.successes,
            failures = failures + excluded.failures,
            success_runs = success_runs + excluded.success_runs,
            failure_runs = failure_runs + excluded.failure_runs,
            consecutive_failures = CASE
                WHEN excluded.successes > 0 THEN 0
                ELSE consecutive_failures + excluded.failure_runs
            END,
            last_status_code = excluded.last_status_code,
            last_success_at = CASE
                WHEN excluded.last_success_at <> '' THEN excluded.last_success_at
                ELSE last_success_at
            END,
            last_failure_at = CASE
                WHEN excluded.last_failure_at <> '' THEN excluded.last_failure_at
                ELSE last_failure_at
            END,
            last_run_started_at = excluded.last_run_started_at,
            last_run_completed_at = excluded.last_run_completed_at,
            last_run_status = excluded.last_run_status,
            last_run_pages_fetched = excluded.last_run_pages_fetched,
            last_run_success_pages = excluded.last_run_success_pages,
            last_run_failure_pages = excluded.last_run_failure_pages,
            last_run_job_pk = excluded.last_run_job_pk,
            updated_at = excluded.updated_at
        """,
        (
            seed_domain,
            seed_name,
            run_attempts,
            run_success_pages,
            run_failure_pages,
            1 if run_success_pages > 0 else 0,
            1 if run_status in {"partial", "empty", "failed"} else 0,
            0,
            last_status_code,
            run_completed_at if run_success_pages > 0 else "",
            run_completed_at if run_status in {"partial", "empty", "failed"} else "",
            run_started_at,
            run_completed_at,
            run_status,
            run_pages_fetched,
            run_success_pages,
            run_failure_pages,
            run_job_pk,
            run_completed_at,
            run_completed_at,
            "",
        ),
    )


class SeedRunRecorder:
    def __init__(
        self,
        *,
        con: sqlite3.Connection,
        seed: DiscoverySeed,
        seed_domain: str,
        job_id: str,
        metrics,
    ) -> None:
        self.con = con
        self.seed = seed
        self.seed_domain = seed_domain
        self.job_id = job_id
        self.metrics = metrics
        self.job_pk = make_pk("job", [seed.website, job_id, seed_domain])
        self.started_at = utcnow_iso()
        self.last_status_code = 0
        self.run_attempts = 0
        self.run_success_pages = 0
        self.run_failure_pages = 0
        self.run_pages_fetched = 0
        self.has_success = False
        self.results: list[FetchResult] = []
        self._status_hint = 0

    def start(self) -> None:
        self.con.execute(
            """
            INSERT OR REPLACE INTO crawl_jobs (
                crawl_job_pk, seed_name, seed_domain, status, mode, last_status_code,
                started_at, completed_at, created_at, updated_at, deleted_at
            ) VALUES (?,?,?,'running','seed',0,?,?,?,?,'')
            """,
            (
                self.job_pk,
                self.seed.name,
                self.seed_domain,
                self.started_at,
                None,
                self.started_at,
                self.started_at,
            ),
        )

    def record_result(
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
        used_browser: bool = False,
    ) -> FetchResult | None:
        fetched_at = utcnow_iso()
        payload_hash = hash_content(content)
        self.con.execute(
            """
            INSERT OR REPLACE INTO crawl_results (
                crawl_result_pk, crawl_job_pk, requested_url, target_url, status_code,
                content_hash, content, fetched_at, error_message, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                make_pk("cr", [self.seed.website, normalized_url, payload_hash or fetched_at]),
                self.job_pk,
                requested_url,
                normalized_url,
                status_code,
                payload_hash,
                content,
                fetched_at,
                error_message,
                fetched_at,
                fetched_at,
            ),
        )

        attempt_total = max(1, int(attempt_count))
        self.run_attempts += attempt_total
        self.run_pages_fetched += 1
        self.last_status_code = status_code
        self.metrics.inc("pages_fetched")
        self.metrics.inc("pages_browser_fetched" if used_browser else "pages_http_fetched")

        retries = max(0, attempt_total - 1)
        if retries:
            self.metrics.inc(f"fetch_retries_{status_class(status_code)}", retries)

        if count_as_success:
            self.run_success_pages += 1
            self.has_success = True
        else:
            self.run_failure_pages += 1
            self.con.execute(
                "UPDATE crawl_jobs SET last_status_code=?, updated_at=?, status='partial' WHERE crawl_job_pk=?",
                (status_code, fetched_at, self.job_pk),
            )

        if not emit_result:
            return None

        result = FetchResult(
            job_pk=self.job_pk,
            seed_name=self.seed.name,
            seed_state=self.seed.state,
            seed_market=self.seed.market,
            seed_website=self.seed.website,
            target_url=requested_url,
            normalized_url=normalized_url,
            status_code=status_code,
            content=content,
            content_hash=payload_hash,
            fetched_at=fetched_at,
        )
        self.results.append(result)
        return result

    def note_status_hint(self, status_code: int) -> None:
        code = int(status_code or 0)
        if code <= 0:
            return
        self._status_hint = code
        if self.last_status_code <= 0:
            self.last_status_code = code

    def finalize(self) -> None:
        completed_at = utcnow_iso()
        last_status_code = self.last_status_code or self._status_hint
        final_status = "completed" if self.has_success else ("partial" if self.run_pages_fetched else "empty")
        self.con.execute(
            "UPDATE crawl_jobs SET status=?, last_status_code=?, completed_at=?, updated_at=? WHERE crawl_job_pk=?",
            (final_status, last_status_code, completed_at, completed_at, self.job_pk),
        )
        _upsert_seed_telemetry(
            self.con,
            seed_domain=self.seed_domain,
            seed_name=self.seed.name,
            run_job_pk=self.job_pk,
            run_started_at=self.started_at,
            run_completed_at=completed_at,
            run_status=final_status,
            last_status_code=last_status_code,
            run_attempts=self.run_attempts,
            run_success_pages=self.run_success_pages,
            run_failure_pages=self.run_failure_pages,
            run_pages_fetched=self.run_pages_fetched,
        )
