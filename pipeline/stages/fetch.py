from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib import robotparser

import sqlite3

from pipeline.config import CrawlConfig
from pipeline.stages.discovery import DiscoverySeed
from pipeline.utils import make_pk, normalize_domain, normalize_url, same_domain, utcnow_iso, resolve_link


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


def _hash_content(html: str) -> str:
    return hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()


def _status_class(status: int | None) -> str:
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


def _retry_wait_seconds(base_delay: float, attempt: int, status_class: str, cfg: CrawlConfig) -> float:
    if status_class in {"client_error", "redirect"}:
        return min(base_delay * 0.75, cfg.retry_delay_seconds)
    return base_delay * (cfg.retry_factor ** attempt)


def _is_retryable(status_class: str) -> bool:
    return status_class in {"server_error", "throttle", "network_error", "unknown_error"}


def _read_robots(base: str, cfg: CrawlConfig, user_agent: str) -> robotparser.RobotFileParser | None:
    if not cfg.respect_robots:
        return None
    rp = robotparser.RobotFileParser()
    robots_url = normalize_url(base).rstrip("/") + "/robots.txt"
    try:
        with urlopen(Request(robots_url, headers={"User-Agent": user_agent}), timeout=cfg.timeout_seconds) as r:
            rp.parse((r.read().decode("utf-8", errors="ignore")).splitlines())
    except Exception:
        return None
    return rp


def _can_fetch(rp: robotparser.RobotFileParser | None, url: str, user_agent: str) -> bool:
    if rp is None:
        return True
    try:
        return rp.can_fetch(user_agent, url)
    except Exception:
        return True


def _fetch_once(url: str, cfg: CrawlConfig) -> tuple[int, str]:
    req = Request(url, headers={"User-Agent": cfg.user_agent})
    with urlopen(req, timeout=cfg.timeout_seconds) as r:
        status = int(r.status)
        content_type = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in content_type:
            return status, ""
        payload = r.read().decode("utf-8", errors="ignore")
        return status, payload


def _sleep_for_domain(last_fetch_by_domain: dict[str, float], domain: str, cfg: CrawlConfig) -> None:
    now = time.time()
    last = last_fetch_by_domain.get(domain, 0.0)
    required = max(cfg.crawl_delay_seconds, cfg.per_domain_min_interval_seconds)
    elapsed = now - last
    if elapsed < required:
        time.sleep(required - elapsed)
    last_fetch_by_domain[domain] = time.time()


def _already_fetched_recently(con: sqlite3.Connection, normalized_url: str, cfg: CrawlConfig) -> bool:
    if cfg.cache_ttl_hours <= 0:
        return False
    cutoff = (datetime.utcnow() - timedelta(hours=cfg.cache_ttl_hours)).isoformat()
    row = con.execute(
        "SELECT fetched_at FROM crawl_results WHERE target_url=? AND fetched_at >= ? ORDER BY fetched_at DESC LIMIT 1",
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
) -> list[FetchResult]:
    denylist = {d for d in cfg.merged_denylist() if d}
    discovered: list[FetchResult] = []
    stage = "fetch"
    last_domain_hits: dict[str, float] = {}

    for seed in seeds:
        start_domain = normalize_domain(seed.website)
        if start_domain and start_domain in denylist:
            logger.warning("Skipping denylisted domain", extra={"job_id": job_id, "stage": stage, "domain": start_domain})
            continue
        if not start_domain:
            continue

        job_pk = make_pk("job", [seed.website, job_id, str(len(discovered))])
        started_at = utcnow_iso()
        con.execute(
            "INSERT OR REPLACE INTO crawl_jobs (crawl_job_pk, seed_name, seed_domain, status, mode, last_status_code, started_at, completed_at, created_at, updated_at, deleted_at) VALUES (?,?,?,'running','seed',0,?,?,?,?,'')",
            (job_pk, seed.name, start_domain, started_at, None, started_at, started_at),
        )

        rp = _read_robots(seed.website, cfg, cfg.user_agent)
        queue = [seed.website]
        seen_urls = {seed.website}
        pages_left = cfg.max_pages_per_domain if cfg.max_pages_per_domain > 0 else 30
        extra_paths = list(cfg.extra_paths)
        crawl_pages = max_pages_per_domain if max_pages_per_domain and max_pages_per_domain > 0 else cfg.max_pages_per_domain
        crawl_pages = crawl_pages if crawl_pages > 0 else 30
        crawl_total = (
            max_total_pages
            if max_total_pages and max_total_pages > 0
            else (cfg.max_total_pages if cfg.max_total_pages and cfg.max_total_pages > 0 else crawl_pages)
        )
        crawl_depth = max_depth if max_depth is not None and max_depth >= 0 else cfg.max_depth
        crawl_depth = min(max(crawl_depth, 0), 10)

        pages_left = crawl_pages
        for path in extra_paths:
            candidate = normalize_url(f"{seed.website.rstrip('/')}{path}")
            if candidate not in seen_urls:
                queue.append(candidate)
                seen_urls.add(candidate)

        max_total = crawl_total
        total_fetched = 0
        has_success = False

        run_attempts = 0
        run_success_pages = 0
        run_failure_pages = 0
        run_pages_fetched = 0

        queue_depths: list[tuple[str, int]] = [(url, 0) for url in queue]

        while queue_depths and total_fetched < max_total and pages_left > 0:
            url, depth = queue_depths.pop(0)
            if not _can_fetch(rp, url, cfg.user_agent):
                logger.warning("Robots blocked", extra={"job_id": job_id, "stage": stage, "url": url})
                continue

            normalized = normalize_url(url)
            domain = normalize_domain(normalized)
            if domain in denylist:
                continue
            if not same_domain(seed.website, normalized):
                continue

            if _already_fetched_recently(con, normalized, cfg):
                metrics.inc("pages_cached")
                continue

            _sleep_for_domain(last_domain_hits, domain, cfg)

            status = 0
            html = ""
            last_error = "non-200"
            for attempt in range(cfg.max_retries + 1):
                run_attempts += 1
                try:
                    status, html = _fetch_once(normalized, cfg)
                    status_class = _status_class(status)
                    last_error = "" if status_class == "success" else status_class
                    if status_class == "success":
                        break
                    if not _is_retryable(status_class):
                        break
                except HTTPError as exc:
                    status = int(exc.code or 0)
                    status_class = _status_class(status)
                    last_error = str(exc)
                    if not _is_retryable(status_class):
                        break
                except (URLError, TimeoutError) as exc:
                    status = 0
                    status_class = _status_class(status)
                    last_error = str(exc)
                    if not _is_retryable(status_class):
                        break
                except Exception as exc:
                    status = 0
                    status_class = "unknown_error"
                    last_error = str(exc)
                    break

                if attempt >= cfg.max_retries:
                    break
                metrics.inc(f"fetch_retries_{status_class}")
                time.sleep(_retry_wait_seconds(cfg.retry_base_delay_seconds, attempt, status_class, cfg))

            payload_hash = _hash_content(html)
            fetched_at = utcnow_iso()
            con.execute(
                "INSERT INTO crawl_results (crawl_result_pk, crawl_job_pk, requested_url, target_url, status_code, content_hash, content, fetched_at, error_message, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    make_pk("cr", [seed.website, normalized, fetched_at]),
                    job_pk,
                    url,
                    normalized,
                    status,
                    payload_hash,
                    html,
                    fetched_at,
                    "" if status == 200 else last_error,
                    fetched_at,
                    fetched_at,
                ),
            )
            metrics.inc("pages_fetched")
            total_fetched += 1
            pages_left -= 1
            run_pages_fetched = total_fetched

            if status == 200 and html:
                run_success_pages += 1
                has_success = True
            else:
                run_failure_pages += 1

            if status != 200 or not html:
                con.execute(
                    "UPDATE crawl_jobs SET last_status_code=?, updated_at=?, status='partial' WHERE crawl_job_pk=?",
                    (status, fetched_at, job_pk),
                )
                continue

            result = FetchResult(
                job_pk=job_pk,
                seed_name=seed.name,
                seed_state=seed.state,
                seed_market=seed.market,
                seed_website=seed.website,
                target_url=url,
                normalized_url=normalized,
                status_code=status,
                content=html,
                content_hash=payload_hash,
                fetched_at=fetched_at,
            )
            discovered.append(result)

            from pipeline.stages.parse import extract_links

            for link in extract_links(normalized, html):
                if not link:
                    continue
                if link.startswith("http://") or link.startswith("https://"):
                    next_url = normalize_url(link)
                else:
                    next_url = resolve_link(normalized, link)
                if not next_url.startswith("http"):
                    continue
                if not same_domain(seed.website, next_url):
                    continue
                if next_url in seen_urls:
                    continue
                next_depth = depth + 1
                if next_depth > crawl_depth:
                    continue
                if len(seen_urls) > crawl_pages * 2:
                    continue
                seen_urls.add(next_url)
                queue_depths.append((next_url, next_depth))

        completed_at = utcnow_iso()
        final_status = "completed" if has_success else ("partial" if total_fetched else "empty")
        con.execute(
            "UPDATE crawl_jobs SET status=?, last_status_code=?, completed_at=?, updated_at=? WHERE crawl_job_pk=?",
            (final_status, status, completed_at, completed_at, job_pk),
        )
        _upsert_seed_telemetry(
            con,
            seed_domain=start_domain,
            seed_name=seed.name,
            run_job_pk=job_pk,
            run_started_at=started_at,
            run_completed_at=completed_at,
            run_status=final_status,
            last_status_code=status,
            run_attempts=run_attempts,
            run_success_pages=run_success_pages,
            run_failure_pages=run_failure_pages,
            run_pages_fetched=run_pages_fetched,
        )

    con.commit()
    return discovered
