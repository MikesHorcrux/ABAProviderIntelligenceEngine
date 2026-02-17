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
        last_status = 0
        has_success = False

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
            for attempt in range(cfg.max_retries + 1):
                try:
                    status, html = _fetch_once(normalized, cfg)
                    break
                except (HTTPError, URLError, TimeoutError):
                    metrics.inc("fetch_retries")
                    if attempt >= cfg.max_retries:
                        html = ""
                        break
                    time.sleep(cfg.retry_delay_seconds)
                except Exception:
                    html = ""
                    break

            payload_hash = _hash_content(html)
            fetched_at = utcnow_iso()
            last_status = status
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
                    "" if status == 200 else "non-200",
                    fetched_at,
                    fetched_at,
                ),
            )
            metrics.inc("pages_fetched")
            total_fetched += 1
            pages_left -= 1

            if status != 200 or not html:
                con.execute(
                    "UPDATE crawl_jobs SET last_status_code=?, updated_at=?, status='partial' WHERE crawl_job_pk=?",
                    (status, fetched_at, job_pk),
                )
                continue
            has_success = True

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
            (final_status, last_status, completed_at, completed_at, job_pk),
        )

    con.commit()
    return discovered
