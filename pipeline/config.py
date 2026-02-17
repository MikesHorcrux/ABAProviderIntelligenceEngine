from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "crawler_config.json"


@dataclass
class CrawlConfig:
    user_agent: str = "LunaLeadCrawler/3.0 (+local; production-grade)"
    timeout_seconds: float = 8.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.5
    crawl_delay_seconds: float = 0.4
    max_depth: int = 2
    max_pages_per_domain: int = 30
    max_total_pages: int | None = None
    respect_robots: bool = True
    allowed_schemes: list[str] = field(default_factory=lambda: ["http", "https"])
    denylist: list[str] = field(default_factory=list)
    seed_file: str = "seeds.csv"
    cache_ttl_hours: int = 24
    per_domain_min_interval_seconds: float = 1.25
    extra_paths: list[str] = field(
        default_factory=lambda: ["/about", "/team", "/contact", "/menu", "/locations"]
    )
    role_keywords: list[str] = field(
        default_factory=lambda: [
            "owner",
            "co-founder",
            "founder",
            "general manager",
            "gm",
            "inventory manager",
            "buyer",
            "purchasing",
            "operator",
            "store manager",
            "director of operations",
        ]
    )
    max_concurrency: int = 1

    def merged_denylist(self) -> set[str]:
        values = list(self.denylist)
        env_denylist = os.environ.get("CANNARADAR_DENYLIST")
        if env_denylist:
            values.extend([x.strip() for x in env_denylist.split(",")])
        return {str(v).strip().lower().lstrip(".") for v in values if str(v).strip()}

    def merged_schemes(self) -> set[str]:
        return {str(s).strip().lower() for s in self.allowed_schemes if str(s).strip()}


def _coalesce_list(value: object, default: list[str]) -> list[str]:
    if not isinstance(value, list):
        return default.copy()
    return [str(v).strip() for v in value if str(v).strip()]


def load_crawl_config(path: str | Path | None = None) -> CrawlConfig:
    cfg_path = Path(path or os.environ.get("CANNARADAR_CRAWLER_CONFIG") or DEFAULT_CONFIG_PATH)
    default_seed = os.environ.get("CANNARADAR_SEED_FILE", CrawlConfig.seed_file)

    if not cfg_path.exists():
        return CrawlConfig(seed_file=default_seed)

    with cfg_path.open() as f:
        data = json.loads(f.read())

    return CrawlConfig(
        user_agent=str(data.get("userAgent", CrawlConfig.user_agent)),
        timeout_seconds=float(data.get("timeoutSeconds", CrawlConfig.timeout_seconds)),
        max_retries=int(data.get("maxRetries", CrawlConfig.max_retries)),
        retry_delay_seconds=float(data.get("retryDelaySeconds", CrawlConfig.retry_delay_seconds)),
        crawl_delay_seconds=float(data.get("crawlDelaySeconds", CrawlConfig.crawl_delay_seconds)),
        max_depth=int(data.get("maxDepth", CrawlConfig.max_depth)),
        max_pages_per_domain=int(data.get("maxPagesPerDomain", CrawlConfig.max_pages_per_domain)),
        max_total_pages=data.get("maxTotalPages"),
        respect_robots=bool(data.get("respectRobots", CrawlConfig.respect_robots)),
        allowed_schemes=_coalesce_list(data.get("allowedSchemes"), CrawlConfig.allowed_schemes),
        denylist=_coalesce_list(data.get("denylist"), CrawlConfig.denylist),
        seed_file=str(data.get("seedFile", default_seed)),
        cache_ttl_hours=int(data.get("cacheTtlHours", CrawlConfig.cache_ttl_hours)),
        per_domain_min_interval_seconds=float(
            data.get("perDomainMinIntervalSeconds", CrawlConfig.per_domain_min_interval_seconds)
        ),
        extra_paths=_coalesce_list(data.get("extraPaths"), CrawlConfig.extra_paths),
        role_keywords=_coalesce_list(data.get("roleKeywords"), CrawlConfig.role_keywords),
        max_concurrency=max(1, int(data.get("maxConcurrency", CrawlConfig.max_concurrency))),
    )


def discover_seed_paths(cfg: CrawlConfig) -> list[str]:
    return [cfg.seed_file]
