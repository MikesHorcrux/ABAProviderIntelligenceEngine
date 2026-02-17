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
    discovery_seed_file: str = "discoveries.csv"
    monitor_stale_days: int = 30
    monitor_max_pages_per_domain: int = 12
    monitor_max_total_pages: int = 24
    monitor_max_depth: int = 1
    growth_max_pages_per_domain: int = 0
    growth_max_total_pages: int = 0
    growth_max_depth: int = 0
    weekly_new_lead_target: int = 100
    growth_window_days: int = 7
    enforce_growth_governor: bool = True
    seed_failure_streak_limit: int = 3
    seed_backoff_hours: int = 24
    cache_ttl_hours: int = 24
    require_fetch_success_gate: bool = True
    output_stale_hours: int = 72
    retry_base_delay_seconds: float = 1.25
    retry_factor: float = 1.9
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


def _coerce_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return default
    return text not in {"0", "false", "no", "off", "n"}


def load_crawl_config(path: str | Path | None = None) -> CrawlConfig:
    defaults = CrawlConfig()
    cfg_path = Path(path or os.environ.get("CANNARADAR_CRAWLER_CONFIG") or DEFAULT_CONFIG_PATH)
    default_seed = os.environ.get("CANNARADAR_SEED_FILE", defaults.seed_file)
    discovery_seed = os.environ.get("CANNARADAR_DISCOVERY_FILE", defaults.discovery_seed_file)
    seed_failure_streak_limit = os.environ.get("CANNARADAR_SEED_FAILURE_STREAK_LIMIT")
    seed_backoff_hours = os.environ.get("CANNARADAR_SEED_BACKOFF_HOURS")
    weekly_new_lead_target = os.environ.get("CANNARADAR_WEEKLY_NEW_LEAD_TARGET")
    growth_window_days = os.environ.get("CANNARADAR_GROWTH_WINDOW_DAYS")
    enforce_growth_governor = os.environ.get("CANNARADAR_ENFORCE_GROWTH_GOVERNOR")
    require_fetch_success_gate = os.environ.get("CANNARADAR_REQUIRE_FETCH_SUCCESS_GATE")
    output_stale_hours = os.environ.get("CANNARADAR_OUTPUT_STALE_HOURS")
    retry_base_delay_seconds = os.environ.get("CANNARADAR_RETRY_BASE_DELAY_SECONDS")
    retry_factor = os.environ.get("CANNARADAR_RETRY_FACTOR")

    if not cfg_path.exists():
        return CrawlConfig(seed_file=default_seed)

    with cfg_path.open() as f:
        data = json.loads(f.read())

    return CrawlConfig(
        user_agent=str(data.get("userAgent", defaults.user_agent)),
        timeout_seconds=float(data.get("timeoutSeconds", defaults.timeout_seconds)),
        max_retries=int(data.get("maxRetries", defaults.max_retries)),
        retry_delay_seconds=float(data.get("retryDelaySeconds", defaults.retry_delay_seconds)),
        crawl_delay_seconds=float(data.get("crawlDelaySeconds", defaults.crawl_delay_seconds)),
        max_depth=int(data.get("maxDepth", defaults.max_depth)),
        max_pages_per_domain=int(data.get("maxPagesPerDomain", defaults.max_pages_per_domain)),
        max_total_pages=data.get("maxTotalPages"),
        respect_robots=bool(data.get("respectRobots", defaults.respect_robots)),
        allowed_schemes=_coalesce_list(data.get("allowedSchemes"), defaults.allowed_schemes),
        denylist=_coalesce_list(data.get("denylist"), defaults.denylist),
        seed_file=str(data.get("seedFile", default_seed)),
        discovery_seed_file=str(data.get("discoverySeedFile", discovery_seed)),
        monitor_stale_days=int(data.get("monitorStaleDays", defaults.monitor_stale_days)),
        monitor_max_pages_per_domain=int(
            data.get("monitorMaxPagesPerDomain", defaults.monitor_max_pages_per_domain)
        ),
        monitor_max_total_pages=int(data.get("monitorMaxTotalPages", defaults.monitor_max_total_pages)),
        monitor_max_depth=int(data.get("monitorMaxDepth", defaults.monitor_max_depth)),
        growth_max_pages_per_domain=int(data.get("growthMaxPagesPerDomain", defaults.growth_max_pages_per_domain)),
        growth_max_total_pages=int(data.get("growthMaxTotalPages", defaults.growth_max_total_pages)),
        growth_max_depth=int(data.get("growthMaxDepth", defaults.growth_max_depth)),
        seed_failure_streak_limit=int(seed_failure_streak_limit or data.get("seedFailureStreakLimit", defaults.seed_failure_streak_limit)),
        seed_backoff_hours=int(seed_backoff_hours or data.get("seedBackoffHours", defaults.seed_backoff_hours)),
        weekly_new_lead_target=int(weekly_new_lead_target or data.get("weeklyNewLeadTarget", defaults.weekly_new_lead_target)),
        growth_window_days=int(growth_window_days or data.get("growthWindowDays", defaults.growth_window_days)),
        enforce_growth_governor=_coerce_bool(
            enforce_growth_governor,
            _coerce_bool(
                data.get("enforceGrowthGovernor"),
                defaults.enforce_growth_governor,
            ),
        ),
        require_fetch_success_gate=_coerce_bool(
            require_fetch_success_gate,
            _coerce_bool(
                data.get("requireFetchSuccessGate"),
                defaults.require_fetch_success_gate,
            ),
        ),
        output_stale_hours=int(output_stale_hours or data.get("outputStaleHours", defaults.output_stale_hours)),
        retry_base_delay_seconds=float(retry_base_delay_seconds or data.get("retryBaseDelaySeconds", defaults.retry_base_delay_seconds)),
        retry_factor=float(retry_factor or data.get("retryFactor", defaults.retry_factor)),
        cache_ttl_hours=int(data.get("cacheTtlHours", defaults.cache_ttl_hours)),
        per_domain_min_interval_seconds=float(
            data.get("perDomainMinIntervalSeconds", defaults.per_domain_min_interval_seconds)
        ),
        extra_paths=_coalesce_list(data.get("extraPaths"), defaults.extra_paths),
        role_keywords=_coalesce_list(data.get("roleKeywords"), defaults.role_keywords),
        max_concurrency=max(1, int(data.get("maxConcurrency", defaults.max_concurrency))),
    )


def discover_seed_paths(cfg: CrawlConfig) -> list[str]:
    paths = [cfg.seed_file]
    if cfg.discovery_seed_file:
        paths.append(cfg.discovery_seed_file)
    return paths
