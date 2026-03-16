from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "crawler_config.json"


def _env_value(*names: str) -> str | None:
    for name in names:
        if name in os.environ:
            return os.environ.get(name)
    return None


@dataclass
class CrawlConfig:
    user_agent: str = "ABAProviderIntelligenceEngine/1.0 (+local; evidence-first)"
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
    seed_file: str = "seed_packs/nj/seed_pack.json"
    discovery_seed_file: str = ""
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
    require_net_new_gate: bool = True
    fail_on_zero_new_leads: bool = False
    output_stale_hours: int = 72
    agent_research_enabled: bool = True
    agent_research_limit: int = 25
    agent_research_min_score: int = 48
    agent_research_paths: list[str] = field(
        default_factory=lambda: [
            "/search",
            "/directory",
            "/find-a-provider",
            "/find-a-doctor",
            "/provider-directory",
            "/verify-a-license",
            "/license-verification",
            "/license-lookup",
            "/physician-search",
            "/about",
            "/providers",
            "/team",
            "/staff",
            "/diagnosis",
            "/evaluations",
            "/evaluation",
            "/testing",
            "/autism",
            "/adhd",
            "/intake",
            "/referrals",
            "/contact",
            "/locations",
            "/telehealth",
            "/insurance",
        ]
    )
    retry_base_delay_seconds: float = 1.25
    retry_factor: float = 1.9
    per_domain_min_interval_seconds: float = 1.25
    extra_paths: list[str] = field(
        default_factory=lambda: ["/about", "/providers", "/team", "/diagnosis", "/evaluation", "/contact", "/locations", "/telehealth"]
    )
    role_keywords: list[str] = field(
        default_factory=lambda: [
            "psy.d",
            "ph.d",
            "psychologist",
            "psychiatrist",
            "developmental pediatrician",
            "physician assistant",
            "nurse practitioner",
            "apn",
            "np",
            "md",
            "do",
        ]
    )
    max_concurrency: int = 1
    crawlee_headless: bool = True
    crawlee_browser_type: str = "chromium"
    crawlee_proxy_urls: list[str] = field(default_factory=list)
    crawlee_use_session_pool: bool = True
    crawlee_retry_on_blocked: bool = True
    crawlee_max_session_rotations: int = 8
    crawlee_viewport_width: int = 1280
    crawlee_viewport_height: int = 1024
    crawlee_max_browser_pages_per_domain: int = 5
    crawlee_browser_isolation: str = "subprocess" if sys.platform == "darwin" else "inline"
    crawlee_extra_block_patterns: list[str] = field(default_factory=list)
    crawlee_domain_policies_file: str = "fetch_policies.json"
    config_path: str = str(DEFAULT_CONFIG_PATH)

    def merged_denylist(self) -> set[str]:
        values = list(self.denylist)
        env_denylist = _env_value("PROVIDER_INTEL_DENYLIST")
        if env_denylist:
            values.extend([x.strip() for x in env_denylist.split(",")])
        return {str(v).strip().lower().lstrip(".") for v in values if str(v).strip()}

    def merged_schemes(self) -> set[str]:
        return {str(s).strip().lower() for s in self.allowed_schemes if str(s).strip()}

    def resolve_runtime_path(self, value: str) -> Path:
        candidate = Path(value)
        if candidate.is_absolute():
            return candidate.resolve()
        return (Path(self.config_path).resolve().parent / candidate).resolve()

    def resolved_crawlee_domain_policies_path(self) -> Path:
        return self.resolve_runtime_path(self.crawlee_domain_policies_file)


def _coalesce_list(value: object, default: list[str]) -> list[str]:
    if not isinstance(value, list):
        return default.copy()
    return [str(v).strip() for v in value if str(v).strip()]


def _coalesce_csv_list(value: object, default: list[str]) -> list[str]:
    if value is None:
        return default.copy()
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return [chunk.strip() for chunk in str(value).split(",") if chunk.strip()]


def _coerce_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return default
    return text not in {"0", "false", "no", "off", "n"}


def _coerce_optional_int(value: object, default: int | None) -> int | None:
    if value is None or str(value).strip() == "":
        return default
    return int(value)


def load_crawl_config(path: str | Path | None = None) -> CrawlConfig:
    defaults = CrawlConfig()
    cfg_path = Path(
        path
        or _env_value("PROVIDER_INTEL_CONFIG", "PROVIDER_INTEL_CRAWLER_CONFIG")
        or DEFAULT_CONFIG_PATH
    ).resolve()
    default_seed = _env_value("PROVIDER_INTEL_SEED_FILE") or defaults.seed_file
    discovery_seed = _env_value("PROVIDER_INTEL_DISCOVERY_FILE") or defaults.discovery_seed_file
    seed_failure_streak_limit = _env_value("PROVIDER_INTEL_SEED_FAILURE_STREAK_LIMIT")
    seed_backoff_hours = _env_value("PROVIDER_INTEL_SEED_BACKOFF_HOURS")
    weekly_new_lead_target = _env_value("PROVIDER_INTEL_WEEKLY_NEW_LEAD_TARGET")
    growth_window_days = _env_value("PROVIDER_INTEL_GROWTH_WINDOW_DAYS")
    enforce_growth_governor = _env_value("PROVIDER_INTEL_ENFORCE_GROWTH_GOVERNOR")
    require_fetch_success_gate = _env_value("PROVIDER_INTEL_REQUIRE_FETCH_SUCCESS_GATE")
    require_net_new_gate = _env_value("PROVIDER_INTEL_REQUIRE_NET_NEW_GATE")
    fail_on_zero_new_leads = _env_value("PROVIDER_INTEL_FAIL_ON_ZERO_NEW_LEADS")
    output_stale_hours = _env_value("PROVIDER_INTEL_OUTPUT_STALE_HOURS")
    agent_research_enabled = _env_value("PROVIDER_INTEL_AGENT_RESEARCH")
    agent_research_limit = _env_value("PROVIDER_INTEL_AGENT_RESEARCH_LIMIT")
    agent_research_min_score = _env_value("PROVIDER_INTEL_AGENT_RESEARCH_MIN_SCORE")
    retry_base_delay_seconds = _env_value("PROVIDER_INTEL_RETRY_BASE_DELAY_SECONDS")
    retry_factor = _env_value("PROVIDER_INTEL_RETRY_FACTOR")
    crawlee_headless = _env_value("PROVIDER_INTEL_CRAWLEE_HEADLESS")
    crawlee_proxy_urls = _env_value("PROVIDER_INTEL_CRAWLEE_PROXY_URLS")
    crawlee_max_browser_pages = _env_value("PROVIDER_INTEL_CRAWLEE_MAX_BROWSER_PAGES_PER_DOMAIN")
    crawlee_browser_isolation = _env_value("PROVIDER_INTEL_CRAWLEE_BROWSER_ISOLATION")
    crawlee_domain_policies_file = _env_value("PROVIDER_INTEL_CRAWLEE_DOMAIN_POLICIES_FILE")

    if not cfg_path.exists():
        return CrawlConfig(
            seed_file=default_seed,
            discovery_seed_file=discovery_seed,
            config_path=str(cfg_path),
        )

    with cfg_path.open(encoding="utf-8") as f:
        data = json.loads(f.read())

    return CrawlConfig(
        user_agent=str(data.get("userAgent", defaults.user_agent)),
        timeout_seconds=float(data.get("timeoutSeconds", defaults.timeout_seconds)),
        max_retries=int(data.get("maxRetries", defaults.max_retries)),
        retry_delay_seconds=float(data.get("retryDelaySeconds", defaults.retry_delay_seconds)),
        crawl_delay_seconds=float(data.get("crawlDelaySeconds", defaults.crawl_delay_seconds)),
        max_depth=int(data.get("maxDepth", defaults.max_depth)),
        max_pages_per_domain=int(data.get("maxPagesPerDomain", defaults.max_pages_per_domain)),
        max_total_pages=_coerce_optional_int(data.get("maxTotalPages"), defaults.max_total_pages),
        respect_robots=_coerce_bool(data.get("respectRobots"), defaults.respect_robots),
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
        require_net_new_gate=_coerce_bool(
            require_net_new_gate,
            _coerce_bool(
                data.get("requireNetNewGate"),
                defaults.require_net_new_gate,
            ),
        ),
        fail_on_zero_new_leads=_coerce_bool(
            fail_on_zero_new_leads,
            _coerce_bool(
                data.get("failOnZeroNewLeads"),
                defaults.fail_on_zero_new_leads,
            ),
        ),
        output_stale_hours=int(output_stale_hours or data.get("outputStaleHours", defaults.output_stale_hours)),
        agent_research_enabled=_coerce_bool(
            agent_research_enabled,
            _coerce_bool(
                data.get("agentResearchEnabled"),
                defaults.agent_research_enabled,
            ),
        ),
        agent_research_limit=int(agent_research_limit or data.get("agentResearchLimit", defaults.agent_research_limit)),
        agent_research_min_score=int(
            agent_research_min_score or data.get("agentResearchMinScore", defaults.agent_research_min_score)
        ),
        agent_research_paths=_coalesce_list(
            data.get("agentResearchPaths"),
            defaults.agent_research_paths,
        ),
        retry_base_delay_seconds=float(retry_base_delay_seconds or data.get("retryBaseDelaySeconds", defaults.retry_base_delay_seconds)),
        retry_factor=float(retry_factor or data.get("retryFactor", defaults.retry_factor)),
        cache_ttl_hours=int(data.get("cacheTtlHours", defaults.cache_ttl_hours)),
        per_domain_min_interval_seconds=float(
            data.get("perDomainMinIntervalSeconds", defaults.per_domain_min_interval_seconds)
        ),
        extra_paths=_coalesce_list(data.get("extraPaths"), defaults.extra_paths),
        role_keywords=_coalesce_list(data.get("roleKeywords"), defaults.role_keywords),
        max_concurrency=max(1, int(data.get("maxConcurrency", defaults.max_concurrency))),
        crawlee_headless=_coerce_bool(
            crawlee_headless,
            _coerce_bool(data.get("crawleeHeadless"), defaults.crawlee_headless),
        ),
        crawlee_browser_type=str(data.get("crawleeBrowserType", defaults.crawlee_browser_type)),
        crawlee_proxy_urls=_coalesce_csv_list(
            crawlee_proxy_urls if crawlee_proxy_urls is not None else data.get("crawleeProxyUrls"),
            defaults.crawlee_proxy_urls,
        ),
        crawlee_use_session_pool=_coerce_bool(
            data.get("crawleeUseSessionPool"),
            defaults.crawlee_use_session_pool,
        ),
        crawlee_retry_on_blocked=_coerce_bool(
            data.get("crawleeRetryOnBlocked"),
            defaults.crawlee_retry_on_blocked,
        ),
        crawlee_max_session_rotations=int(
            data.get("crawleeMaxSessionRotations", defaults.crawlee_max_session_rotations)
        ),
        crawlee_viewport_width=int(data.get("crawleeViewportWidth", defaults.crawlee_viewport_width)),
        crawlee_viewport_height=int(data.get("crawleeViewportHeight", defaults.crawlee_viewport_height)),
        crawlee_max_browser_pages_per_domain=max(
            1,
            int(
                crawlee_max_browser_pages
                or data.get("crawleeMaxBrowserPagesPerDomain", defaults.crawlee_max_browser_pages_per_domain)
            ),
        ),
        crawlee_browser_isolation=str(
            (
                crawlee_browser_isolation
                or data.get("crawleeBrowserIsolation", defaults.crawlee_browser_isolation)
            )
        ).strip().lower()
        or defaults.crawlee_browser_isolation,
        crawlee_extra_block_patterns=_coalesce_list(
            data.get("crawleeExtraBlockPatterns"),
            defaults.crawlee_extra_block_patterns,
        ),
        crawlee_domain_policies_file=str(
            crawlee_domain_policies_file
            or data.get("crawleeDomainPoliciesFile", defaults.crawlee_domain_policies_file)
        ),
        config_path=str(cfg_path),
    )


def discover_seed_paths(cfg: CrawlConfig) -> list[str]:
    paths = [cfg.seed_file]
    if cfg.discovery_seed_file:
        paths.append(cfg.discovery_seed_file)
    return paths
