from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from pipeline.config import DEFAULT_CONFIG_PATH, CrawlConfig, load_crawl_config
from pipeline.db import connect_db
from pipeline.run_state import ensure_run_state_dir

from cli.errors import ConfigError


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out"
STATE_DIR = ROOT / "data" / "state"
DEFAULT_POLICY_NAME = "fetch_policies.json"
MIN_FREE_BYTES = 256 * 1024 * 1024


def resolve_config_path(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value).expanduser().resolve()
    env_value = os.environ.get("CANNARADAR_CRAWLER_CONFIG")
    if env_value:
        return Path(env_value).expanduser().resolve()
    return DEFAULT_CONFIG_PATH.resolve()


def check_item(
    check_id: str,
    status: str,
    summary: str,
    *,
    details: dict[str, Any] | None = None,
    remediation: str = "",
) -> dict[str, Any]:
    return {
        "id": check_id,
        "status": status,
        "summary": summary,
        "details": details or {},
        "remediation": remediation,
    }


def run_doctor(
    *,
    db_path: str,
    config_path: str | None,
    run_state_dir: str | None,
) -> dict[str, Any]:
    resolved_config = resolve_config_path(config_path)
    checks: list[dict[str, Any]] = []
    cfg: CrawlConfig | None = None

    python_ok = os.sys.version_info >= (3, 11)
    checks.append(
        check_item(
            "python_version",
            "pass" if python_ok else "fail",
            f"Python runtime is {os.sys.version.split()[0]}",
            details={"required": "3.11+"},
            remediation="Use python3.11 for agent CLI and Crawlee runtime.",
        )
    )

    config_exists = resolved_config.exists()
    checks.append(
        check_item(
            "config_path",
            "pass" if config_exists else "fail",
            f"Config path {'exists' if config_exists else 'is missing'}: {resolved_config}",
            remediation="Run `cannaradar_cli.py init` to create the default config, or pass `--config`.",
        )
    )

    try:
        cfg = load_crawl_config(resolved_config)
        checks.append(
            check_item(
                "config_load",
                "pass",
                "Crawler config loaded successfully",
                details={"config_path": str(resolved_config)},
            )
        )
    except Exception as exc:
        checks.append(
            check_item(
                "config_load",
                "fail",
                f"Failed to load crawler config: {exc}",
                remediation="Validate crawler_config.json syntax and required numeric values.",
            )
        )
        cfg = None

    seed_paths: list[str] = []
    if cfg is not None:
        for candidate in [cfg.seed_file, cfg.discovery_seed_file]:
            if not candidate:
                continue
            path = cfg.resolve_runtime_path(candidate)
            seed_paths.append(str(path))
        missing_seed_paths = [path for path in seed_paths if not Path(path).exists()]
        checks.append(
            check_item(
                "seed_inputs",
                "pass" if not missing_seed_paths else "warn",
                "Seed inputs resolved",
                details={"paths": seed_paths, "missing": missing_seed_paths},
                remediation="Create or point `seedFile` / `discoverySeedFile` at real CSV inputs.",
            )
        )

        policy_path = cfg.resolved_crawlee_domain_policies_path()
        policy_status = "pass" if policy_path.exists() else "fail"
        checks.append(
            check_item(
                "fetch_policies",
                policy_status,
                f"Fetch policy file {'found' if policy_path.exists() else 'missing'}: {policy_path}",
                remediation="Create fetch_policies.json or set `crawleeDomainPoliciesFile` to a valid path.",
            )
        )

    for label, path in {
        "db_parent": Path(db_path).expanduser().resolve().parent,
        "out_dir": OUT_DIR,
        "state_dir": STATE_DIR,
        "run_state_dir": ensure_run_state_dir(run_state_dir),
    }.items():
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".doctor_write_test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            checks.append(check_item(f"writable_{label}", "pass", f"Writable path ready: {path}"))
        except Exception as exc:
            checks.append(
                check_item(
                    f"writable_{label}",
                    "fail",
                    f"Cannot write to {path}: {exc}",
                    remediation="Fix directory permissions before running sync/export commands.",
                )
            )

    try:
        con = connect_db(Path(db_path).expanduser().resolve())
        con.close()
        checks.append(check_item("db_connectivity", "pass", f"SQLite DB is reachable: {db_path}"))
    except Exception as exc:
        checks.append(
            check_item(
                "db_connectivity",
                "fail",
                f"Failed to open SQLite DB: {exc}",
                remediation="Ensure the DB path is writable and not held by another process.",
            )
        )

    try:
        from jobs.ingest_sources import assert_schema_layout, assert_schema_migration

        con = sqlite3.connect(Path(db_path).expanduser().resolve())
        assert_schema_layout(con)
        assert_schema_migration(con)
        con.close()
        checks.append(check_item("db_schema", "pass", "Schema layout and migration metadata are healthy"))
    except Exception as exc:
        checks.append(
            check_item(
                "db_schema",
                "fail",
                f"Schema validation failed: {exc}",
                remediation="Run `cannaradar_cli.py init` or `PYTHONPATH=$PWD python3.11 jobs/ingest_sources.py`.",
            )
        )

    crawlee_ready = importlib.util.find_spec("crawlee") is not None
    checks.append(
        check_item(
            "crawlee_import",
            "pass" if crawlee_ready else "fail",
            "Crawlee import check",
            details={"available": crawlee_ready},
            remediation="Install runtime dependencies with `pip install -r requirements.txt`.",
        )
    )

    playwright_ready = False
    playwright_details: dict[str, Any] = {}
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser_type = getattr(pw, "chromium")
            executable = Path(browser_type.executable_path)
            playwright_ready = executable.exists()
            playwright_details = {
                "browser": "chromium",
                "executable": str(executable),
            }
    except Exception as exc:
        playwright_details = {"error": str(exc)}
    checks.append(
        check_item(
            "playwright_browser",
            "pass" if playwright_ready else "warn",
            "Playwright browser runtime check",
            details=playwright_details,
            remediation="Run `playwright install chromium` before browser-escalated crawls.",
        )
    )

    token_envs = [name for name in ("OPENAI_API_KEY", "CANNARADAR_API_TOKEN") if os.environ.get(name)]
    checks.append(
        check_item(
            "env_tokens",
            "skip",
            "No required external auth tokens are needed for the local crawler pipeline",
            details={"present": token_envs, "required": []},
        )
    )

    try:
        usage = shutil.disk_usage(ROOT)
        disk_status = "pass" if usage.free >= MIN_FREE_BYTES else "warn"
        checks.append(
            check_item(
                "disk_space",
                disk_status,
                "Disk space check",
                details={"free_bytes": usage.free, "threshold_bytes": MIN_FREE_BYTES},
                remediation="Free disk space before long crawl runs if available space is low.",
            )
        )
    except Exception as exc:
        checks.append(
            check_item(
                "disk_space",
                "warn",
                f"Could not determine disk space: {exc}",
            )
        )

    failed = [item for item in checks if item["status"] == "fail"]
    warned = [item for item in checks if item["status"] == "warn"]
    return {
        "ok": not failed,
        "checks": checks,
        "summary": {
            "total": len(checks),
            "failed": len(failed),
            "warned": len(warned),
            "config_path": str(resolved_config),
            "db_path": str(Path(db_path).expanduser().resolve()),
        },
    }


def default_config_payload() -> dict[str, Any]:
    cfg = CrawlConfig()
    return {
        "userAgent": cfg.user_agent,
        "timeoutSeconds": cfg.timeout_seconds,
        "maxRetries": cfg.max_retries,
        "retryDelaySeconds": cfg.retry_delay_seconds,
        "crawlDelaySeconds": cfg.crawl_delay_seconds,
        "maxDepth": cfg.max_depth,
        "maxPagesPerDomain": cfg.max_pages_per_domain,
        "maxTotalPages": cfg.max_total_pages,
        "respectRobots": cfg.respect_robots,
        "allowedSchemes": cfg.allowed_schemes,
        "denylist": cfg.denylist,
        "seedFile": cfg.seed_file,
        "discoverySeedFile": cfg.discovery_seed_file,
        "monitorStaleDays": cfg.monitor_stale_days,
        "monitorMaxPagesPerDomain": cfg.monitor_max_pages_per_domain,
        "monitorMaxTotalPages": cfg.monitor_max_total_pages,
        "monitorMaxDepth": cfg.monitor_max_depth,
        "growthMaxPagesPerDomain": cfg.growth_max_pages_per_domain,
        "growthMaxTotalPages": cfg.growth_max_total_pages,
        "growthMaxDepth": cfg.growth_max_depth,
        "weeklyNewLeadTarget": cfg.weekly_new_lead_target,
        "growthWindowDays": cfg.growth_window_days,
        "enforceGrowthGovernor": cfg.enforce_growth_governor,
        "seedFailureStreakLimit": cfg.seed_failure_streak_limit,
        "seedBackoffHours": cfg.seed_backoff_hours,
        "cacheTtlHours": cfg.cache_ttl_hours,
        "requireFetchSuccessGate": cfg.require_fetch_success_gate,
        "requireNetNewGate": cfg.require_net_new_gate,
        "failOnZeroNewLeads": cfg.fail_on_zero_new_leads,
        "outputStaleHours": cfg.output_stale_hours,
        "agentResearchEnabled": cfg.agent_research_enabled,
        "agentResearchLimit": cfg.agent_research_limit,
        "agentResearchMinScore": cfg.agent_research_min_score,
        "agentResearchPaths": cfg.agent_research_paths,
        "retryBaseDelaySeconds": cfg.retry_base_delay_seconds,
        "retryFactor": cfg.retry_factor,
        "perDomainMinIntervalSeconds": cfg.per_domain_min_interval_seconds,
        "extraPaths": cfg.extra_paths,
        "roleKeywords": cfg.role_keywords,
        "maxConcurrency": cfg.max_concurrency,
        "crawleeHeadless": cfg.crawlee_headless,
        "crawleeBrowserType": cfg.crawlee_browser_type,
        "crawleeProxyUrls": cfg.crawlee_proxy_urls,
        "crawleeUseSessionPool": cfg.crawlee_use_session_pool,
        "crawleeRetryOnBlocked": cfg.crawlee_retry_on_blocked,
        "crawleeMaxSessionRotations": cfg.crawlee_max_session_rotations,
        "crawleeViewportWidth": cfg.crawlee_viewport_width,
        "crawleeViewportHeight": cfg.crawlee_viewport_height,
        "crawleeMaxBrowserPagesPerDomain": cfg.crawlee_max_browser_pages_per_domain,
        "crawleeExtraBlockPatterns": cfg.crawlee_extra_block_patterns,
        "crawleeDomainPoliciesFile": cfg.crawlee_domain_policies_file,
    }


def default_fetch_policies_payload() -> dict[str, Any]:
    return {
        "default": {
            "mode": "http_then_browser_on_block",
            "browserOnBlock": True,
        },
        "domains": {},
    }


def run_init(
    *,
    db_path: str,
    config_path: str | None,
    run_state_dir: str | None,
) -> dict[str, Any]:
    resolved_config = resolve_config_path(config_path)
    resolved_db = Path(db_path).expanduser().resolve()

    resolved_db.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    ensure_run_state_dir(run_state_dir)

    if not resolved_config.exists():
        resolved_config.write_text(json.dumps(default_config_payload(), indent=2), encoding="utf-8")
    cfg = load_crawl_config(resolved_config)
    resolved_policy = cfg.resolved_crawlee_domain_policies_path()
    if not resolved_policy.exists():
        resolved_policy.parent.mkdir(parents=True, exist_ok=True)
        resolved_policy.write_text(json.dumps(default_fetch_policies_payload(), indent=2), encoding="utf-8")

    try:
        from jobs.ingest_sources import init_db
    except Exception as exc:
        raise ConfigError(
            f"Failed to import schema bootstrap helpers: {exc}",
            details={"module": "jobs.ingest_sources"},
        )

    con = sqlite3.connect(resolved_db)
    init_db(con)
    con.close()

    doctor = run_doctor(db_path=str(resolved_db), config_path=str(resolved_config), run_state_dir=run_state_dir)
    return {
        "ok": doctor["ok"],
        "db_path": str(resolved_db),
        "config_path": str(resolved_config),
        "fetch_policies_path": str(resolved_policy),
        "run_state_dir": str(ensure_run_state_dir(run_state_dir)),
        "doctor": doctor,
        "next_steps": [
            "cannaradar_cli.py doctor --json",
            "cannaradar_cli.py sync --json",
        ],
    }
