from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from cli.errors import ConfigError
from jobs.ingest_sources import assert_schema_layout, assert_schema_migration, init_db
from pipeline.config import DEFAULT_CONFIG_PATH, CrawlConfig, load_crawl_config
from pipeline.db import connect_db
from pipeline.run_state import ensure_run_state_dir


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "out" / "provider_intel"
STATE_DIR = ROOT / "data" / "state"
MIN_FREE_BYTES = 128 * 1024 * 1024


def resolve_config_path(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value).expanduser().resolve()
    env_value = os.environ.get("PROVIDER_INTEL_CONFIG") or os.environ.get("CANNARADAR_CRAWLER_CONFIG")
    if env_value:
        return Path(env_value).expanduser().resolve()
    return DEFAULT_CONFIG_PATH.resolve()


def check_item(check_id: str, status: str, summary: str, *, details: dict[str, Any] | None = None, remediation: str = "") -> dict[str, Any]:
    return {
        "id": check_id,
        "status": status,
        "summary": summary,
        "details": details or {},
        "remediation": remediation,
    }


def run_doctor(*, db_path: str, config_path: str | None, run_state_dir: str | None) -> dict[str, Any]:
    resolved_config = resolve_config_path(config_path)
    checks: list[dict[str, Any]] = []
    cfg: CrawlConfig | None = None

    checks.append(
        check_item(
            "python_version",
            "pass" if os.sys.version_info >= (3, 11) else "fail",
            f"Python runtime is {os.sys.version.split()[0]}",
            details={"required": "3.11+"},
            remediation="Use python3.11 for the provider intelligence CLI.",
        )
    )
    checks.append(
        check_item(
            "config_path",
            "pass" if resolved_config.exists() else "fail",
            f"Config path {'exists' if resolved_config.exists() else 'is missing'}: {resolved_config}",
            remediation="Run `provider_intel_cli.py init` or pass `--config`.",
        )
    )

    try:
        cfg = load_crawl_config(resolved_config)
        checks.append(check_item("config_load", "pass", "Crawler config loaded successfully", details={"config_path": str(resolved_config)}))
    except Exception as exc:
        checks.append(check_item("config_load", "fail", f"Failed to load config: {exc}", remediation="Fix JSON syntax in crawler_config.json."))

    if cfg is not None:
        seed_path = cfg.resolve_runtime_path(cfg.seed_file) if cfg.seed_file else None
        if seed_path and not seed_path.exists():
            repo_relative = (ROOT / cfg.seed_file).resolve()
            if repo_relative.exists():
                seed_path = repo_relative
        checks.append(
            check_item(
                "seed_pack",
                "pass" if seed_path and seed_path.exists() and seed_path.suffix.lower() == ".json" else "fail",
                f"Seed pack path: {seed_path}",
                remediation="Create the New Jersey seed pack or point `seedFile` at a valid manifest.",
            )
        )
        rules_path = ROOT / "reference" / "prescriber_rules" / "nj.json"
        checks.append(
            check_item(
                "prescriber_rules",
                "pass" if rules_path.exists() else "fail",
                f"Prescriber rules path: {rules_path}",
                remediation="Add `reference/prescriber_rules/nj.json` before pilot runs.",
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
            checks.append(check_item(f"writable_{label}", "fail", f"Cannot write to {path}: {exc}", remediation="Fix permissions."))

    try:
        con = connect_db(Path(db_path).expanduser().resolve())
        con.close()
        checks.append(check_item("db_connectivity", "pass", f"SQLite DB is reachable: {db_path}"))
    except Exception as exc:
        checks.append(check_item("db_connectivity", "fail", f"Failed to open SQLite DB: {exc}", remediation="Check the DB path and permissions."))

    try:
        con = sqlite3.connect(Path(db_path).expanduser().resolve())
        assert_schema_layout(con)
        assert_schema_migration(con)
        con.close()
        checks.append(check_item("db_schema", "pass", "Schema layout and migration metadata are healthy"))
    except Exception as exc:
        checks.append(check_item("db_schema", "fail", f"Schema validation failed: {exc}", remediation="Run `provider_intel_cli.py init`."))

    checks.append(
        check_item(
            "crawlee_import",
            "pass" if importlib.util.find_spec("crawlee") is not None else "fail",
            "Crawlee import check",
            remediation="Install dependencies with `pip install -r requirements.txt`.",
        )
    )
    checks.append(
        check_item(
            "playwright_import",
            "pass" if importlib.util.find_spec("playwright") is not None else "warn",
            "Playwright import check",
            remediation="Install Playwright for browser-heavy directories and richer PDFs.",
        )
    )

    try:
        usage = shutil.disk_usage(ROOT)
        checks.append(
            check_item(
                "disk_space",
                "pass" if usage.free >= MIN_FREE_BYTES else "warn",
                "Disk space check",
                details={"free_bytes": usage.free, "threshold_bytes": MIN_FREE_BYTES},
            )
        )
    except Exception as exc:
        checks.append(check_item("disk_space", "warn", f"Could not determine disk space: {exc}"))

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
        "cacheTtlHours": cfg.cache_ttl_hours,
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
        "crawleeBrowserIsolation": cfg.crawlee_browser_isolation,
        "crawleeExtraBlockPatterns": cfg.crawlee_extra_block_patterns,
        "crawleeDomainPoliciesFile": cfg.crawlee_domain_policies_file,
    }


def default_fetch_policies_payload() -> dict[str, Any]:
    return {
        "default": {"mode": "http_then_browser_on_block", "browserOnBlock": True},
        "domains": {},
    }


def _config_requires_provider_rewrite(path: Path) -> bool:
    if not path.exists():
        return True
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return True
    seed_file = str(payload.get("seedFile") or "").strip()
    return not seed_file.endswith("seed_pack.json")


def run_init(*, db_path: str, config_path: str | None, run_state_dir: str | None) -> dict[str, Any]:
    resolved_config = resolve_config_path(config_path)
    resolved_db = Path(db_path).expanduser().resolve()
    resolved_db.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    ensure_run_state_dir(run_state_dir)

    if _config_requires_provider_rewrite(resolved_config):
        resolved_config.write_text(json.dumps(default_config_payload(), indent=2), encoding="utf-8")
    cfg = load_crawl_config(resolved_config)
    resolved_policy = cfg.resolved_crawlee_domain_policies_path()
    if not resolved_policy.exists():
        resolved_policy.parent.mkdir(parents=True, exist_ok=True)
        resolved_policy.write_text(json.dumps(default_fetch_policies_payload(), indent=2), encoding="utf-8")

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
            "provider_intel_cli.py doctor --json",
            "provider_intel_cli.py sync --json",
        ],
    }
