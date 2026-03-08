#!/usr/bin/env python3.11
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from pipeline.config import load_crawl_config


def _set_env(**updates: str | None):
    previous = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    return previous


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_load_crawl_config_defaults_include_crawlee_settings() -> None:
    cfg = load_crawl_config("/tmp/does-not-exist-cannaradar-config.json")
    assert cfg.crawlee_headless is True
    assert cfg.crawlee_browser_type == "chromium"
    assert cfg.crawlee_proxy_urls == []
    assert cfg.crawlee_use_session_pool is True
    assert cfg.crawlee_retry_on_blocked is True
    assert cfg.crawlee_max_session_rotations == 8
    assert cfg.crawlee_viewport_width == 1280
    assert cfg.crawlee_viewport_height == 1024
    assert cfg.crawlee_max_browser_pages_per_domain == 5
    assert cfg.crawlee_domain_policies_file == "fetch_policies.json"


def test_env_overrides_apply_to_crawlee_settings() -> None:
    with tempfile.TemporaryDirectory() as td:
        config_path = Path(td) / "crawler_config.json"
        config_path.write_text(
            json.dumps(
                {
                    "seedFile": "seeds.csv",
                    "crawleeHeadless": True,
                    "crawleeProxyUrls": ["http://config-proxy:8080"],
                    "crawleeMaxBrowserPagesPerDomain": 3,
                    "crawleeDomainPoliciesFile": "fetch_policies.json",
                }
            ),
            encoding="utf-8",
        )

        previous = _set_env(
            CANNARADAR_CRAWLEE_HEADLESS="off",
            CANNARADAR_CRAWLEE_PROXY_URLS="http://env-proxy-1:8080,http://env-proxy-2:8080",
            CANNARADAR_CRAWLEE_MAX_BROWSER_PAGES_PER_DOMAIN="9",
            CANNARADAR_CRAWLEE_DOMAIN_POLICIES_FILE="ops/custom_policies.json",
        )
        try:
            cfg = load_crawl_config(config_path)
        finally:
            _restore_env(previous)

        assert cfg.crawlee_headless is False
        assert cfg.crawlee_proxy_urls == [
            "http://env-proxy-1:8080",
            "http://env-proxy-2:8080",
        ]
        assert cfg.crawlee_max_browser_pages_per_domain == 9
        assert cfg.crawlee_domain_policies_file == "ops/custom_policies.json"
        assert cfg.resolved_crawlee_domain_policies_path() == (config_path.parent / "ops/custom_policies.json").resolve()


def main() -> None:
    test_load_crawl_config_defaults_include_crawlee_settings()
    test_env_overrides_apply_to_crawlee_settings()
    print("test_fetch_config: ok")


if __name__ == "__main__":
    main()
