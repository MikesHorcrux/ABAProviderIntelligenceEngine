#!/usr/bin/env python3.11
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pipeline.config import load_crawl_config
from pipeline.observability import Metrics, build_logger
from pipeline.stages.discovery import DiscoverySeed


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


class FixtureHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        routes = {
            "/": (
                200,
                "text/html; charset=utf-8",
                """
                <html><body>
                <div id="ready">home</div>
                <a href="/about">About</a>
                <a href="/team">Team</a>
                </body></html>
                """,
            ),
            "/about": (200, "text/html; charset=utf-8", "<html><body>about</body></html>"),
            "/team": (200, "text/html; charset=utf-8", "<html><body>team</body></html>"),
            "/blocked-marker": (
                200,
                "text/html; charset=utf-8",
                "<html><body>verify you are human</body></html>",
            ),
        }
        status, content_type, body = routes.get(self.path, (404, "text/html; charset=utf-8", "<html>missing</html>"))
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def _start_server() -> tuple[ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _write_config(base: Path, *, port: int, browser_mode: bool = False) -> Path:
    domain = f"127.0.0.1:{port}"
    config_path = base / "crawler_config.json"
    policy_path = base / "fetch_policies.json"

    config_path.write_text(
        json.dumps(
            {
                "seedFile": "seeds.csv",
                "timeoutSeconds": 5,
                "maxRetries": 1,
                "crawlDelaySeconds": 0.0,
                "perDomainMinIntervalSeconds": 0.0,
                "maxDepth": 1,
                "maxPagesPerDomain": 3,
                "respectRobots": False,
                "cacheTtlHours": 0,
                "crawleeHeadless": True,
                "crawleeBrowserType": "chromium",
                "crawleeProxyUrls": [],
                "crawleeUseSessionPool": True,
                "crawleeRetryOnBlocked": True,
                "crawleeMaxSessionRotations": 2,
                "crawleeViewportWidth": 1280,
                "crawleeViewportHeight": 1024,
                "crawleeMaxBrowserPagesPerDomain": 2,
                "crawleeExtraBlockPatterns": [],
                "crawleeDomainPoliciesFile": "fetch_policies.json",
            }
        ),
        encoding="utf-8",
    )
    policy_path.write_text(
        json.dumps(
            {
                "default": {
                    "mode": "http_then_browser_on_block",
                    "waitForSelector": "",
                    "extraBlockPatterns": [],
                    "maxPagesPerDomain": None,
                    "maxDepth": None,
                    "browserOnBlock": True,
                },
                "domains": {
                    domain: {
                        "mode": "browser" if browser_mode else "http_then_browser_on_block",
                        "waitForSelector": "#ready" if browser_mode else "",
                        "extraBlockPatterns": [],
                        "maxPagesPerDomain": 2,
                        "maxDepth": 1,
                        "browserOnBlock": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_fetch_integration_local_server() -> None:
    if os.environ.get("CANNARADAR_RUN_FETCH_INTEGRATION") != "1":
        print("test_fetch_integration: skipped (set CANNARADAR_RUN_FETCH_INTEGRATION=1)")
        return

    from pipeline.stages.fetch import run_fetch

    server, thread = _start_server()
    try:
        port = server.server_address[1]
        logger = build_logger("fetch-integration", "fetch")

        with tempfile.TemporaryDirectory() as td_http:
            config_path = _write_config(Path(td_http), port=port, browser_mode=False)
            cfg = load_crawl_config(config_path)
            metrics = Metrics("fetch-http")
            con = _connect()
            seed = DiscoverySeed(name="Blocked", website=f"http://127.0.0.1:{port}/blocked-marker", state="CA", market="CA")
            results = run_fetch(con, [seed], cfg, logger, metrics, "fetch-http")
            assert metrics.snapshot().get("browser_escalations", 0) == 1
            assert metrics.snapshot().get("pages_browser_fetched", 0) >= 1
            assert con.execute("SELECT COUNT(*) AS c FROM crawl_results").fetchone()["c"] >= 2
            assert isinstance(results, list)
            con.close()

        with tempfile.TemporaryDirectory() as td_browser:
            config_path = _write_config(Path(td_browser), port=port, browser_mode=True)
            cfg = load_crawl_config(config_path)
            metrics = Metrics("fetch-browser")
            con = _connect()
            seed = DiscoverySeed(name="Browser", website=f"http://127.0.0.1:{port}/", state="CA", market="CA")
            results = run_fetch(con, [seed], cfg, logger, metrics, "fetch-browser")
            assert metrics.snapshot().get("pages_browser_fetched", 0) >= 1
            assert any(result.normalized_url == f"http://127.0.0.1:{port}/" for result in results)
            con.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def main() -> None:
    test_fetch_integration_local_server()


if __name__ == "__main__":
    main()
