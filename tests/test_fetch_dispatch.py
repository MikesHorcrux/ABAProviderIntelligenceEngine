#!/usr/bin/env python3.11
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from pipeline.fetch_backends.common import (
    SeedRunRecorder,
    detect_block_signal,
    first_positive_status_code,
    status_code_from_error_text,
)
from pipeline.fetch_backends.domain_policy import load_domain_policies
from pipeline.observability import Metrics
from pipeline.stages.discovery import DiscoverySeed


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def test_domain_policy_loader_uses_exact_normalized_domains() -> None:
    with tempfile.TemporaryDirectory() as td:
        policy_path = Path(td) / "fetch_policies.json"
        policy_path.write_text(
            json.dumps(
                {
                    "default": {"mode": "http_only", "browserOnBlock": False},
                    "domains": {
                        "www.Example.com": {
                            "mode": "browser",
                            "waitForSelector": "#ready",
                            "extraBlockPatterns": ["challenge-page"],
                            "maxPagesPerDomain": 2,
                            "maxDepth": 1,
                            "browserOnBlock": True,
                        },
                        "*.ignored.example.com": {"mode": "browser"},
                    },
                }
            ),
            encoding="utf-8",
        )

        policies = load_domain_policies(policy_path)
        exact = policies.resolve("https://www.example.com/contact")
        subdomain = policies.resolve("https://sub.example.com")

        assert exact.mode == "browser"
        assert exact.wait_for_selector == "#ready"
        assert exact.extra_block_patterns == ("challenge-page",)
        assert exact.max_pages_per_domain == 2
        assert exact.max_depth == 1
        assert subdomain.mode == "http_only"


def test_block_signal_classification_covers_status_and_markers() -> None:
    status_signal = detect_block_signal(status_code=403, content="")
    marker_signal = detect_block_signal(status_code=200, content="<html>Verify you are human</html>")
    extra_signal = detect_block_signal(status_code=200, content="<html>custom interstitial</html>", extra_patterns=("custom interstitial",))
    normal_signal = detect_block_signal(status_code=200, content="<html>all good</html>")

    assert status_signal.triggered is True
    assert status_signal.reason == "status:403"
    assert marker_signal.triggered is True
    assert marker_signal.reason.startswith("marker:")
    assert extra_signal.triggered is True
    assert normal_signal.triggered is False


def test_block_signal_ignores_script_only_markers() -> None:
    script_only = detect_block_signal(
        status_code=200,
        content=(
            "<html><head>"
            "<script src='https://challenges.cloudflare.com/turnstile.js'></script>"
            "<script>window.recaptcha = true;</script>"
            "</head><body><main>Planet 13</main></body></html>"
        ),
    )
    visible_marker = detect_block_signal(
        status_code=200,
        content="<html><body><h1>Attention Required</h1><p>Cloudflare challenge</p></body></html>",
    )

    assert script_only.triggered is False
    assert visible_marker.triggered is True
    assert visible_marker.reason == "marker:attention required"


def test_first_positive_status_code_prefers_nonzero_values() -> None:
    assert first_positive_status_code(0, None, "403") == 403
    assert first_positive_status_code(None, "", 0, 429) == 429
    assert first_positive_status_code(None, "", 0) == 0


def test_status_code_from_error_text_parses_session_error_message() -> None:
    assert status_code_from_error_text("Assuming the session is blocked based on HTTP status code 403") == 403
    assert status_code_from_error_text("Client error status code returned (status code: 404).") == 404
    assert status_code_from_error_text("no status here") == 0


def test_seed_run_recorder_persists_contract_rows_and_results() -> None:
    con = _connect()
    metrics = Metrics("fetch-test")
    seed = DiscoverySeed(name="Green Leaf", website="https://greenleaf.com", state="CA", market="CA")
    recorder = SeedRunRecorder(
        con=con,
        seed=seed,
        seed_domain="greenleaf.com",
        job_id="job-1",
        metrics=metrics,
    )
    recorder.start()

    success = recorder.record_result(
        requested_url="https://greenleaf.com/",
        normalized_url="https://greenleaf.com/",
        status_code=200,
        content="<html>home</html>",
        error_message="",
        attempt_count=1,
        emit_result=True,
        count_as_success=True,
    )
    blocked = recorder.record_result(
        requested_url="https://greenleaf.com/about",
        normalized_url="https://greenleaf.com/about",
        status_code=200,
        content="<html>captcha</html>",
        error_message="blocked",
        attempt_count=1,
        emit_result=False,
        count_as_success=False,
    )
    browser = recorder.record_result(
        requested_url="https://greenleaf.com/team",
        normalized_url="https://greenleaf.com/team",
        status_code=200,
        content="<html>team</html>",
        error_message="",
        attempt_count=2,
        emit_result=True,
        count_as_success=True,
        used_browser=True,
    )
    failure = recorder.record_result(
        requested_url="https://greenleaf.com/contact",
        normalized_url="https://greenleaf.com/contact",
        status_code=503,
        content="",
        error_message="Service Unavailable",
        attempt_count=3,
        emit_result=False,
        count_as_success=False,
    )
    recorder.finalize()

    assert success is not None
    assert browser is not None
    assert blocked is None
    assert failure is None
    assert success.seed_name == "Green Leaf"
    assert browser.normalized_url == "https://greenleaf.com/team"

    crawl_job = con.execute("SELECT * FROM crawl_jobs WHERE seed_domain='greenleaf.com'").fetchone()
    assert crawl_job["status"] == "completed"
    assert int(crawl_job["last_status_code"]) == 503

    crawl_results = con.execute("SELECT * FROM crawl_results ORDER BY requested_url").fetchall()
    assert len(crawl_results) == 4

    telemetry = con.execute("SELECT * FROM seed_telemetry WHERE seed_domain='greenleaf.com'").fetchone()
    assert telemetry["last_run_status"] == "completed"
    assert int(telemetry["attempts"]) == 7
    assert int(telemetry["successes"]) == 2
    assert int(telemetry["failures"]) == 2
    assert int(telemetry["last_run_pages_fetched"]) == 4
    assert int(telemetry["last_run_success_pages"]) == 2
    assert int(telemetry["last_run_failure_pages"]) == 2

    counters = metrics.snapshot()
    assert counters["pages_fetched"] == 4
    assert counters["pages_http_fetched"] == 3
    assert counters["pages_browser_fetched"] == 1
    assert counters["fetch_retries_success"] == 1
    assert counters["fetch_retries_server_error"] == 2

    con.close()


def test_seed_run_recorder_uses_status_hint_for_empty_blocked_seed() -> None:
    con = _connect()
    metrics = Metrics("fetch-test-empty")
    seed = DiscoverySeed(name="Blocked Seed", website="https://blocked.example", state="CA", market="CA")
    recorder = SeedRunRecorder(
        con=con,
        seed=seed,
        seed_domain="blocked.example",
        job_id="job-empty",
        metrics=metrics,
    )
    recorder.start()
    recorder.note_status_hint(403)
    recorder.finalize()

    crawl_job = con.execute("SELECT status, last_status_code FROM crawl_jobs WHERE seed_domain='blocked.example'").fetchone()
    telemetry = con.execute("SELECT last_run_status, last_status_code FROM seed_telemetry WHERE seed_domain='blocked.example'").fetchone()

    assert crawl_job["status"] == "empty"
    assert int(crawl_job["last_status_code"]) == 403
    assert telemetry["last_run_status"] == "empty"
    assert int(telemetry["last_status_code"]) == 403
    con.close()


def main() -> None:
    test_domain_policy_loader_uses_exact_normalized_domains()
    test_block_signal_classification_covers_status_and_markers()
    test_block_signal_ignores_script_only_markers()
    test_first_positive_status_code_prefers_nonzero_values()
    test_status_code_from_error_text_parses_session_error_message()
    test_seed_run_recorder_persists_contract_rows_and_results()
    test_seed_run_recorder_uses_status_hint_for_empty_blocked_seed()
    print("test_fetch_dispatch: ok")


if __name__ == "__main__":
    main()
