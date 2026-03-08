#!/usr/bin/env python3.11
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from pipeline.config import load_crawl_config
import pipeline.fetch_backends.crawlee_backend as crawlee_backend
from pipeline.fetch_backends.crawlee_backend import SeedCrawlState
from pipeline.fetch_backends.common import (
    SeedRunRecorder,
    detect_block_signal,
    first_positive_status_code,
    status_code_from_error_text,
)
from pipeline.fetch_backends.domain_policy import load_domain_policies
from pipeline.observability import Metrics, build_logger
from pipeline.run_control import load_run_control
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


def test_seed_run_recorder_commits_progress_immediately() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "fetch-progress.db"
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        metrics = Metrics("fetch-progress")
        seed = DiscoverySeed(name="Progress Seed", website="https://progress.example", state="CA", market="CA")
        recorder = SeedRunRecorder(
            con=con,
            seed=seed,
            seed_domain="progress.example",
            job_id="job-progress",
            metrics=metrics,
        )
        recorder.start()
        recorder.record_result(
            requested_url="https://progress.example/",
            normalized_url="https://progress.example/",
            status_code=200,
            content="<html>ok</html>",
            error_message="",
            attempt_count=1,
            emit_result=True,
            count_as_success=True,
        )

        observer = sqlite3.connect(db_path)
        observer.row_factory = sqlite3.Row
        crawl_job = observer.execute("SELECT status FROM crawl_jobs WHERE seed_domain='progress.example'").fetchone()
        crawl_results = observer.execute("SELECT COUNT(*) FROM crawl_results WHERE crawl_job_pk=?", (recorder.job_pk,)).fetchone()[0]

        assert crawl_job is not None
        assert crawl_job["status"] == "running"
        assert int(crawl_results) == 1

        observer.close()
        con.close()


def test_seed_crawl_state_filters_assets_and_honors_manual_controls() -> None:
    con = _connect()
    metrics = Metrics("fetch-controls")
    logger = build_logger("fetch-controls", "fetch")
    cfg = load_crawl_config("/tmp/cannaradar-fetch-controls-does-not-exist.json")
    seed = DiscoverySeed(name="Green Leaf", website="https://greenleaf.com", state="CA", market="CA")
    recorder = SeedRunRecorder(
        con=con,
        seed=seed,
        seed_domain="greenleaf.com",
        job_id="job-controls",
        metrics=metrics,
    )
    recorder.start()

    with tempfile.TemporaryDirectory() as td:
        state = SeedCrawlState(
            con=con,
            seed=seed,
            cfg=cfg,
            policy=load_domain_policies("/tmp/does-not-exist-fetch-policy.json").default,
            metrics=metrics,
            logger=logger,
            job_id="job-controls",
            denylist=set(),
            recorder=recorder,
            crawl_pages=10,
            total_page_limit=10,
            crawl_depth=2,
            browser_page_limit=3,
            run_state_dir=td,
        )

        assert state.should_accept_url("https://greenleaf.com/about", 0) is True
        assert state.should_accept_url("https://greenleaf.com/_next/static/chunk.js", 1) is False

        from cli.control import run_control_apply

        run_control_apply(
            run_id="job-controls",
            run_state_dir=td,
            action="suppress-prefix",
            domain="greenleaf.com",
            value="/about",
            reason="agent_test",
        )
        assert state.should_accept_url("https://greenleaf.com/about/company", 1) is False
    con.close()


def test_seed_crawl_state_auto_suppresses_prefix_and_stops_on_dns() -> None:
    con = _connect()
    metrics = Metrics("fetch-healing")
    logger = build_logger("fetch-healing", "fetch")
    cfg = load_crawl_config("/tmp/cannaradar-fetch-healing-does-not-exist.json")
    seed = DiscoverySeed(name="Healing Seed", website="https://healing.example", state="CA", market="CA")
    recorder = SeedRunRecorder(
        con=con,
        seed=seed,
        seed_domain="healing.example",
        job_id="job-healing",
        metrics=metrics,
    )
    recorder.start()

    with tempfile.TemporaryDirectory() as td:
        state = SeedCrawlState(
            con=con,
            seed=seed,
            cfg=cfg,
            policy=load_domain_policies("/tmp/does-not-exist-fetch-policy.json").default,
            metrics=metrics,
            logger=logger,
            job_id="job-healing",
            denylist=set(),
            recorder=recorder,
            crawl_pages=10,
            total_page_limit=10,
            crawl_depth=2,
            browser_page_limit=3,
            run_state_dir=td,
        )

        for _ in range(3):
            state.observe_failure(
                normalized_url="https://healing.example/blog/post",
                status_code=404,
                error_message="missing",
            )
        assert "/blog/" in state.suppressed_path_prefixes
        assert state.should_accept_url("https://healing.example/blog/next-post", 1) is False

        stop_reason = state.observe_failure(
            normalized_url="https://healing.example/",
            status_code=0,
            error_message="dns error: failed to lookup address information",
        )
        assert stop_reason == "dns_failure"
        assert state.stop_requested is True
        assert state.seed_quarantined is True
    con.close()


def test_run_fetch_contains_browser_driver_failure_to_one_seed() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "fetch.db"
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        metrics = Metrics("fetch-runtime-failure")
        logger = build_logger("fetch-runtime-failure", "fetch")
        cfg = load_crawl_config("/tmp/cannaradar-fetch-runtime-failure.json")
        cfg.crawlee_browser_isolation = "subprocess"
        seeds = [
            DiscoverySeed(name="Crash Seed", website="https://fail.example", state="CA", market="CA"),
            DiscoverySeed(name="Healthy Seed", website="https://pass.example", state="CA", market="CA"),
        ]

        original_http = crawlee_backend._run_http_crawl
        original_browser_worker = crawlee_backend._run_browser_worker_subprocess

        async def fake_run_http_crawl(state, initial_requests):
            target_url = initial_requests[0].normalized_url
            state.mark_processed(target_url)
            state.recorder.record_result(
                requested_url=target_url,
                normalized_url=target_url,
                status_code=200,
                content="<html>ok</html>",
                error_message="",
                attempt_count=1,
                emit_result=True,
                count_as_success=True,
            )
            state.observe_success()
            if state.domain == "fail.example":
                state.request_browser_escalation("marker:captcha")

        def fake_browser_worker_subprocess(state, initial_requests):
            raise RuntimeError("Connection closed while reading from the driver")

        crawlee_backend._run_http_crawl = fake_run_http_crawl
        crawlee_backend._run_browser_worker_subprocess = fake_browser_worker_subprocess
        try:
            results = crawlee_backend.run_fetch(
                con,
                seeds,
                cfg,
                logger,
                metrics,
                "job-seed-runtime-failure",
                max_pages_per_domain=2,
                max_total_pages=2,
                max_depth=0,
                run_state_dir=td,
            )
        finally:
            crawlee_backend._run_http_crawl = original_http
            crawlee_backend._run_browser_worker_subprocess = original_browser_worker

        statuses = {
            row["seed_domain"]: row["status"]
            for row in con.execute("SELECT seed_domain, status FROM crawl_jobs ORDER BY seed_domain").fetchall()
        }
        telemetry = {
            row["seed_domain"]: row["last_run_status"]
            for row in con.execute("SELECT seed_domain, last_run_status FROM seed_telemetry ORDER BY seed_domain").fetchall()
        }
        control = load_run_control("job-seed-runtime-failure", td)
        crash_control = control["agent_controls"]["domains"]["fail.example"]
        crash_runtime = control["runtime"]["domains"]["fail.example"]

        assert len(results) == 2
        assert sorted(result.seed_name for result in results) == ["Crash Seed", "Healthy Seed"]
        assert statuses["fail.example"] == "partial"
        assert statuses["pass.example"] == "completed"
        assert telemetry["fail.example"] == "partial"
        assert crash_control["stop_requested"] is True
        assert crash_runtime["status"] == "partial"
        assert metrics.snapshot()["seed_crawl_exceptions"] == 1

        con.close()


def main() -> None:
    test_domain_policy_loader_uses_exact_normalized_domains()
    test_block_signal_classification_covers_status_and_markers()
    test_block_signal_ignores_script_only_markers()
    test_first_positive_status_code_prefers_nonzero_values()
    test_status_code_from_error_text_parses_session_error_message()
    test_seed_run_recorder_persists_contract_rows_and_results()
    test_seed_run_recorder_uses_status_hint_for_empty_blocked_seed()
    test_seed_run_recorder_commits_progress_immediately()
    test_seed_crawl_state_filters_assets_and_honors_manual_controls()
    test_seed_crawl_state_auto_suppresses_prefix_and_stops_on_dns()
    test_run_fetch_contains_browser_driver_failure_to_one_seed()
    print("test_fetch_dispatch: ok")


if __name__ == "__main__":
    main()
