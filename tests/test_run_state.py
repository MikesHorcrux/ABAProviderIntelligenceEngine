#!/usr/bin/env python3.11
from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace

from cli.sync import execute_sync
from pipeline.run_control import ensure_run_control, finalize_run_control, load_run_control, save_run_control
from pipeline.run_state import create_run_state, load_run_state, mark_stage_completed, mark_stage_started, next_stage, save_run_state
from pipeline.stages.discovery import DiscoverySeed
from pipeline.fetch_backends.common import FetchResult


class FakeRunner:
    should_fail_once = True
    last_init: dict[str, object] = {}

    def __init__(self, seeds=None, db_path=None, **kwargs):
        self.seeds_path = seeds or "seed_packs/nj/seed_pack.json"
        self.db_path = Path(db_path)
        self.job_id = "fake-job"
        self.config = SimpleNamespace(config_path=str(self.db_path.parent / "crawler_config.json"))
        self.metrics = SimpleNamespace(snapshot=lambda: {})
        self._seeds = [DiscoverySeed(name="NJ Seed", website="https://seed.example", state="NJ", market="Newark", tier="A", source_type="licensing_board", extraction_profile="board")]
        FakeRunner.last_init = {
            "seeds": self.seeds_path,
            "db_path": str(self.db_path),
            **kwargs,
        }

    def _load_seeds(self, seed_limit=None):
        return self._seeds[: seed_limit or len(self._seeds)]

    def run_seed_ingest(self, seed_limit=None):
        return {"seed_count": len(self._load_seeds(seed_limit)), "rule_count": 1, "state": "NJ"}

    def run_fetch(self, **_kwargs):
        return [
            FetchResult(
                job_pk="job_resume",
                seed_name="NJ Seed",
                seed_state="NJ",
                seed_market="Newark",
                seed_website="https://seed.example",
                target_url="https://seed.example/provider",
                normalized_url="https://seed.example/provider",
                status_code=200,
                content="<html>ok</html>",
                content_hash="hash",
                fetched_at="2026-03-09T00:00:00Z",
            )
        ]

    def run_extract(self, **_kwargs):
        if FakeRunner.should_fail_once:
            FakeRunner.should_fail_once = False
            raise RuntimeError("synthetic extract failure")
        return 1

    def run_resolve(self):
        return {"resolved_count": 1, "review_only_count": 0}

    def run_score(self):
        return 1

    def run_qa(self):
        return {"approved_records": 1, "queued_records": 0, "contradictions": 0}

    def run_export(self, limit=100):
        del limit
        return {"record_count": 1, "review_count": 0}

    def _write_last_run_manifest(self, _payload):
        return None


def test_run_state_round_trip() -> None:
    with tempfile.TemporaryDirectory() as td:
        run_dir = Path(td)
        state = create_run_state(
            run_id="run-1",
            command="sync",
            db_path=str(run_dir / "db.sqlite"),
            config_path=str(run_dir / "crawler_config.json"),
            seeds_path="seed_packs/nj/seed_pack.json",
            crawl_mode="full",
            options={"seed_limit": 1},
        )
        mark_stage_started(state, "seed_ingest")
        mark_stage_completed(state, "seed_ingest", {"seed_count": 1})
        path = save_run_state(state, run_dir)
        loaded = load_run_state("run-1", run_dir)
        assert path.exists()
        assert loaded["stages"]["seed_ingest"]["status"] == "completed"
        assert next_stage(loaded) == "crawl"


def test_execute_sync_can_resume_from_checkpoint() -> None:
    FakeRunner.should_fail_once = True
    FakeRunner.last_init = {}
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        checkpoint_dir = root / "checkpoints"
        args = SimpleNamespace(
            db=str(root / "provider_intel.db"),
            seeds="seed_packs/nj/seed_pack.json",
            max=1,
            crawl_mode="full",
            limit=100,
            crawlee_headless="off",
            run_id="resume-test",
            resume=None,
            checkpoint_dir=str(checkpoint_dir),
            config=None,
            db_timeout_ms=12345,
        )

        try:
            execute_sync(args, runner_factory=FakeRunner)
            raise AssertionError("sync should have failed on the first extract attempt")
        except RuntimeError as exc:
            assert "synthetic extract failure" in str(exc)

        assert FakeRunner.last_init["db_timeout_ms"] == 12345
        assert FakeRunner.last_init["crawl_mode"] == "full"
        assert FakeRunner.last_init["config_overrides"] == {"crawlee_headless": False}

        state = load_run_state("resume-test", checkpoint_dir)
        assert state["stages"]["extract"]["status"] == "failed"

        args.resume = "resume-test"
        result = execute_sync(args, runner_factory=FakeRunner)
        resumed = load_run_state("resume-test", checkpoint_dir)
        assert result["summary"]["discovered"] == 1
        assert resumed["status"] == "completed"
        assert resumed["stages"]["qa"]["status"] == "completed"
        assert resumed["stages"]["export"]["status"] == "completed"


def test_execute_sync_passes_refresh_mode_to_runner() -> None:
    FakeRunner.last_init = {}
    FakeRunner.should_fail_once = False
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        checkpoint_dir = root / "checkpoints"
        args = SimpleNamespace(
            db=str(root / "provider_intel.db"),
            seeds="seed_packs/nj/seed_pack.json",
            max=1,
            crawl_mode="refresh",
            limit=25,
            crawlee_headless=None,
            run_id="refresh-test",
            resume=None,
            checkpoint_dir=str(checkpoint_dir),
            config=None,
            db_timeout_ms=5000,
        )

        execute_sync(args, runner_factory=FakeRunner)

        assert FakeRunner.last_init["crawl_mode"] == "refresh"
        assert FakeRunner.last_init["db_timeout_ms"] == 5000


def test_finalize_run_control_clears_stale_running_domain() -> None:
    with tempfile.TemporaryDirectory() as td:
        run_dir = Path(td)
        state = ensure_run_control("run-control-test", run_dir)
        state["runtime"]["current_seed_domain"] = "demo.example"
        state["runtime"]["domains"]["demo.example"] = {
            "status": "running",
            "processed_urls": 3,
            "success_pages": 1,
            "failure_pages": 1,
            "filtered_urls": 2,
            "last_status_code": 403,
            "last_error": "",
            "discovery_enabled": False,
            "browser_escalated": True,
            "updated_at": "2026-03-08T00:00:00",
        }
        save_run_control(state, run_dir)
        finalize_run_control("run-control-test", status="failed", base_dir=run_dir, replace_running_with="failed", message="browser driver crashed")
        updated = load_run_control("run-control-test", run_dir)
        assert updated["runtime"]["current_seed_domain"] == ""
        assert updated["runtime"]["domains"]["demo.example"]["status"] == "failed"


def main() -> None:
    test_run_state_round_trip()
    test_execute_sync_can_resume_from_checkpoint()
    test_execute_sync_passes_refresh_mode_to_runner()
    test_finalize_run_control_clears_stale_running_domain()
    print("test_run_state: ok")


if __name__ == "__main__":
    main()
