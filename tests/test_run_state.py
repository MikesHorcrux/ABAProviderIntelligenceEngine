#!/usr/bin/env python3.11
from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace

from cli.sync import execute_sync
from pipeline.run_state import (
    create_run_state,
    load_run_state,
    mark_stage_completed,
    mark_stage_started,
    next_stage,
    save_run_state,
)
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.fetch import FetchResult


class FakeRunner:
    should_fail_once = True

    def __init__(self, seeds=None, db_path=None):
        self.seeds_path = seeds or "seeds.csv"
        self.db_path = Path(db_path)
        self.job_id = "fake-job"
        self.logger = None
        self.metrics = None
        self.config = SimpleNamespace(
            config_path=str(self.db_path.parent / "crawler_config.json"),
            growth_max_pages_per_domain=0,
            growth_max_total_pages=0,
            growth_max_depth=0,
            monitor_max_pages_per_domain=1,
            monitor_max_total_pages=1,
            monitor_max_depth=0,
            require_fetch_success_gate=True,
            require_net_new_gate=False,
            weekly_new_lead_target=100,
            growth_window_days=7,
            enforce_growth_governor=True,
            agent_research_enabled=True,
            agent_research_limit=25,
            agent_research_min_score=48,
            agent_research_paths=["/about", "/team"],
            crawlee_headless=True,
            crawlee_proxy_urls=[],
            crawlee_max_browser_pages_per_domain=5,
            crawlee_domain_policies_file="fetch_policies.json",
        )

    def _growth_governor(self, *_args, **_kwargs):
        return {
            "enabled": True,
            "mode": "manual",
            "target": 100,
            "window_days": 7,
            "observed_new_leads": 0,
            "manifest_observed_new_leads": 0,
            "db_observed_new_leads": 0,
            "shortfall": 100,
            "remaining": 100,
            "requested_discovery_limit": 1,
            "discovery_limit": 1,
        }

    def _intake_inbound_discovery_seeds(self):
        return {"inbound_rows": 0, "added": 0, "skipped": 0}

    def _build_seed_plan(self, **_kwargs):
        return ([DiscoverySeed(name="Resume Seed", website="https://resume.example", state="CA", market="CA")], [])

    def _previous_run_started_at(self):
        return None

    def run_fetch(self, **_kwargs):
        self.metrics.inc("pages_fetched")
        return [
            FetchResult(
                job_pk="job_resume",
                seed_name="Resume Seed",
                seed_state="CA",
                seed_market="CA",
                seed_website="https://resume.example",
                target_url="https://resume.example",
                normalized_url="https://resume.example",
                status_code=200,
                content="<html>ok</html>",
                content_hash="hash",
                fetched_at="2026-03-08T00:00:00",
            )
        ]

    def run_enrich(self, **_kwargs):
        if FakeRunner.should_fail_once:
            FakeRunner.should_fail_once = False
            raise RuntimeError("synthetic enrich failure")
        self.metrics.inc("parse_success")
        self.metrics.inc("locations_enriched")
        return ["loc_resume"]

    def run_score(self):
        return 0

    def run_lead_research(self, **_kwargs):
        self.metrics.inc("agent_researched")
        self.metrics.inc("agent_enhanced")
        return {
            "enabled": True,
            "researched_locations": 1,
            "ready_locations": 0,
            "enhanced_locations": 1,
            "research_needed_locations": 0,
        }

    def _run_reliability_gate(self, *_args, **_kwargs):
        return {"passed": True, "failed": False, "reason": ""}

    def run_export(self, **_kwargs):
        research = self.db_path.parent / "research_queue.csv"
        research.write_text("company_name,website\nResume Seed,resume.example\n", encoding="utf-8")
        agent_research = self.db_path.parent / "agent_research_queue.csv"
        agent_research.write_text("company_name,website\nResume Seed,resume.example\n", encoding="utf-8")
        return {
            "outreach": {"count": 0},
            "research": str(research),
            "agent_research": str(agent_research),
            "merge_suggestions": "",
            "quality": {},
            "new_leads": "",
            "buying_signal_watchlist": "",
            "discovery_metrics": {
                "new_leads_count": 1,
                "callable_leads_count": 0,
            },
        }

    def _evaluate_net_new_gate(self, report):
        return {
            "passed": True,
            "failed": False,
            "reason": "passed",
            "new_leads_count": report["discovery_metrics"]["new_leads_count"],
        }

    def _write_last_run_manifest(self, _payload):
        return None

    def _write_daily_growth_summary(self, _payload):
        return None


def test_run_state_round_trip() -> None:
    with tempfile.TemporaryDirectory() as td:
        run_dir = Path(td)
        state = create_run_state(
            run_id="run-1",
            command="sync",
            db_path=str(run_dir / "db.sqlite"),
            config_path=str(run_dir / "crawler_config.json"),
            seeds_path="seeds.csv",
            crawl_mode="growth",
            options={"seed_limit": 1},
        )
        mark_stage_started(state, "discovery")
        mark_stage_completed(state, "discovery", {"seed_count": 1})
        path = save_run_state(state, run_dir)
        loaded = load_run_state("run-1", run_dir)

        assert path.exists()
        assert loaded["stages"]["discovery"]["status"] == "completed"
        assert next_stage(loaded) == "fetch"


def test_execute_sync_can_resume_from_checkpoint() -> None:
    FakeRunner.should_fail_once = True
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        checkpoint_dir = root / "checkpoints"
        db_path = root / "resume.db"
        args = SimpleNamespace(
            db=str(db_path),
            seeds="seeds.csv",
            max=1,
            crawl_mode="growth",
            discovery_limit=1,
            monitor_limit=None,
            stale_days=None,
            growth_max_pages=1,
            growth_max_total=1,
            growth_max_depth=0,
            monitor_max_pages=None,
            monitor_max_total=None,
            monitor_max_depth=None,
            export_tier="A",
            export_limit=10,
            research_limit=10,
            new_limit=10,
            signal_limit=10,
            weekly_lead_target=None,
            growth_window_days=None,
            growth_governor=None,
            enforce_fetch_gate=None,
            crawlee_headless=None,
            crawlee_proxy_urls=None,
            crawlee_max_browser_pages=None,
            crawlee_domain_policies_file=None,
            run_id="resume-test",
            resume=None,
            checkpoint_dir=str(checkpoint_dir),
            config=None,
        )

        try:
            execute_sync(args, runner_factory=FakeRunner)
            raise AssertionError("sync should have failed on the first enrich attempt")
        except RuntimeError as exc:
            assert "synthetic enrich failure" in str(exc)

        state = load_run_state("resume-test", checkpoint_dir)
        assert state["stages"]["enrich"]["status"] == "failed"

        args.resume = "resume-test"
        result = execute_sync(args, runner_factory=FakeRunner)
        resumed = load_run_state("resume-test", checkpoint_dir)

        assert result["summary"]["discovered"] == 1
        assert resumed["status"] == "completed"
        assert resumed["stages"]["research"]["status"] == "completed"
        assert resumed["stages"]["export"]["status"] == "completed"


def main() -> None:
    test_run_state_round_trip()
    test_execute_sync_can_resume_from_checkpoint()
    print("test_run_state: ok")


if __name__ == "__main__":
    main()
