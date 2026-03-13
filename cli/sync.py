from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import Any

from cli.doctor import run_init
from cli.errors import ResumeStateError, RuntimeCommandError
from pipeline.pipeline import DB_PATH, PipelineRunner
from pipeline.run_control import ensure_run_control, finalize_run_control
from pipeline.run_state import (
    create_run_state,
    deserialize_seeds,
    ensure_run_state_dir,
    latest_run_state,
    load_run_state,
    mark_run_completed,
    mark_stage_completed,
    mark_stage_failed,
    mark_stage_started,
    save_run_state,
    serialize_seed,
)
from pipeline.utils import utcnow_iso


def _runner_config_overrides(options: dict[str, Any]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    headless = str(options.get("crawlee_headless") or "").strip().lower()
    if headless in {"on", "off"}:
        overrides["crawlee_headless"] = headless == "on"
    return overrides


def _runner_factory_kwargs(*, args, options: dict[str, Any]) -> dict[str, Any]:
    return {
        "db_timeout_ms": getattr(args, "db_timeout_ms", None),
        "config_overrides": _runner_config_overrides(options),
        "crawl_mode": str(options.get("crawl_mode") or "full"),
    }


def _build_runner(run_id: str, *, db_path: str, seeds_path: str, args, options: dict[str, Any], runner_factory=PipelineRunner) -> PipelineRunner:
    runner = runner_factory(
        seeds=seeds_path,
        db_path=db_path,
        **_runner_factory_kwargs(args=args, options=options),
    )
    runner.job_id = run_id
    return runner


def _sync_options_from_args(args) -> dict[str, Any]:
    return {
        "seed_limit": getattr(args, "max", None),
        "export_limit": getattr(args, "limit", 100),
        "crawl_mode": getattr(args, "crawl_mode", "full"),
        "crawlee_headless": getattr(args, "crawlee_headless", None),
    }


def _load_requested_run_state(resume_value: str, checkpoint_dir: str | None) -> dict[str, Any]:
    if resume_value == "latest":
        state = latest_run_state(checkpoint_dir)
        if not state:
            raise ResumeStateError("No checkpoint is available to resume.")
        return state
    try:
        return load_run_state(resume_value, checkpoint_dir)
    except FileNotFoundError as exc:
        raise ResumeStateError(f"Checkpoint not found for run_id={resume_value}") from exc


def execute_sync(args, *, runner_factory=PipelineRunner) -> dict[str, Any]:
    checkpoint_dir = ensure_run_state_dir(getattr(args, "checkpoint_dir", None))
    resume_value = getattr(args, "resume", None)
    started_at = utcnow_iso()

    if resume_value:
        state = _load_requested_run_state(resume_value, checkpoint_dir)
        if state.get("status") == "completed":
            raise ResumeStateError(f"Run {state['run_id']} is already completed.")
        options = dict(state.get("options") or {})
        run_id = str(state["run_id"])
        seeds_path = str(state.get("seeds_path") or getattr(args, "seeds", "seed_packs/nj/seed_pack.json"))
        db_path = str(state.get("db_path") or getattr(args, "db", DB_PATH))
    else:
        options = _sync_options_from_args(args)
        bootstrap = runner_factory(
            seeds=getattr(args, "seeds", "seed_packs/nj/seed_pack.json"),
            db_path=getattr(args, "db", DB_PATH),
            **_runner_factory_kwargs(args=args, options=options),
        )
        run_id = getattr(args, "run_id", None) or bootstrap.job_id
        seeds_path = bootstrap.seeds_path
        db_path = str(Path(getattr(args, "db", DB_PATH)).expanduser().resolve())
        state = create_run_state(
            run_id=run_id,
            command="sync",
            db_path=db_path,
            config_path=str(Path(bootstrap.config.config_path).resolve()),
            seeds_path=seeds_path,
            crawl_mode=str(options.get("crawl_mode") or "full"),
            options=options,
        )
        save_run_state(state, checkpoint_dir)

    ensure_run_control(run_id, checkpoint_dir)
    runner = _build_runner(
        run_id,
        db_path=db_path,
        seeds_path=seeds_path,
        args=args,
        options=options,
        runner_factory=runner_factory,
    )
    state["db_path"] = db_path
    state["seeds_path"] = seeds_path
    report = dict(state.get("report") or {})
    discovered_seeds = deserialize_seeds(state.get("discovery_seeds"))

    try:
        if state["stages"]["seed_ingest"]["status"] != "completed":
            mark_stage_started(state, "seed_ingest")
            save_run_state(state, checkpoint_dir)
            ingest_result = runner.run_seed_ingest(seed_limit=options.get("seed_limit"))
            discovered_seeds = runner._load_seeds(options.get("seed_limit"))
            state["seed_counts"] = {"discovery": len(discovered_seeds), "monitor": 0}
            state["discovery_seeds"] = [serialize_seed(seed) for seed in discovered_seeds]
            mark_stage_completed(state, "seed_ingest", ingest_result)
            save_run_state(state, checkpoint_dir)

        if state["stages"]["crawl"]["status"] != "completed":
            mark_stage_started(state, "crawl")
            save_run_state(state, checkpoint_dir)
            fetched = runner.run_fetch(seeds=discovered_seeds, run_state_dir=checkpoint_dir)
            mark_stage_completed(state, "crawl", {"fetched_results": len(fetched), "metrics": copy.deepcopy(runner.metrics.snapshot())})
            save_run_state(state, checkpoint_dir)
        else:
            fetched = []

        if state["stages"]["extract"]["status"] != "completed":
            mark_stage_started(state, "extract")
            save_run_state(state, checkpoint_dir)
            extracted = runner.run_extract(fetched=fetched if fetched else None, since=state["stages"]["crawl"].get("started_at"))
            mark_stage_completed(state, "extract", {"extracted_records": extracted})
            save_run_state(state, checkpoint_dir)

        if state["stages"]["resolve"]["status"] != "completed":
            mark_stage_started(state, "resolve")
            save_run_state(state, checkpoint_dir)
            resolved = runner.run_resolve()
            mark_stage_completed(state, "resolve", resolved)
            save_run_state(state, checkpoint_dir)

        if state["stages"]["score"]["status"] != "completed":
            mark_stage_started(state, "score")
            save_run_state(state, checkpoint_dir)
            scores = runner.run_score()
            mark_stage_completed(state, "score", {"scored_records": scores})
            save_run_state(state, checkpoint_dir)

        if state["stages"]["qa"]["status"] != "completed":
            mark_stage_started(state, "qa")
            save_run_state(state, checkpoint_dir)
            qa_result = runner.run_qa()
            mark_stage_completed(state, "qa", qa_result)
            save_run_state(state, checkpoint_dir)

        if state["stages"]["export"]["status"] != "completed":
            mark_stage_started(state, "export")
            save_run_state(state, checkpoint_dir)
            report = runner.run_export(limit=int(options.get("export_limit") or 100))
            report.update(
                {
                    "status": "completed",
                    "started_at_utc": started_at,
                    "completed_at_utc": utcnow_iso(),
                    "crawl_mode": options.get("crawl_mode"),
                    "seed_counts": state.get("seed_counts", {}),
                }
            )
            runner._write_last_run_manifest(report)
            mark_stage_completed(state, "export", report)
            save_run_state(state, checkpoint_dir)

        summary = {
            "run_id": run_id,
            "discovered": len(discovered_seeds),
            "fetched": int(state["stages"]["crawl"]["details"].get("fetched_results", 0)),
            "extracted": int(state["stages"]["extract"]["details"].get("extracted_records", 0)),
            "resolved": int(state["stages"]["resolve"]["details"].get("resolved_count", 0)),
            "approved": int(state["stages"]["qa"]["details"].get("approved_records", 0)),
            "queued": int(state["stages"]["qa"]["details"].get("queued_records", 0)),
            "outreach_ready": int(state["stages"]["qa"]["details"].get("outreach_ready_records", 0)),
            "exported": int((report or {}).get("record_count", 0)),
            "sales_exported": int((report or {}).get("sales_count", 0)),
        }
        checkpoint_path = save_run_state(state, checkpoint_dir)
        mark_run_completed(state, summary=summary, report=report)
        checkpoint_path = save_run_state(state, checkpoint_dir)
        finalize_run_control(run_id, status="completed", base_dir=checkpoint_dir, replace_running_with="stopped")
        return {
            "run_id": run_id,
            "checkpoint_path": str(checkpoint_path),
            "recovery_pointer": state.get("recovery_pointer"),
            "summary": summary,
            "report": report,
        }
    except Exception as exc:
        current_stage = state.get("recovery_pointer") or "sync"
        if current_stage in state.get("stages", {}):
            mark_stage_failed(state, current_stage, code="sync_failed", message=str(exc))
        save_run_state(state, checkpoint_dir)
        finalize_run_control(run_id, status="failed", base_dir=checkpoint_dir, replace_running_with="failed", message=str(exc))
        raise


def execute_tail(args, *, runner_factory=PipelineRunner) -> dict[str, Any]:
    iterations = int(getattr(args, "iterations", 0) or 0)
    interval_seconds = max(1, int(getattr(args, "interval_seconds", 300) or 300))
    completed_runs: list[dict[str, Any]] = []
    count = 0
    while True:
        sync_args = copy.deepcopy(args)
        sync_args.resume = None
        sync_args.run_id = None
        result = execute_sync(sync_args, runner_factory=runner_factory)
        completed_runs.append({"run_id": result["run_id"], "summary": result.get("summary", {})})
        count += 1
        if iterations > 0 and count >= iterations:
            break
        time.sleep(interval_seconds)
    return {"iterations": count, "interval_seconds": interval_seconds, "runs": completed_runs}


def execute_export(args, *, runner_factory=PipelineRunner) -> dict[str, Any]:
    runner = runner_factory(
        db_path=getattr(args, "db", DB_PATH),
        db_timeout_ms=getattr(args, "db_timeout_ms", None),
    )
    result = runner.run_export(limit=int(getattr(args, "limit", 100) or 100))
    return result


def execute_init(args) -> dict[str, Any]:
    return run_init(
        db_path=getattr(args, "db", DB_PATH),
        config_path=getattr(args, "config", None),
        run_state_dir=getattr(args, "checkpoint_dir", None),
        db_timeout_ms=getattr(args, "db_timeout_ms", None),
    )
