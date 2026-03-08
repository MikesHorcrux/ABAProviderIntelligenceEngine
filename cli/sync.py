from __future__ import annotations

import copy
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cli.errors import ResumeStateError, RuntimeCommandError
from cli.doctor import run_init
from pipeline.db import connect_db
from pipeline.observability import Metrics, build_logger
from pipeline.pipeline import DB_PATH, SCHEMA_PATH, PipelineRunner
from pipeline.run_control import ensure_run_control, update_runtime_controls
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
from pipeline.utils import normalize_domain, utcnow_iso


def _apply_runner_overrides(runner: PipelineRunner, args) -> None:
    runner.max_pages = getattr(args, "max", None)
    if getattr(args, "weekly_lead_target", None) is not None:
        runner.config.weekly_new_lead_target = args.weekly_lead_target
    if getattr(args, "growth_window_days", None) is not None:
        runner.config.growth_window_days = args.growth_window_days
    if getattr(args, "growth_governor", None) is not None:
        runner.config.enforce_growth_governor = args.growth_governor == "on"
    if getattr(args, "enforce_fetch_gate", None) is not None:
        runner.config.require_fetch_success_gate = args.enforce_fetch_gate == "on"
    if getattr(args, "crawlee_headless", None) is not None:
        runner.config.crawlee_headless = args.crawlee_headless == "on"
    if getattr(args, "crawlee_proxy_urls", None) is not None:
        runner.config.crawlee_proxy_urls = [
            item.strip() for item in args.crawlee_proxy_urls.split(",") if item.strip()
        ]
    if getattr(args, "crawlee_max_browser_pages", None) is not None:
        runner.config.crawlee_max_browser_pages_per_domain = max(1, args.crawlee_max_browser_pages)
    if getattr(args, "crawlee_domain_policies_file", None) is not None:
        runner.config.crawlee_domain_policies_file = args.crawlee_domain_policies_file
    if getattr(args, "agent_research", None) is not None:
        runner.config.agent_research_enabled = args.agent_research == "on"
    if getattr(args, "agent_research_limit", None) is not None:
        runner.config.agent_research_limit = max(1, int(args.agent_research_limit))
    if getattr(args, "agent_research_min_score", None) is not None:
        runner.config.agent_research_min_score = max(0, min(100, int(args.agent_research_min_score)))


def _seed_domains(discovery_seeds, monitoring_seeds) -> list[str]:
    domains = {
        normalize_domain(seed.website)
        for seed in [*discovery_seeds, *monitoring_seeds]
        if normalize_domain(seed.website)
    }
    return sorted(domains)


def _sync_options_from_args(args) -> dict[str, Any]:
    return {
        "seed_limit": getattr(args, "max", None),
        "crawl_mode": getattr(args, "crawl_mode", "full"),
        "discovery_limit": getattr(args, "discovery_limit", None),
        "monitor_limit": getattr(args, "monitor_limit", None),
        "stale_days": getattr(args, "stale_days", None),
        "growth_max_pages": getattr(args, "growth_max_pages", None),
        "growth_max_total": getattr(args, "growth_max_total", None),
        "growth_max_depth": getattr(args, "growth_max_depth", None),
        "monitor_max_pages": getattr(args, "monitor_max_pages", None),
        "monitor_max_total": getattr(args, "monitor_max_total", None),
        "monitor_max_depth": getattr(args, "monitor_max_depth", None),
        "export_tier": getattr(args, "export_tier", "A"),
        "export_limit": getattr(args, "export_limit", 200),
        "research_limit": getattr(args, "research_limit", 200),
        "agent_research_limit": getattr(args, "agent_research_limit", None),
        "agent_research_min_score": getattr(args, "agent_research_min_score", None),
        "new_limit": getattr(args, "new_limit", 100),
        "signal_limit": getattr(args, "signal_limit", 200),
        "agent_research": getattr(args, "agent_research", None),
    }


def _build_runner(run_id: str, *, db_path: str, seeds_path: str, args, runner_factory=PipelineRunner) -> PipelineRunner:
    runner = runner_factory(seeds=seeds_path, db_path=db_path)
    runner.job_id = run_id
    runner.logger = build_logger(run_id, "pipeline")
    runner.metrics = Metrics(run_id)
    _apply_runner_overrides(runner, args)
    return runner


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


def _build_run_summary(
    *,
    db_path: str,
    run_id: str,
    seed_domains: list[str],
    report: dict[str, Any],
    metrics_snapshot: dict[str, int],
) -> dict[str, Any]:
    con = connect_db(db_path, SCHEMA_PATH)
    placeholders = ",".join("?" for _ in seed_domains) or "''"
    params = tuple(seed_domains)
    blocked_failures: list[dict[str, Any]] = []
    status_counts: list[dict[str, Any]] = []
    if seed_domains:
        blocked_failures = [
            dict(row)
            for row in con.execute(
                f"""
                SELECT seed_domain, last_status_code, last_run_status,
                       last_run_success_pages, last_run_failure_pages
                FROM seed_telemetry
                WHERE seed_domain IN ({placeholders})
                  AND (
                    last_run_success_pages = 0
                    OR last_status_code IN (401, 403, 429, 503)
                  )
                ORDER BY last_run_completed_at DESC, seed_domain ASC
                """,
                params,
            ).fetchall()
        ]
        status_counts = [
            dict(row)
            for row in con.execute(
                f"""
                SELECT status, COUNT(*) AS count
                FROM crawl_jobs
                WHERE seed_domain IN ({placeholders})
                GROUP BY status
                ORDER BY count DESC, status ASC
                """,
                params,
            ).fetchall()
        ]
    score_rows = int(
        con.execute("SELECT COUNT(*) AS c FROM lead_scores WHERE run_id=?", (run_id,)).fetchone()["c"]
    )
    con.close()

    return {
        "run_id": run_id,
        "discovered": len(seed_domains),
        "fetched": metrics_snapshot.get("pages_fetched", 0),
        "parsed": metrics_snapshot.get("parse_success", 0),
        "resolved": metrics_snapshot.get("locations_enriched", 0),
        "scored": score_rows,
        "researched": metrics_snapshot.get("agent_researched", 0),
        "exported": {
            "outreach": int((report.get("outreach") or {}).get("count", 0) or 0),
            "research": int((report.get("research_row_count") or 0)),
            "agent_research": int((report.get("agent_research_row_count") or 0)),
            "new_leads": int((report.get("discovery_metrics") or {}).get("new_leads_count", 0) or 0),
            "callable_leads": int((report.get("discovery_metrics") or {}).get("callable_leads_count", 0) or 0),
        },
        "seed_status_counts": status_counts,
        "blocked_or_failed": {
            "count": len(blocked_failures),
            "domains": blocked_failures[:10],
        },
        "metrics": metrics_snapshot,
    }


def _no_work_report(
    *,
    started_at: str,
    crawl_mode: str,
    intake_stats: dict[str, Any],
    governor: dict[str, Any],
) -> dict[str, Any]:
    return {
        "outreach": "",
        "research": "",
        "agent_research": "",
        "merge_suggestions": "",
        "quality": {},
        "new_leads": "",
        "buying_signal_watchlist": "",
        "status": "no_work",
        "growth_governor": governor,
        "started_at_utc": started_at,
        "completed_at_utc": utcnow_iso(),
        "crawl_mode": crawl_mode,
        "seed_counts": {"discovery": 0, "monitor": 0},
        "seed_intake": intake_stats,
        "reliability_gate": {"passed": True, "failed": False, "reason": "no_seeds"},
        "net_new_gate": {"passed": True, "failed": False, "reason": "no_work", "new_leads_count": 0},
    }


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
        seeds_path = str(state.get("seeds_path") or getattr(args, "seeds", "seeds.csv"))
        db_path = str(state.get("db_path") or getattr(args, "db", DB_PATH))
    else:
        options = _sync_options_from_args(args)
        bootstrap = runner_factory(seeds=getattr(args, "seeds", "seeds.csv"), db_path=getattr(args, "db", DB_PATH))
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
    runner = _build_runner(run_id, db_path=db_path, seeds_path=seeds_path, args=args, runner_factory=runner_factory)
    state["db_path"] = db_path
    state["seeds_path"] = seeds_path

    discovery_seeds = deserialize_seeds(state.get("discovery_seeds"))
    monitoring_seeds = deserialize_seeds(state.get("monitoring_seeds"))
    seed_domains = _seed_domains(discovery_seeds, monitoring_seeds)
    report = dict(state.get("report") or {})

    try:
        if state["stages"]["discovery"]["status"] != "completed":
            mark_stage_started(state, "discovery")
            save_run_state(state, checkpoint_dir)

            con = connect_db(runner.db_path, SCHEMA_PATH)
            governor = runner._growth_governor(
                con,
                crawl_mode=str(options.get("crawl_mode") or "full"),
                requested_discovery_limit=options.get("discovery_limit") or options.get("seed_limit"),
            )
            con.close()

            intake_stats = runner._intake_inbound_discovery_seeds()
            requested_limit = options.get("discovery_limit") or options.get("seed_limit")
            discovery_limit = governor.get("discovery_limit")
            if requested_limit is not None and discovery_limit is not None:
                discovery_limit = min(int(requested_limit), int(discovery_limit))
            if discovery_limit is None and options.get("seed_limit") is not None:
                discovery_limit = options.get("seed_limit")

            discovery_seeds, monitoring_seeds = runner._build_seed_plan(
                crawl_mode=str(options.get("crawl_mode") or "full"),
                discovery_limit=discovery_limit,
                monitor_limit=options.get("monitor_limit"),
                stale_days=options.get("stale_days"),
            )
            seed_domains = _seed_domains(discovery_seeds, monitoring_seeds)

            previous_run = runner._previous_run_started_at()
            export_since = previous_run or (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")

            state["governor"] = governor
            state["seed_intake"] = intake_stats
            state["seed_counts"] = {"discovery": len(discovery_seeds), "monitor": len(monitoring_seeds)}
            state["discovery_seeds"] = [serialize_seed(seed) for seed in discovery_seeds]
            state["monitoring_seeds"] = [serialize_seed(seed) for seed in monitoring_seeds]
            state["export_since"] = export_since

            mark_stage_completed(
                state,
                "discovery",
                {
                    "seed_counts": state["seed_counts"],
                    "seed_intake": intake_stats,
                    "governor": governor,
                    "export_since": export_since,
                },
            )
            save_run_state(state, checkpoint_dir)
        else:
            governor = dict(state.get("governor") or {})
            intake_stats = dict(state.get("seed_intake") or {})

        if not discovery_seeds and not monitoring_seeds:
            report = _no_work_report(
                started_at=started_at,
                crawl_mode=str(options.get("crawl_mode") or "full"),
                intake_stats=intake_stats,
                governor=governor,
            )
            state["report"] = report
            mark_run_completed(state, summary={"run_id": run_id, "discovered": 0}, report=report)
            save_run_state(state, checkpoint_dir)
            update_runtime_controls(
                run_id,
                lambda payload: payload.update({"status": "completed"}),
                base_dir=checkpoint_dir,
            )
            runner._write_last_run_manifest(report)
            runner._write_daily_growth_summary(report)
            return {
                "run_id": run_id,
                "checkpoint_path": str(save_run_state(state, checkpoint_dir)),
                "report": report,
                "summary": state["summary"],
                "recovery_pointer": state.get("recovery_pointer"),
            }

        fetched = []
        if state["stages"]["fetch"]["status"] != "completed":
            mark_stage_started(state, "fetch", {"seed_domains": seed_domains})
            save_run_state(state, checkpoint_dir)
            if discovery_seeds:
                fetched.extend(
                    runner.run_fetch(
                        seeds=discovery_seeds,
                        max_pages_per_domain=options.get("growth_max_pages") or (runner.config.growth_max_pages_per_domain or None),
                        max_total_pages=options.get("growth_max_total") or (runner.config.growth_max_total_pages or None),
                        max_depth=options.get("growth_max_depth") or (runner.config.growth_max_depth or None),
                        run_state_dir=checkpoint_dir,
                    )
                )
            if monitoring_seeds:
                fetched.extend(
                    runner.run_fetch(
                        seeds=monitoring_seeds,
                        max_pages_per_domain=options.get("monitor_max_pages") or runner.config.monitor_max_pages_per_domain,
                        max_total_pages=options.get("monitor_max_total") or runner.config.monitor_max_total_pages,
                        max_depth=options.get("monitor_max_depth") or runner.config.monitor_max_depth,
                        run_state_dir=checkpoint_dir,
                    )
                )
            mark_stage_completed(
                state,
                "fetch",
                {
                    "fetched_results": len(fetched),
                    "metrics": copy.deepcopy(runner.metrics.snapshot()),
                },
            )
            save_run_state(state, checkpoint_dir)

        if state["stages"]["enrich"]["status"] != "completed":
            mark_stage_started(state, "enrich")
            save_run_state(state, checkpoint_dir)
            if fetched:
                enriched_locations = runner.run_enrich(fetched=fetched)
            else:
                fetch_started_at = state["stages"]["fetch"].get("started_at") or ""
                enriched_locations = runner.run_enrich(since=fetch_started_at)
            mark_stage_completed(
                state,
                "enrich",
                {
                    "locations_enriched": len(enriched_locations),
                    "metrics": copy.deepcopy(runner.metrics.snapshot()),
                },
            )
            save_run_state(state, checkpoint_dir)

        if state["stages"]["score"]["status"] != "completed":
            mark_stage_started(state, "score")
            save_run_state(state, checkpoint_dir)
            scores_written = runner.run_score()
            mark_stage_completed(
                state,
                "score",
                {
                    "scores_written": scores_written,
                    "metrics": copy.deepcopy(runner.metrics.snapshot()),
                },
            )
            save_run_state(state, checkpoint_dir)

        if state["stages"]["research"]["status"] != "completed":
            mark_stage_started(state, "research")
            save_run_state(state, checkpoint_dir)
            research_result = runner.run_lead_research(
                since=str(state.get("export_since") or ""),
                limit=(
                    int(options.get("agent_research_limit"))
                    if options.get("agent_research_limit") not in (None, "")
                    else None
                ),
                min_score=(
                    int(options.get("agent_research_min_score"))
                    if options.get("agent_research_min_score") not in (None, "")
                    else None
                ),
            )
            mark_stage_completed(
                state,
                "research",
                {
                    **research_result,
                    "metrics": copy.deepcopy(runner.metrics.snapshot()),
                },
            )
            save_run_state(state, checkpoint_dir)

        if state["stages"]["export"]["status"] != "completed":
            mark_stage_started(state, "export")
            save_run_state(state, checkpoint_dir)

            con = connect_db(runner.db_path, SCHEMA_PATH)
            reliability = runner._run_reliability_gate(
                con,
                total_seeds=len(seed_domains),
                fetch_successes=int(state["stages"]["fetch"]["details"].get("fetched_results", 0)),
            )
            con.close()

            if not reliability["passed"] and runner.config.require_fetch_success_gate:
                report = {
                    "outreach": "",
                    "research": "",
                    "agent_research": "",
                    "merge_suggestions": "",
                    "quality": {},
                    "new_leads": "",
                    "buying_signal_watchlist": "",
                    "status": "failed",
                    "reliability_gate": reliability,
                    "growth_governor": state.get("governor", {}),
                    "started_at_utc": started_at,
                    "completed_at_utc": utcnow_iso(),
                    "crawl_mode": options.get("crawl_mode"),
                    "seed_counts": state.get("seed_counts", {}),
                    "seed_limit": options.get("discovery_limit") or options.get("seed_limit"),
                    "seed_intake": state.get("seed_intake", {}),
                }
                runner._write_last_run_manifest(report)
                runner._write_daily_growth_summary(report)
                mark_stage_failed(
                    state,
                    "export",
                    code="reliability_gate_failed",
                    message=f"Reliability gate failed: {reliability['reason']}",
                    details={"reliability_gate": reliability},
                )
                save_run_state(state, checkpoint_dir)
                raise RuntimeCommandError(f"Reliability gate failed: {reliability['reason']}")

            report = runner.run_export(
                tier=str(options.get("export_tier") or "A"),
                limit=int(options.get("export_limit") or 200),
                research_limit=int(options.get("research_limit") or 200),
                agent_research_limit=(
                    int(options.get("agent_research_limit"))
                    if options.get("agent_research_limit") not in (None, "")
                    else None
                ),
                since=str(state.get("export_since") or ""),
                new_limit=int(options.get("new_limit") or 100),
                signal_limit=int(options.get("signal_limit") or 200),
            )
            research_path = Path(str(report.get("research") or ""))
            agent_research_path = Path(str(report.get("agent_research") or ""))
            report["research_row_count"] = 0
            if research_path.exists():
                with research_path.open(encoding="utf-8") as f:
                    report["research_row_count"] = max(0, sum(1 for _ in f) - 1)
            report["agent_research_row_count"] = 0
            if agent_research_path.exists():
                with agent_research_path.open(encoding="utf-8") as f:
                    report["agent_research_row_count"] = max(0, sum(1 for _ in f) - 1)
            report.update(
                {
                    "status": "passed" if reliability["passed"] else "degraded",
                    "reliability_gate": reliability,
                    "growth_governor": state.get("governor", {}),
                    "started_at_utc": started_at,
                    "completed_at_utc": utcnow_iso(),
                    "crawl_mode": options.get("crawl_mode"),
                    "seed_counts": state.get("seed_counts", {}),
                    "seed_intake": state.get("seed_intake", {}),
                }
            )
            net_new_gate = runner._evaluate_net_new_gate(report)
            report["net_new_gate"] = net_new_gate
            if runner.config.require_net_new_gate and not net_new_gate["passed"]:
                report["status"] = "failed" if net_new_gate["failed"] else "degraded"
                runner._write_last_run_manifest(report)
                runner._write_daily_growth_summary(report)
                if net_new_gate["failed"]:
                    mark_stage_failed(
                        state,
                        "export",
                        code="net_new_gate_failed",
                        message="Net-new gate failed: new_leads_count == 0",
                        details={"net_new_gate": net_new_gate},
                    )
                    save_run_state(state, checkpoint_dir)
                    raise RuntimeCommandError("Net-new gate failed: new_leads_count == 0")
            runner._write_last_run_manifest(report)
            runner._write_daily_growth_summary(report)
            mark_stage_completed(
                state,
                "export",
                {
                    "report_status": report.get("status"),
                    "reliability_gate": reliability,
                    "net_new_gate": net_new_gate,
                },
            )
            save_run_state(state, checkpoint_dir)

        summary = _build_run_summary(
            db_path=db_path,
            run_id=run_id,
            seed_domains=seed_domains,
            report=report,
            metrics_snapshot=runner.metrics.snapshot(),
        )
        mark_run_completed(state, summary=summary, report=report)
        checkpoint_path = save_run_state(state, checkpoint_dir)
        update_runtime_controls(
            run_id,
            lambda payload: payload.update({"status": "completed"}),
            base_dir=checkpoint_dir,
        )
        return {
            "run_id": run_id,
            "checkpoint_path": str(checkpoint_path),
            "recovery_pointer": state.get("recovery_pointer"),
            "summary": summary,
            "report": report,
        }
    except Exception as exc:
        cli_message = str(exc)
        current_stage = state.get("recovery_pointer") or "sync"
        if current_stage in state.get("stages", {}):
            mark_stage_failed(
                state,
                current_stage,
                code="sync_failed",
                message=cli_message,
            )
        save_run_state(state, checkpoint_dir)
        update_runtime_controls(
            run_id,
            lambda payload: payload.update({"status": "failed"}),
            base_dir=checkpoint_dir,
        )
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
        completed_runs.append(
            {
                "run_id": result["run_id"],
                "status": (result.get("report") or {}).get("status"),
                "summary": result.get("summary", {}),
                "checkpoint_path": result.get("checkpoint_path"),
            }
        )
        count += 1
        if iterations > 0 and count >= iterations:
            break
        time.sleep(interval_seconds)

    return {
        "iterations": count,
        "interval_seconds": interval_seconds,
        "runs": completed_runs,
    }


def execute_export(args, *, runner_factory=PipelineRunner) -> dict[str, Any]:
    runner = runner_factory(db_path=getattr(args, "db", DB_PATH))
    kind = getattr(args, "kind", "all")
    if kind == "all":
        result = runner.run_export(
            tier=getattr(args, "tier", "A"),
            limit=getattr(args, "limit", 200),
            research_limit=getattr(args, "research_limit", 200),
            agent_research_limit=getattr(args, "agent_research_limit", None),
            since=getattr(args, "since", None),
            new_limit=getattr(args, "new_limit", 100),
            signal_limit=getattr(args, "signal_limit", 200),
        )
        research_path = Path(str(result.get("research") or ""))
        result["research_row_count"] = 0
        if research_path.exists():
            with research_path.open(encoding="utf-8") as f:
                result["research_row_count"] = max(0, sum(1 for _ in f) - 1)
        agent_research_path = Path(str(result.get("agent_research") or ""))
        result["agent_research_row_count"] = 0
        if agent_research_path.exists():
            with agent_research_path.open(encoding="utf-8") as f:
                result["agent_research_row_count"] = max(0, sum(1 for _ in f) - 1)
        return result
    if kind == "quality":
        return runner.run_quality()
    if kind == "outreach":
        return runner.run_export(
            tier=getattr(args, "tier", "A"),
            limit=getattr(args, "limit", 200),
            research_limit=0,
            agent_research_limit=0,
            new_limit=0,
            signal_limit=0,
        )
    if kind == "research":
        return runner.run_export(
            tier="C",
            limit=0,
            research_limit=getattr(args, "research_limit", 200),
            agent_research_limit=0,
            new_limit=0,
            signal_limit=0,
        )
    if kind == "agent-research":
        return runner.run_export(
            tier="C",
            limit=0,
            research_limit=0,
            agent_research_limit=getattr(args, "agent_research_limit", 200),
            since=getattr(args, "since", None),
            new_limit=0,
            signal_limit=0,
        )
    if kind == "new":
        return runner.run_export(
            tier="C",
            limit=0,
            research_limit=0,
            agent_research_limit=0,
            since=getattr(args, "since", None),
            new_limit=getattr(args, "new_limit", 100),
            signal_limit=0,
        )
    if kind == "signals":
        return runner.run_export(
            tier="C",
            limit=0,
            research_limit=0,
            agent_research_limit=0,
            since=getattr(args, "since", None),
            new_limit=0,
            signal_limit=getattr(args, "signal_limit", 200),
        )
    raise RuntimeCommandError(f"Unsupported export kind: {kind}")


def execute_init(args) -> dict[str, Any]:
    return run_init(
        db_path=getattr(args, "db", DB_PATH),
        config_path=getattr(args, "config", None),
        run_state_dir=getattr(args, "checkpoint_dir", None),
    )
