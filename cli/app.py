from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from cli.agent_runtime_ops import run_agent_external_research
from cli.control import run_control_apply, run_control_show
from cli.doctor import run_doctor
from cli.errors import ConfigError, ExitCode, UsageError, classify_exception
from cli.output import emit_payload, error_payload, success_payload
from cli.query import run_search, run_sql, run_status
from cli.sync import execute_export, execute_init, execute_sync, execute_tail
from pipeline.pipeline import PipelineRunner


def _require_python_311() -> None:
    if sys.version_info < (3, 11):
        raise ConfigError("CannaRadar requires Python 3.11+ for the agent-operable Crawlee runtime.")


class CliArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise UsageError(message)

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if status == 0:
            raise SystemExit(0)
        raise UsageError((message or "").strip() or "usage error")


def _add_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seeds", default="seeds.csv")
    parser.add_argument("--max", type=int, default=None)
    parser.add_argument("--crawl-mode", default="full", choices=["full", "balanced", "growth", "monitor"])
    parser.add_argument("--discovery-limit", type=int, default=None)
    parser.add_argument("--monitor-limit", type=int, default=None)
    parser.add_argument("--stale-days", type=int, default=None)
    parser.add_argument("--growth-max-pages", type=int, default=None)
    parser.add_argument("--growth-max-total", type=int, default=None)
    parser.add_argument("--growth-max-depth", type=int, default=None)
    parser.add_argument("--monitor-max-pages", type=int, default=None)
    parser.add_argument("--monitor-max-total", type=int, default=None)
    parser.add_argument("--monitor-max-depth", type=int, default=None)
    parser.add_argument("--export-tier", default="A")
    parser.add_argument("--export-limit", type=int, default=200)
    parser.add_argument("--research-limit", type=int, default=200)
    parser.add_argument("--agent-research", type=str, default=None, choices=["on", "off"])
    parser.add_argument("--agent-research-limit", type=int, default=None)
    parser.add_argument("--agent-research-min-score", type=int, default=None)
    parser.add_argument("--new-limit", type=int, default=100)
    parser.add_argument("--signal-limit", type=int, default=200)
    parser.add_argument("--weekly-lead-target", type=int, default=None)
    parser.add_argument("--growth-window-days", type=int, default=None)
    parser.add_argument("--growth-governor", type=str, default=None, choices=["on", "off"])
    parser.add_argument("--enforce-fetch-gate", type=str, default=None, choices=["on", "off"])
    parser.add_argument("--crawlee-headless", type=str, default=None, choices=["on", "off"])
    parser.add_argument("--crawlee-proxy-urls", default=None)
    parser.add_argument("--crawlee-max-browser-pages", type=int, default=None)
    parser.add_argument("--crawlee-browser-isolation", default=None, choices=["inline", "subprocess"])
    parser.add_argument("--crawlee-domain-policies-file", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", default=None, help="Resume a checkpointed run by run_id or `latest`.")
    parser.add_argument("--checkpoint-dir", default=None)


def make_parser() -> argparse.ArgumentParser:
    parser = CliArgumentParser(description="CannaRadar agent-operable CLI")
    parser.add_argument("--db", default=str(Path(__file__).resolve().parents[1] / "data" / "cannaradar_v1.db"))
    parser.add_argument("--db-timeout-ms", type=int, default=30000)
    parser.add_argument("--config", default=None, help="Alternate crawler_config.json path.")
    fmt = parser.add_mutually_exclusive_group()
    fmt.add_argument("--json", action="store_true", help="Emit strict machine-readable JSON.")
    fmt.add_argument("--plain", action="store_true", help="Emit line-oriented plain text output.")

    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Initialize config, DB schema, state dirs, and agent checkpoints.")
    init.add_argument("--checkpoint-dir", default=None)

    doctor = sub.add_parser("doctor", help="Run preflight diagnostics for config, DB, runtime, and writable state.")
    doctor.add_argument("--checkpoint-dir", default=None)

    sync = sub.add_parser("sync", help="Run the batch crawl pipeline with checkpointed stage resumability.")
    _add_sync_args(sync)

    tail = sub.add_parser("tail", help="Run sync in a loop for continuous monitoring workflows.")
    _add_sync_args(tail)
    tail.add_argument("--interval-seconds", type=int, default=300)
    tail.add_argument("--iterations", type=int, default=0, help="0 means run continuously until interrupted.")

    search = sub.add_parser("search", help="Query local lead state or curated diagnostics presets.")
    search.add_argument("query", nargs="?", default=None)
    search.add_argument("--preset", choices=["failed-domains", "blocked-domains", "stale-records", "low-confidence-leads", "research-needed"])
    search.add_argument("--limit", type=int, default=20)

    status = sub.add_parser("status", help="Summarize last manifest, checkpoint state, DB counts, and recent failures.")
    status.add_argument("--run-id", default=None)
    status.add_argument("--checkpoint-dir", default=None)

    control = sub.add_parser("control", help="Inspect or apply bounded runtime interventions for an active or resumable run.")
    control.add_argument("--run-id", default="latest")
    control.add_argument("--checkpoint-dir", default=None)
    control_sub = control.add_subparsers(dest="control_action", required=True)

    control_show = control_sub.add_parser("show", help="Show the current run control state.")

    control_quarantine = control_sub.add_parser("quarantine-seed", help="Quarantine a seed/domain for the run.")
    control_quarantine.add_argument("--domain", required=True)
    control_quarantine.add_argument("--reason", default="agent_quarantine")

    control_suppress = control_sub.add_parser("suppress-prefix", help="Suppress a path prefix for a domain.")
    control_suppress.add_argument("--domain", required=True)
    control_suppress.add_argument("--prefix", required=True)
    control_suppress.add_argument("--reason", default="agent_suppress_prefix")

    control_cap = control_sub.add_parser("cap-domain", help="Apply a lower per-domain page cap for the run.")
    control_cap.add_argument("--domain", required=True)
    control_cap.add_argument("--max-pages", type=int, required=True)
    control_cap.add_argument("--reason", default="agent_cap_domain")

    control_stop = control_sub.add_parser("stop-domain", help="Stop crawling a domain for the current run.")
    control_stop.add_argument("--domain", required=True)
    control_stop.add_argument("--reason", default="agent_stop_domain")

    control_clear = control_sub.add_parser("clear-domain", help="Clear manual controls for a domain.")
    control_clear.add_argument("--domain", required=True)
    control_clear.add_argument("--reason", default="agent_clear_domain")

    sql = sub.add_parser("sql", help="Execute a read-only SELECT/WITH query against the local SQLite state.")
    sql.add_argument("query", nargs="?", default=None)
    sql.add_argument("--query", dest="query_flag", default=None)
    sql.add_argument("--limit", type=int, default=200)

    export = sub.add_parser("export", help="Export outreach, intelligence, research, agent research, new leads, signals, or quality outputs.")
    export.add_argument("--kind", default="all", choices=["all", "outreach", "intelligence", "research", "agent-research", "new", "signals", "quality"])
    
    agent_external = sub.add_parser(
        "agent:external-research",
        help="Run provider-backed external research generation for lead packages.",
    )
    agent_external.add_argument("--out-dir", default=str(Path(__file__).resolve().parents[1] / "out"))
    agent_external.add_argument("--config-path", default=str(Path(__file__).resolve().parents[1] / "config" / "agent_runtime.json"))
    agent_external.add_argument("--limit", type=int, default=0)
    export.add_argument("--tier", default="A")
    export.add_argument("--limit", type=int, default=200)
    export.add_argument("--research-limit", type=int, default=200)
    export.add_argument("--agent-research-limit", type=int, default=200)
    export.add_argument("--new-limit", type=int, default=100)
    export.add_argument("--signal-limit", type=int, default=200)
    export.add_argument("--since", default=None)

    crawl = sub.add_parser("crawl:run", help=argparse.SUPPRESS)
    _add_sync_args(crawl)

    enrich = sub.add_parser("enrich:run", help=argparse.SUPPRESS)
    enrich.add_argument("--since", default=None)

    score = sub.add_parser("score:run", help=argparse.SUPPRESS)

    export_outreach = sub.add_parser("export:outreach", help=argparse.SUPPRESS)
    export_outreach.add_argument("--tier", default="A")
    export_outreach.add_argument("--limit", type=int, default=200)

    export_research = sub.add_parser("export:research", help=argparse.SUPPRESS)
    export_research.add_argument("--limit", type=int, default=200)

    export_intelligence = sub.add_parser("export:intelligence", help=argparse.SUPPRESS)
    export_intelligence.add_argument("--tier", default="A")
    export_intelligence.add_argument("--limit", type=int, default=100)

    export_new = sub.add_parser("export:new", help=argparse.SUPPRESS)
    export_new.add_argument("--since", default=None)
    export_new.add_argument("--limit", type=int, default=100)

    export_signals = sub.add_parser("export:signals", help=argparse.SUPPRESS)
    export_signals.add_argument("--since", default=None)
    export_signals.add_argument("--limit", type=int, default=200)

    quality = sub.add_parser("quality:report", help=argparse.SUPPRESS)
    schema = sub.add_parser("schema:check", help=argparse.SUPPRESS)
    schema.add_argument("--checkpoint-dir", default=None)
    return parser


def _canonical_command(command: str) -> str:
    mapping = {
        "crawl:run": "sync",
        "export:outreach": "export",
        "export:intelligence": "export",
        "export:research": "export",
        "export:new": "export",
        "export:signals": "export",
        "quality:report": "export",
        "schema:check": "doctor",
    }
    return mapping.get(command, command)


def _extract_output_format_flags(argv: list[str]) -> tuple[list[str], str]:
    filtered: list[str] = []
    requested_format = "plain"
    seen_json = False
    seen_plain = False

    for arg in argv:
        if arg == "--json":
            seen_json = True
            if seen_plain:
                raise UsageError("Choose either --json or --plain.")
            requested_format = "json"
            continue
        if arg == "--plain":
            seen_plain = True
            if seen_json:
                raise UsageError("Choose either --json or --plain.")
            requested_format = "plain"
            continue
        filtered.append(arg)

    return filtered, requested_format


def _dispatch(args) -> dict[str, object]:
    command = args.command

    if command == "init":
        return execute_init(args)
    if command == "doctor":
        return run_doctor(db_path=args.db, config_path=args.config, run_state_dir=args.checkpoint_dir)
    if command in {"sync", "crawl:run"}:
        return execute_sync(args)
    if command == "tail":
        return execute_tail(args)
    if command == "status":
        return run_status(db_path=args.db, run_id=args.run_id, run_state_dir=args.checkpoint_dir)
    if command == "control":
        if args.control_action == "show":
            return run_control_show(run_id=args.run_id, run_state_dir=args.checkpoint_dir)
        action_value = None
        if args.control_action == "suppress-prefix":
            action_value = args.prefix
        elif args.control_action == "cap-domain":
            action_value = args.max_pages
        return run_control_apply(
            run_id=args.run_id,
            run_state_dir=args.checkpoint_dir,
            action=args.control_action,
            domain=args.domain,
            value=action_value,
            reason=args.reason,
        )
    if command == "sql":
        query = args.query_flag or args.query
        return run_sql(db_path=args.db, query=query or "", limit=args.limit)
    if command == "search":
        return run_search(db_path=args.db, query=args.query, preset=args.preset, limit=args.limit)
    if command == "export":
        return execute_export(args)
    if command == "agent:external-research":
        return run_agent_external_research(out_dir=args.out_dir, config_path=args.config_path, limit=args.limit)
    if command == "export:outreach":
        args.kind = "outreach"
        args.research_limit = 0
        args.new_limit = 0
        args.signal_limit = 0
        return execute_export(args)
    if command == "export:research":
        args.kind = "research"
        args.research_limit = args.limit
        args.new_limit = 0
        args.signal_limit = 0
        return execute_export(args)
    if command == "export:intelligence":
        args.kind = "intelligence"
        args.research_limit = 0
        args.agent_research_limit = 0
        args.new_limit = 0
        args.signal_limit = 0
        return execute_export(args)
    if command == "export:new":
        args.kind = "new"
        args.new_limit = args.limit
        args.signal_limit = 0
        return execute_export(args)
    if command == "export:signals":
        args.kind = "signals"
        args.signal_limit = args.limit
        args.new_limit = 0
        return execute_export(args)
    if command == "quality:report":
        args.kind = "quality"
        return execute_export(args)
    if command == "enrich:run":
        runner = PipelineRunner(db_path=args.db)
        updated = runner.run_enrich(since=args.since)
        return {"locations_enriched": len(updated)}
    if command == "score:run":
        runner = PipelineRunner(db_path=args.db)
        scores = runner.run_score()
        return {"scores_written": scores}
    if command == "schema:check":
        doctor = run_doctor(db_path=args.db, config_path=args.config, run_state_dir=args.checkpoint_dir)
        return {
            "ok": doctor["ok"],
            "db_schema": next((item for item in doctor["checks"] if item["id"] == "db_schema"), {}),
            "summary": doctor["summary"],
        }
    raise RuntimeError(f"Unsupported command: {command}")


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    argv, output_format = _extract_output_format_flags(argv)
    command = "unknown"

    try:
        _require_python_311()
        parser = make_parser()
        args = parser.parse_args(argv)
        args.json = output_format == "json"
        args.plain = output_format == "plain"

        if args.config:
            os.environ["CANNARADAR_CRAWLER_CONFIG"] = str(Path(args.config).expanduser().resolve())

        command = _canonical_command(args.command)
        data = _dispatch(args)
        payload = success_payload(command, data=data, message=f"{command} completed")
        emit_payload(payload, output_format=output_format)
        return int(ExitCode.SUCCESS)
    except Exception as exc:
        cli_error = classify_exception(exc)
        payload = error_payload(command, code=cli_error.code, message=cli_error.message, details=cli_error.details)
        emit_payload(payload, output_format=output_format)
        return int(cli_error.exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
