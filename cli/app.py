from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from cli.control import run_control_apply, run_control_show
from cli.doctor import run_doctor
from cli.errors import ExitCode, UsageError, classify_exception
from cli.output import emit_payload, error_payload, success_payload
from cli.query import run_search, run_sql, run_status
from cli.sync import execute_export, execute_init, execute_sync, execute_tail
from pipeline.pipeline import PipelineRunner


def _require_python_311() -> None:
    if sys.version_info < (3, 11):
        raise UsageError("Provider Intel requires Python 3.11+.")


class CliArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise UsageError(message)


def _add_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seeds", default="seed_packs/nj/seed_pack.json")
    parser.add_argument("--max", type=int, default=None)
    parser.add_argument("--crawl-mode", default="full", choices=["full", "refresh"])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--crawlee-headless", type=str, default=None, choices=["on", "off"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", default=None, help="Resume a checkpointed run by run_id or `latest`.")
    parser.add_argument("--checkpoint-dir", default=None)


def make_parser() -> argparse.ArgumentParser:
    parser = CliArgumentParser(description="Provider Intelligence agent-operable CLI")
    parser.add_argument("--db", default=str(Path(__file__).resolve().parents[1] / "data" / "provider_intel_v1.db"))
    parser.add_argument("--db-timeout-ms", type=int, default=30000)
    parser.add_argument("--config", default=None, help="Alternate crawler_config.json path.")
    fmt = parser.add_mutually_exclusive_group()
    fmt.add_argument("--json", action="store_true", help="Emit strict machine-readable JSON.")
    fmt.add_argument("--plain", action="store_true", help="Emit line-oriented plain text output.")

    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Initialize config, DB schema, state dirs, and checkpoints.")
    init.add_argument("--checkpoint-dir", default=None)

    doctor = sub.add_parser("doctor", help="Run preflight diagnostics for config, DB, runtime, and writable state.")
    doctor.add_argument("--checkpoint-dir", default=None)

    sync = sub.add_parser("sync", help="Run the provider intelligence pipeline with checkpointed stage resumability.")
    _add_sync_args(sync)

    tail = sub.add_parser("tail", help="Run sync in a loop for monitoring workflows.")
    _add_sync_args(tail)
    tail.add_argument("--interval-seconds", type=int, default=300)
    tail.add_argument("--iterations", type=int, default=0, help="0 means run continuously until interrupted.")

    search = sub.add_parser("search", help="Query local provider intelligence state or curated diagnostics presets.")
    search.add_argument("query", nargs="?", default=None)
    search.add_argument("--preset", choices=["failed-domains", "blocked-domains", "low-confidence-records", "review-queue", "contradictions", "outreach-ready"])
    search.add_argument("--limit", type=int, default=20)

    status = sub.add_parser("status", help="Summarize last manifest, checkpoint state, DB counts, and output snapshots.")
    status.add_argument("--run-id", default=None)
    status.add_argument("--checkpoint-dir", default=None)

    control = sub.add_parser("control", help="Inspect or apply bounded runtime interventions for an active or resumable run.")
    control.add_argument("--run-id", default="latest")
    control.add_argument("--checkpoint-dir", default=None)
    control_sub = control.add_subparsers(dest="control_action", required=True)
    control_sub.add_parser("show", help="Show the current run control state.")

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

    export = sub.add_parser("export", help="Export provider records, profiles, PDFs, evidence bundles, and review queue outputs.")
    export.add_argument("--limit", type=int, default=100)
    return parser


def _extract_output_format_flags(argv: list[str]) -> tuple[list[str], str]:
    filtered: list[str] = []
    requested_format = "plain"
    for arg in argv:
        if arg == "--json":
            requested_format = "json"
            continue
        if arg == "--plain":
            requested_format = "plain"
            continue
        filtered.append(arg)
    return filtered, requested_format


def _dispatch(args) -> dict[str, object]:
    if args.command == "init":
        return execute_init(args)
    if args.command == "doctor":
        return run_doctor(db_path=args.db, config_path=args.config, run_state_dir=args.checkpoint_dir)
    if args.command == "sync":
        return execute_sync(args)
    if args.command == "tail":
        return execute_tail(args)
    if args.command == "status":
        return run_status(db_path=args.db, run_id=args.run_id, run_state_dir=args.checkpoint_dir)
    if args.command == "control":
        if args.control_action == "show":
            return run_control_show(run_id=args.run_id, run_state_dir=args.checkpoint_dir)
        action_value = None
        if args.control_action == "suppress-prefix":
            action_value = args.prefix
        elif args.control_action == "cap-domain":
            action_value = args.max_pages
        return run_control_apply(run_id=args.run_id, run_state_dir=args.checkpoint_dir, action=args.control_action, domain=args.domain, value=action_value, reason=args.reason)
    if args.command == "sql":
        return run_sql(db_path=args.db, query=args.query_flag or args.query or "", limit=args.limit)
    if args.command == "search":
        return run_search(db_path=args.db, query=args.query, preset=args.preset, limit=args.limit)
    if args.command == "export":
        return execute_export(args)
    raise RuntimeError(f"Unsupported command: {args.command}")


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
            resolved = str(Path(args.config).expanduser().resolve())
            os.environ["PROVIDER_INTEL_CONFIG"] = resolved
            os.environ["CANNARADAR_CRAWLER_CONFIG"] = resolved
        command = args.command
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
