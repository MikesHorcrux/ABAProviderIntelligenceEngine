from __future__ import annotations

import argparse
import sys
from typing import Sequence

from cli.app import main as canonical_main
from cli.errors import UsageError, classify_exception
from cli.output import emit_payload, error_payload


CANONICAL_COMMANDS = {
    "init",
    "doctor",
    "sync",
    "tail",
    "search",
    "status",
    "control",
    "sql",
    "export",
    "agent",
}
GLOBAL_VALUE_FLAGS = {"--db", "--db-timeout-ms", "--config", "--tenant", "--tenant-root-base"}
GLOBAL_BOOL_FLAGS = {"--json", "--plain"}


class AeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise UsageError(message)


def make_parser() -> argparse.ArgumentParser:
    parser = AeArgumentParser(
        prog="ae",
        description=(
            "Friendly wrapper for the provider-intel CLI. Canonical commands "
            "pass through unchanged; `run`, `session-status`, and "
            "`session-resume` map to the tenant agent surface."
        ),
    )
    parser.add_argument("--db", default=None)
    parser.add_argument("--db-timeout-ms", type=int, default=None)
    parser.add_argument("--config", default=None, help="Alternate crawler_config.json path.")
    parser.add_argument("--tenant", default=None, help="Tenant id for an isolated runtime root.")
    parser.add_argument("--tenant-root-base", default=None, help="Override the base directory for tenant runtime roots.")
    fmt = parser.add_mutually_exclusive_group()
    fmt.add_argument("--json", action="store_true", help="Emit strict machine-readable JSON.")
    fmt.add_argument("--plain", action="store_true", help="Emit line-oriented plain text output.")

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run a tenant agent session with a natural-language goal.")
    run.add_argument("goal_words", nargs="*")
    run.add_argument("--goal", default=None, help="Goal text. Use this or a trailing positional goal.")
    run.add_argument("--session-id", default=None, help="Reuse an existing agent session id.")
    run.add_argument("--model", default=None, help="Override the configured model for this run.")
    run.add_argument("--trace", action="store_true", help="Stream observable agent activity to stderr while the session runs.")

    session_status = sub.add_parser("session-status", help="Show stored agent-session status for a tenant.")
    session_status.add_argument("--session-id", default=None)

    session_resume = sub.add_parser("session-resume", help="Resume a stored tenant agent session.")
    session_resume.add_argument("--session-id", required=True)
    session_resume.add_argument("--model", default=None)
    session_resume.add_argument("--trace", action="store_true", help="Stream observable agent activity to stderr while the session runs.")
    return parser


def _extract_output_format(argv: Sequence[str]) -> str:
    output_format = "plain"
    for arg in argv:
        if arg == "--json":
            output_format = "json"
        elif arg == "--plain":
            output_format = "plain"
    return output_format


def _normalize_global_flags(argv: Sequence[str]) -> list[str]:
    globals_prefix: list[str] = []
    remainder: list[str] = []
    index = 0
    while index < len(argv):
        token = str(argv[index])
        if token in GLOBAL_BOOL_FLAGS:
            globals_prefix.append(token)
            index += 1
            continue
        if token in GLOBAL_VALUE_FLAGS:
            if index + 1 >= len(argv):
                raise UsageError(f"Missing value for {token}.")
            globals_prefix.extend([token, str(argv[index + 1])])
            index += 2
            continue
        remainder.append(token)
        index += 1
    return globals_prefix + remainder


def _find_command(argv: Sequence[str]) -> str | None:
    argv = _normalize_global_flags(argv)
    index = 0
    while index < len(argv):
        token = str(argv[index])
        if token in GLOBAL_BOOL_FLAGS or token in {"-h", "--help"}:
            index += 1
            continue
        if token in GLOBAL_VALUE_FLAGS:
            index += 2
            continue
        if token.startswith("-"):
            return None
        return token
    return None


def _append_global_flags(args, argv: list[str]) -> None:  # noqa: ANN001
    if args.db:
        argv.extend(["--db", str(args.db)])
    if args.db_timeout_ms is not None:
        argv.extend(["--db-timeout-ms", str(args.db_timeout_ms)])
    if args.config:
        argv.extend(["--config", str(args.config)])
    if args.tenant:
        argv.extend(["--tenant", str(args.tenant)])
    if args.tenant_root_base:
        argv.extend(["--tenant-root-base", str(args.tenant_root_base)])
    if args.json:
        argv.append("--json")
    elif args.plain:
        argv.append("--plain")


def _require_tenant(args, command_name: str) -> None:  # noqa: ANN001
    if not getattr(args, "tenant", None):
        raise UsageError(f"`ae {command_name}` requires --tenant.")


def _goal_from_args(args) -> str:  # noqa: ANN001
    goal_flag = str(args.goal or "").strip()
    goal_words = " ".join(str(item) for item in args.goal_words).strip()
    if goal_flag and goal_words:
        raise UsageError("Use either a trailing goal or `--goal`, not both.")
    goal = goal_flag or goal_words
    if not goal:
        raise UsageError("`ae run` requires a goal.")
    return goal


def translate_args(argv: Sequence[str]) -> list[str]:
    normalized = _normalize_global_flags(argv)
    command = _find_command(normalized)
    if command in CANONICAL_COMMANDS:
        return list(normalized)

    parser = make_parser()
    args = parser.parse_args(list(normalized))
    translated: list[str] = []
    _append_global_flags(args, translated)

    if args.command == "run":
        _require_tenant(args, "run")
        translated.extend(["agent", "run", "--goal", _goal_from_args(args)])
        if args.session_id:
            translated.extend(["--session-id", str(args.session_id)])
        if args.model:
            translated.extend(["--model", str(args.model)])
        if args.trace:
            translated.append("--trace")
        return translated

    if args.command == "session-status":
        _require_tenant(args, "session-status")
        translated.extend(["agent", "status"])
        if args.session_id:
            translated.extend(["--session-id", str(args.session_id)])
        return translated

    if args.command == "session-resume":
        _require_tenant(args, "session-resume")
        translated.extend(["agent", "resume", "--session-id", str(args.session_id)])
        if args.model:
            translated.extend(["--model", str(args.model)])
        if args.trace:
            translated.append("--trace")
        return translated

    raise UsageError(f"Unsupported ae command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    output_format = _extract_output_format(argv)
    command = _find_command(argv) or "ae"
    try:
        translated = translate_args(argv)
        return canonical_main(translated)
    except Exception as exc:
        cli_error = classify_exception(exc)
        payload = error_payload(command, code=cli_error.code, message=cli_error.message, details=cli_error.details)
        emit_payload(payload, output_format=output_format)
        return int(cli_error.exit_code)


__all__ = ["main", "make_parser", "translate_args"]
