from __future__ import annotations

from typing import Any

from cli.errors import DataValidationError


class PolicyEngine:
    MAX_AGENT_SEEDS = 3
    MAX_AGENT_EXPORT_LIMIT = 25
    ALLOWED_TOOLS = {
        "doctor",
        "sync",
        "resume",
        "status",
        "search",
        "control_show",
        "control_apply",
        "export",
        "sql",
    }
    ALLOWED_CONTROL_ACTIONS = {
        "quarantine-seed",
        "suppress-prefix",
        "cap-domain",
        "stop-domain",
        "clear-domain",
    }

    def validate(self, tool_name: str, arguments: dict[str, Any]) -> None:
        if tool_name not in self.ALLOWED_TOOLS:
            raise DataValidationError(f"Unsupported agent tool: {tool_name}")
        reason = str(arguments.get("reason") or "").strip()
        if not reason:
            raise DataValidationError("Agent tool calls must include a non-empty reason.")
        if tool_name == "sync":
            self._validate_sync(arguments)
        if tool_name == "resume":
            self._validate_resume(arguments)
        if tool_name == "control_apply":
            self._validate_control(arguments)
        if tool_name == "sql":
            query = str(arguments.get("query") or "").strip().lower()
            if not query.startswith(("select", "with", "pragma")):
                raise DataValidationError("Agent SQL must be read-only.")

    def _validate_sync(self, arguments: dict[str, Any]) -> None:
        crawl_mode = str(arguments.get("crawl_mode") or "refresh").strip().lower()
        if crawl_mode != "refresh":
            raise DataValidationError("Agent sync runs must use crawl_mode='refresh'.")

        seed_limit = int(arguments.get("max") or 2)
        if seed_limit <= 0 or seed_limit > self.MAX_AGENT_SEEDS:
            raise DataValidationError(
                f"Agent sync seed_limit must be between 1 and {self.MAX_AGENT_SEEDS}."
            )

        export_limit = int(arguments.get("limit") or 15)
        if export_limit <= 0 or export_limit > self.MAX_AGENT_EXPORT_LIMIT:
            raise DataValidationError(
                f"Agent sync export limit must be between 1 and {self.MAX_AGENT_EXPORT_LIMIT}."
            )

    def _validate_resume(self, arguments: dict[str, Any]) -> None:
        export_limit = int(arguments.get("limit") or 15)
        if export_limit <= 0 or export_limit > self.MAX_AGENT_EXPORT_LIMIT:
            raise DataValidationError(
                f"Agent resume export limit must be between 1 and {self.MAX_AGENT_EXPORT_LIMIT}."
            )

    def _validate_control(self, arguments: dict[str, Any]) -> None:
        action = str(arguments.get("action") or "").strip()
        if action not in self.ALLOWED_CONTROL_ACTIONS:
            raise DataValidationError(f"Unsupported control action: {action}")
        if not str(arguments.get("domain") or "").strip():
            raise DataValidationError("Control actions require a domain.")
        if action == "suppress-prefix" and not str(arguments.get("prefix") or "").strip():
            raise DataValidationError("suppress-prefix requires a prefix.")
        if action == "cap-domain" and int(arguments.get("max_pages") or 0) <= 0:
            raise DataValidationError("cap-domain requires a positive max_pages value.")
