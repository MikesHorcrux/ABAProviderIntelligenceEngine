from __future__ import annotations

from typing import Any

from cli.errors import DataValidationError


class PolicyEngine:
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
        if tool_name == "control_apply":
            self._validate_control(arguments)
        if tool_name == "sql":
            query = str(arguments.get("query") or "").strip().lower()
            if not query.startswith(("select", "with")):
                raise DataValidationError("Agent SQL must be read-only.")

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
