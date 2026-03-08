from __future__ import annotations

import json
from typing import Any


CLI_SCHEMA_VERSION = "cli.v1"


def success_payload(command: str, *, data: dict[str, Any] | None = None, message: str = "") -> dict[str, Any]:
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "command": command,
        "ok": True,
        "message": message,
        "data": data or {},
    }


def error_payload(command: str, *, code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": CLI_SCHEMA_VERSION,
        "command": command,
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        },
    }


def emit_payload(payload: dict[str, Any], *, output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(payload, indent=2, default=str))
        return

    if payload.get("ok") is False:
        error = payload.get("error", {})
        print(f"ERROR [{error.get('code', 'error')}]: {error.get('message', '')}")
        details = error.get("details", {})
        if isinstance(details, dict):
            for key, value in details.items():
                print(f"{key}: {value}")
        return

    message = str(payload.get("message") or "").strip()
    if message:
        print(message)

    data = payload.get("data", {})
    if not isinstance(data, dict):
        print(data)
        return

    _emit_plain_mapping(data)


def _emit_plain_mapping(mapping: dict[str, Any], prefix: str = "") -> None:
    for key, value in mapping.items():
        label = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            print(f"{label}:")
            _emit_plain_mapping(value, prefix=f"{label}.")
            continue
        if isinstance(value, list):
            print(f"{label}:")
            for item in value:
                if isinstance(item, dict):
                    print(f"- {json.dumps(item, sort_keys=True, default=str)}")
                else:
                    print(f"- {item}")
            continue
        print(f"{label}: {value}")
