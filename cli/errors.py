from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from enum import IntEnum


class ExitCode(IntEnum):
    SUCCESS = 0
    USAGE = 2
    CONFIG = 10
    AUTH = 11
    NETWORK = 12
    DATA = 13
    STORAGE = 14
    RESUME = 15
    RUNTIME = 16
    FAILED = 17


@dataclass
class CliError(Exception):
    message: str
    exit_code: ExitCode
    code: str
    details: dict[str, object] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message


class ConfigError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.CONFIG, code="config_error", details=details or {})


class UsageError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.USAGE, code="usage_error", details=details or {})


class AuthError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.AUTH, code="auth_error", details=details or {})


class NetworkError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.NETWORK, code="network_error", details=details or {})


class DataValidationError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.DATA, code="data_validation_error", details=details or {})


class StorageError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.STORAGE, code="storage_error", details=details or {})


class ResumeStateError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.RESUME, code="resume_state_error", details=details or {})


class RuntimeCommandError(CliError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(message=message, exit_code=ExitCode.RUNTIME, code="runtime_error", details=details or {})


def classify_exception(exc: Exception) -> CliError:
    if isinstance(exc, CliError):
        return exc
    if isinstance(exc, FileNotFoundError):
        return ConfigError(str(exc))
    if isinstance(exc, PermissionError):
        return StorageError(str(exc))
    if isinstance(exc, sqlite3.Error):
        return StorageError(str(exc))
    if isinstance(exc, json.JSONDecodeError):
        return ConfigError(str(exc))

    message = str(exc)
    lowered = message.lower()
    if "token" in lowered or "auth" in lowered or "credential" in lowered:
        return AuthError(message)
    if "timeout" in lowered or "network" in lowered or "connection" in lowered or "rate limit" in lowered:
        return NetworkError(message)
    if "resume" in lowered or "checkpoint" in lowered:
        return ResumeStateError(message)
    if "validation" in lowered or "select-only" in lowered or "read-only" in lowered:
        return DataValidationError(message)
    return RuntimeCommandError(message or exc.__class__.__name__)


def exit_codes_payload() -> dict[str, object]:
    return {
        "schema_version": "cli.v1",
        "exit_codes": {
            "0": "success",
            "2": "usage_error",
            "10": "config_error",
            "11": "auth_error",
            "12": "network_error",
            "13": "data_validation_error",
            "14": "storage_error",
            "15": "resume_state_error",
            "16": "runtime_error",
            "17": "command_failed",
        },
    }
