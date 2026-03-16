from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from runtime_context import RuntimePaths, ensure_runtime_dirs


@dataclass(frozen=True)
class AgentConfig:
    provider: str = "openai"
    model: str = "gpt-5"
    max_turns: int = 8
    retry_limit: int = 2
    retry_backoff_seconds: float = 1.0
    request_timeout_seconds: int = 60
    autonomy_mode: str = "full_local_auto"
    default_client_id: str = "default"
    provider_options: dict[str, Any] = field(default_factory=lambda: {"base_url": "https://api.openai.com/v1/responses"})


def _config_payload(config: AgentConfig) -> dict[str, Any]:
    payload = asdict(config)
    provider_options = dict(payload.pop("provider_options", {}) or {})
    payload["providerOptions"] = provider_options
    return payload


def ensure_agent_config(paths: RuntimePaths) -> Path:
    ensure_runtime_dirs(paths)
    config_path = paths.agent_config_path
    if not config_path.exists():
        config_path.write_text(json.dumps(_config_payload(AgentConfig()), indent=2), encoding="utf-8")
    return config_path


def load_agent_config(paths: RuntimePaths) -> AgentConfig:
    config_path = ensure_agent_config(paths)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return AgentConfig(
        provider=str(payload.get("provider") or "openai"),
        model=str(payload.get("model") or "gpt-5"),
        max_turns=max(1, int(payload.get("maxTurns", payload.get("max_turns", 8)))),
        retry_limit=max(0, int(payload.get("retryLimit", payload.get("retry_limit", 2)))),
        retry_backoff_seconds=float(payload.get("retryBackoffSeconds", payload.get("retry_backoff_seconds", 1.0))),
        request_timeout_seconds=max(5, int(payload.get("requestTimeoutSeconds", payload.get("request_timeout_seconds", 60)))),
        autonomy_mode=str(payload.get("autonomyMode", payload.get("autonomy_mode", "full_local_auto"))),
        default_client_id=str(payload.get("defaultClientId", payload.get("default_client_id", "default"))),
        provider_options=dict(payload.get("providerOptions", payload.get("provider_options", {})) or {}),
    )
