from __future__ import annotations

import os
import subprocess
from typing import Any

from agent_runtime.contracts import ProviderCall, ProviderMode, ProviderResult, SelectedProvider


def _invoke_codex_auth(*, model: str, call: ProviderCall, timeout_seconds: int = 240) -> ProviderResult:
    cmd = ["codex", "exec", call.prompt]
    env = os.environ.copy()
    if model:
        env.setdefault("CODEX_MODEL", model)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds, env=env)
    if proc.returncode != 0:
        return {
            "provider_mode": "codex_auth",
            "model": model,
            "text": "",
            "error": (proc.stderr or proc.stdout or "codex command failed").strip(),
            "raw": {"returncode": proc.returncode},
        }
    text = (proc.stdout or "").strip()
    return {
        "provider_mode": "codex_auth",
        "model": model,
        "text": text,
        "raw": {"returncode": proc.returncode},
    }


def _invoke_openai_api(*, model: str, call: ProviderCall) -> ProviderResult:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {
            "provider_mode": "openai_api",
            "model": model,
            "text": "",
            "error": "OPENAI_API_KEY is not set",
        }

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        rsp = client.responses.create(model=model or "gpt-4.1-mini", input=call.prompt)
        text = getattr(rsp, "output_text", "") or ""
        return {
            "provider_mode": "openai_api",
            "model": model,
            "text": text.strip(),
            "raw": {"id": getattr(rsp, "id", "")},
        }
    except Exception as exc:  # pragma: no cover - depends on env/api
        return {
            "provider_mode": "openai_api",
            "model": model,
            "text": "",
            "error": str(exc),
        }


def _invoke_clawbot(*, model: str, call: ProviderCall) -> ProviderResult:
    # Placeholder until dedicated clawbot endpoint/SDK is wired.
    return {
        "provider_mode": "clawbot",
        "model": model,
        "text": "",
        "error": "clawbot provider adapter not wired yet",
    }


def invoke_provider_mode(*, mode: ProviderMode, model: str, call: ProviderCall) -> ProviderResult:
    if mode == "codex_auth":
        return _invoke_codex_auth(model=model, call=call)
    if mode == "openai_api":
        return _invoke_openai_api(model=model, call=call)
    if mode == "clawbot":
        return _invoke_clawbot(model=model, call=call)
    return {
        "provider_mode": mode,
        "model": model,
        "text": "",
        "error": f"unsupported provider mode: {mode}",
    }


def invoke_provider_stub(selection: SelectedProvider, call: ProviderCall) -> ProviderResult:
    """Backward-compatible entrypoint used by scaffold docs/tests."""
    return invoke_provider_mode(mode=selection.provider_mode, model=selection.model, call=call)
