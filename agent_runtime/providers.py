from __future__ import annotations

from agent_runtime.contracts import ProviderCall, ProviderResult, SelectedProvider


def invoke_provider_stub(selection: SelectedProvider, call: ProviderCall) -> ProviderResult:
    """Temporary provider stub for scaffold wiring and tests.

    TODO: Replace this with real provider adapters:
    - openai_api: direct OpenAI API key flow
    - codex_auth: OpenClaw/Codex authenticated provider flow
    - clawbot: internal clawbot provider endpoint
    """
    return {
        "provider_mode": selection.provider_mode,
        "model": selection.model,
        "text": "",
        "raw": {
            "todo": "real provider call not implemented",
            "role": call.role,
        },
    }
