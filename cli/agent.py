from __future__ import annotations

import json
import sys
from dataclasses import replace
from typing import Any

from agent_runtime.config import load_agent_config
from agent_runtime.memory import MemoryStore, SessionStore
from agent_runtime.models import ModelAdapter, ModelResponse
from agent_runtime.openai_adapter import OpenAIResponsesAdapter
from agent_runtime.orchestrator import AgentOrchestrator
from agent_runtime.policy import PolicyEngine
from agent_runtime.tools import ToolRegistry
from cli.errors import UsageError
from runtime_context import TenantContext


class _UnavailableModelAdapter(ModelAdapter):
    provider_name = "unavailable"

    def generate(self, *, agent_name: str, instructions: str, messages, tools, model: str, previous_response_id: str | None = None) -> ModelResponse:  # noqa: ANN001
        del agent_name, instructions, messages, tools, model, previous_response_id
        raise UsageError("This command does not support model generation.")


def _tenant_context_from_args(args) -> TenantContext:
    return TenantContext(
        tenant_id=getattr(args, "tenant", None),
        tenant_root_base=None,
        runtime_paths=getattr(args, "runtime_paths"),
    )


def _format_trace_event(event: dict[str, Any]) -> str:
    event_type = str(event.get("type") or "")
    if event_type == "session_started":
        return f"[agent] session started tenant={event.get('tenant_id')} session={event.get('session_id')}"
    if event_type == "session_completed":
        run_ids = ",".join(str(item) for item in event.get("run_ids") or [])
        return f"[agent] session completed session={event.get('session_id')} runs={run_ids or '-'}"
    if event_type == "session_failed":
        return f"[agent] session failed session={event.get('session_id')} error={event.get('error')}"
    if event_type == "agent_message":
        text = " ".join(str(event.get("text") or "").split())
        text = text[:240] + ("..." if len(text) > 240 else "")
        return f"[agent] {event.get('agent_name')}: {text}"
    if event_type == "tool_call_requested":
        arguments = dict(event.get("arguments") or {})
        reason = str(arguments.get("reason") or "").strip()
        return f"[agent] tool request name={event.get('tool_name')} reason={reason or '-'}"
    if event_type == "tool_call_completed":
        return f"[agent] tool result name={event.get('tool_name')} ok={event.get('ok')} summary={event.get('summary')}"
    return f"[agent] {json.dumps(event, default=str, sort_keys=True)}"


def _trace_hook_from_args(args):  # noqa: ANN001
    if not getattr(args, "trace", False):
        return None

    def _emit(event: dict[str, Any]) -> None:
        print(_format_trace_event(event), file=sys.stderr, flush=True)

    return _emit


def _build_orchestrator(args, *, model_adapter=None, require_model: bool = True) -> AgentOrchestrator:
    tenant_context = _tenant_context_from_args(args)
    config = load_agent_config(tenant_context.runtime_paths)
    if getattr(args, "model", None):
        config = replace(config, model=str(args.model))
    if require_model and model_adapter is None and config.provider != "openai":
        raise UsageError(f"Unsupported agent model provider in agent_config.json: {config.provider}")
    if model_adapter is not None:
        adapter = model_adapter
    elif require_model:
        adapter = OpenAIResponsesAdapter(
            base_url=str(config.provider_options.get("base_url") or "https://api.openai.com/v1/responses"),
            timeout_seconds=config.request_timeout_seconds,
            retry_limit=config.retry_limit,
            retry_backoff_seconds=config.retry_backoff_seconds,
        )
    else:
        adapter = _UnavailableModelAdapter()
    session_store = SessionStore(tenant_context.runtime_paths.agent_memory_db_path)
    memory_store = MemoryStore(tenant_context.runtime_paths.agent_memory_db_path)
    tool_registry = ToolRegistry(
        tenant_context=tenant_context,
        session_store=session_store,
        memory_store=memory_store,
        policy_engine=PolicyEngine(),
        db_timeout_ms=getattr(args, "db_timeout_ms", 30000),
    )
    return AgentOrchestrator(
        config=config,
        model_adapter=adapter,
        session_store=session_store,
        memory_store=memory_store,
        tool_registry=tool_registry,
        trace_hook=_trace_hook_from_args(args),
    )


def execute_agent_run(args, *, model_adapter=None) -> dict[str, object]:
    tenant_context = _tenant_context_from_args(args)
    orchestrator = _build_orchestrator(args, model_adapter=model_adapter)
    return orchestrator.run(
        goal=str(getattr(args, "goal")),
        tenant_context=tenant_context,
        session_id=getattr(args, "session_id", None),
    )


def execute_agent_status(args, *, model_adapter=None) -> dict[str, object]:
    orchestrator = _build_orchestrator(args, model_adapter=model_adapter, require_model=False)
    return orchestrator.status(
        getattr(args, "session_id", None),
        tenant_id=str(getattr(args, "tenant", "") or ""),
    )


def execute_agent_resume(args, *, model_adapter=None) -> dict[str, object]:
    tenant_context = _tenant_context_from_args(args)
    orchestrator = _build_orchestrator(args, model_adapter=model_adapter)
    session = orchestrator.session_store.get_session_for_tenant(
        str(getattr(args, "session_id")),
        str(getattr(args, "tenant", "") or ""),
    )
    return orchestrator.run(
        goal=str(session.get("goal") or ""),
        tenant_context=tenant_context,
        session_id=str(getattr(args, "session_id")),
    )
