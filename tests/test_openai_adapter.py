#!/usr/bin/env python3.11
from __future__ import annotations

import io
import json
import os
import tempfile
from urllib import error

import agent_runtime.openai_adapter as openai_adapter_module
from agent_runtime.memory import MemoryStore, SessionStore
from agent_runtime.models import ModelMessage, ToolDefinition
from agent_runtime.openai_adapter import OpenAIResponsesAdapter
from agent_runtime.policy import PolicyEngine
from agent_runtime.tools import ToolRegistry
from runtime_context import build_tenant_context


class _FakeResponse:
    def __init__(self, payload: dict[str, object]):
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_openai_responses_adapter_parses_tool_calls() -> None:
    original_urlopen = openai_adapter_module.request.urlopen

    def fake_urlopen(req, timeout=0):  # noqa: ANN001
        assert timeout == 30
        body = json.loads(req.data.decode("utf-8"))
        assert body["model"] == "gpt-5"
        assert body["tools"][0]["name"] == "status"
        return _FakeResponse(
            {
                "id": "resp_123",
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "status",
                        "arguments": "{\"reason\":\"Inspect counts\"}",
                    },
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "Checking runtime state."}],
                    },
                ],
            }
        )

    openai_adapter_module.request.urlopen = fake_urlopen
    previous_key = os.environ.get("OPENAI_API_KEY")
    os.environ["OPENAI_API_KEY"] = "test-key"
    try:
        adapter = OpenAIResponsesAdapter(timeout_seconds=30, retry_limit=0)
        response = adapter.generate(
            agent_name="RunOpsAgent",
            instructions="Use tools.",
            messages=[ModelMessage(role="user", content="Check status")],
            tools=[ToolDefinition(name="status", description="Status", parameters={"type": "object", "properties": {}, "required": []})],
            model="gpt-5",
            previous_response_id=None,
        )
    finally:
        openai_adapter_module.request.urlopen = original_urlopen
        if previous_key is None:
            os.environ.pop("OPENAI_API_KEY", None)
        else:
            os.environ["OPENAI_API_KEY"] = previous_key

    assert response.text == "Checking runtime state."
    assert len(response.tool_calls) == 1
    assert response.tool_calls[0].name == "status"
    assert response.tool_calls[0].arguments["reason"] == "Inspect counts"


def test_openai_responses_adapter_retries_transient_http_errors() -> None:
    original_urlopen = openai_adapter_module.request.urlopen
    attempts = {"count": 0}

    def fake_urlopen(req, timeout=0):  # noqa: ANN001
        del req, timeout
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise error.HTTPError(
                url="https://api.openai.com/v1/responses",
                code=429,
                msg="rate limit",
                hdrs=None,
                fp=io.BytesIO(b'{"error":"rate limit"}'),
            )
        return _FakeResponse({"output": [{"type": "message", "content": [{"type": "output_text", "text": "ok"}]}]})

    openai_adapter_module.request.urlopen = fake_urlopen
    previous_key = os.environ.get("OPENAI_API_KEY")
    os.environ["OPENAI_API_KEY"] = "test-key"
    try:
        adapter = OpenAIResponsesAdapter(timeout_seconds=30, retry_limit=1, retry_backoff_seconds=0.01)
        response = adapter.generate(
            agent_name="SupervisorAgent",
            instructions="Summarize.",
            messages=[ModelMessage(role="user", content="hello")],
            tools=[],
            model="gpt-5",
            previous_response_id=None,
        )
    finally:
        openai_adapter_module.request.urlopen = original_urlopen
        if previous_key is None:
            os.environ.pop("OPENAI_API_KEY", None)
        else:
            os.environ["OPENAI_API_KEY"] = previous_key

    assert attempts["count"] == 2
    assert response.text == "ok"


def test_openai_responses_adapter_sends_previous_response_id() -> None:
    original_urlopen = openai_adapter_module.request.urlopen

    def fake_urlopen(req, timeout=0):  # noqa: ANN001
        assert timeout == 30
        body = json.loads(req.data.decode("utf-8"))
        assert body["previous_response_id"] == "resp_prev_123"
        return _FakeResponse({"id": "resp_next_456", "output": [{"type": "message", "content": [{"type": "output_text", "text": "ok"}]}]})

    openai_adapter_module.request.urlopen = fake_urlopen
    previous_key = os.environ.get("OPENAI_API_KEY")
    os.environ["OPENAI_API_KEY"] = "test-key"
    try:
        adapter = OpenAIResponsesAdapter(timeout_seconds=30, retry_limit=0)
        response = adapter.generate(
            agent_name="RunOpsAgent",
            instructions="Continue.",
            messages=[ModelMessage(role="tool", type="function_call_output", call_id="call_1", content="{}")],
            tools=[],
            model="gpt-5",
            previous_response_id="resp_prev_123",
        )
    finally:
        openai_adapter_module.request.urlopen = original_urlopen
        if previous_key is None:
            os.environ.pop("OPENAI_API_KEY", None)
        else:
            os.environ["OPENAI_API_KEY"] = previous_key

    assert response.response_id == "resp_next_456"


def test_tool_registry_definitions_are_strict_mode_compatible() -> None:
    with tempfile.TemporaryDirectory() as td:
        context = build_tenant_context(tenant_id="tenant-a", tenant_root_base=td)
        registry = ToolRegistry(
            tenant_context=context,
            session_store=SessionStore(context.runtime_paths.agent_memory_db_path),
            memory_store=MemoryStore(context.runtime_paths.agent_memory_db_path),
            policy_engine=PolicyEngine(),
        )
        definitions = {tool.name: tool for tool in registry.definitions()}

    sync_parameters = definitions["sync"].parameters
    assert sync_parameters["additionalProperties"] is False
    assert set(sync_parameters["required"]) == set(sync_parameters["properties"].keys())
    assert sync_parameters["properties"]["reason"]["type"] == "string"
    assert sync_parameters["properties"]["seeds"]["type"] == ["string", "null"]
    assert sync_parameters["properties"]["crawl_mode"]["type"] == ["string", "null"]
    assert sync_parameters["properties"]["crawl_mode"]["enum"] == ["full", "refresh", None]


def main() -> None:
    test_openai_responses_adapter_parses_tool_calls()
    test_openai_responses_adapter_retries_transient_http_errors()
    test_openai_responses_adapter_sends_previous_response_id()
    test_tool_registry_definitions_are_strict_mode_compatible()
    print("test_openai_adapter: ok")


if __name__ == "__main__":
    main()
