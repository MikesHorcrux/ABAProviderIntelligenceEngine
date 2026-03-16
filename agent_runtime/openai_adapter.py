from __future__ import annotations

import json
import os
import time
from typing import Any
from urllib import error, request

from cli.errors import AuthError, NetworkError, RuntimeCommandError

from agent_runtime.models import ModelAdapter, ModelMessage, ModelResponse, ToolCall, ToolDefinition


class OpenAIResponsesAdapter(ModelAdapter):
    provider_name = "openai"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.openai.com/v1/responses",
        timeout_seconds: int = 60,
        retry_limit: int = 2,
        retry_backoff_seconds: float = 1.0,
    ):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.base_url = base_url
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.retry_limit = max(0, int(retry_limit))
        self.retry_backoff_seconds = max(0.1, float(retry_backoff_seconds))
        if not self.api_key:
            raise AuthError("OPENAI_API_KEY is required for the OpenAI Responses adapter.")

    def generate(
        self,
        *,
        agent_name: str,
        instructions: str,
        messages: list[ModelMessage],
        tools: list[ToolDefinition],
        model: str,
        previous_response_id: str | None = None,
    ) -> ModelResponse:
        payload = {
            "model": model,
            "instructions": instructions,
            "input": [self._serialize_message(item) for item in messages],
        }
        if previous_response_id:
            payload["previous_response_id"] = previous_response_id
        if tools:
            payload["tools"] = [self._serialize_tool(tool) for tool in tools]

        raw = self._post_json(payload)
        return ModelResponse(
            text=self._extract_text(raw),
            tool_calls=self._extract_tool_calls(raw),
            response_id=str(raw.get("id") or ""),
            raw={"agent_name": agent_name, **raw},
        )

    def _post_json(self, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            self.base_url,
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        attempt = 0
        while True:
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                body_text = exc.read().decode("utf-8", errors="ignore")
                if exc.code in {401, 403}:
                    raise AuthError(f"OpenAI authentication failed ({exc.code}).", details={"body": body_text[:400]}) from exc
                if exc.code in {408, 409, 429, 500, 502, 503, 504} and attempt < self.retry_limit:
                    attempt += 1
                    time.sleep(self.retry_backoff_seconds * attempt)
                    continue
                raise NetworkError(f"OpenAI Responses request failed ({exc.code}).", details={"body": body_text[:400]}) from exc
            except error.URLError as exc:
                if attempt < self.retry_limit:
                    attempt += 1
                    time.sleep(self.retry_backoff_seconds * attempt)
                    continue
                raise NetworkError(f"OpenAI Responses request failed: {exc.reason}") from exc
            except json.JSONDecodeError as exc:
                raise RuntimeCommandError("OpenAI Responses API returned invalid JSON.") from exc

    @staticmethod
    def _serialize_tool(tool: ToolDefinition) -> dict[str, Any]:
        return {
            "type": "function",
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
            "strict": True,
        }

    @staticmethod
    def _serialize_message(message: ModelMessage) -> dict[str, Any]:
        if message.type == "function_call_output":
            return {
                "type": "function_call_output",
                "call_id": message.call_id,
                "output": message.content,
            }
        return {
            "role": message.role,
            "content": message.content,
        }

    @staticmethod
    def _extract_tool_calls(payload: dict[str, Any]) -> list[ToolCall]:
        out: list[ToolCall] = []
        for item in payload.get("output", []) or []:
            if str(item.get("type") or "") != "function_call":
                continue
            arguments_raw = item.get("arguments") or "{}"
            try:
                arguments = json.loads(arguments_raw) if isinstance(arguments_raw, str) else dict(arguments_raw or {})
            except Exception:
                arguments = {}
            out.append(
                ToolCall(
                    call_id=str(item.get("call_id") or item.get("id") or ""),
                    name=str(item.get("name") or ""),
                    arguments=arguments,
                )
            )
        return out

    @staticmethod
    def _extract_text(payload: dict[str, Any]) -> str:
        output_text = payload.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()
        parts: list[str] = []
        for item in payload.get("output", []) or []:
            if str(item.get("type") or "") != "message":
                continue
            for content in item.get("content", []) or []:
                if not isinstance(content, dict):
                    continue
                if content.get("type") in {"output_text", "text"}:
                    text = content.get("text") or content.get("value") or ""
                    if isinstance(text, str) and text.strip():
                        parts.append(text.strip())
        return "\n".join(parts).strip()
