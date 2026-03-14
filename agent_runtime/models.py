from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    parameters: dict[str, Any]


@dataclass(frozen=True)
class ToolCall:
    call_id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ModelMessage:
    role: str
    content: str
    type: str = "message"
    call_id: str = ""


@dataclass(frozen=True)
class ModelResponse:
    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


class ModelAdapter(ABC):
    provider_name: str = "unknown"

    @abstractmethod
    def generate(
        self,
        *,
        agent_name: str,
        instructions: str,
        messages: list[ModelMessage],
        tools: list[ToolDefinition],
        model: str,
    ) -> ModelResponse:
        raise NotImplementedError
