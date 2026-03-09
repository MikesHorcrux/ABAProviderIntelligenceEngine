from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict


ProviderMode = Literal["openai_api", "codex_auth", "clawbot"]
TaskRole = Literal["summarize", "research", "writer", "qa"]


class ProviderResult(TypedDict, total=False):
    provider_mode: ProviderMode
    model: str
    text: str
    raw: dict[str, Any]
    error: str


@dataclass(frozen=True)
class ProviderCall:
    role: TaskRole
    prompt: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SelectedProvider:
    role: TaskRole
    provider_mode: ProviderMode
    model: str
    attempted_order: tuple[ProviderMode, ...]


@dataclass(frozen=True)
class QAGateThresholds:
    min_sources: int
    min_signals: int
    min_contact_coverage_pct: float


@dataclass(frozen=True)
class QAGateMetrics:
    source_count: int
    signal_count: int
    contact_coverage_pct: float


@dataclass(frozen=True)
class QAGateResult:
    passed: bool
    failures: tuple[str, ...]
    thresholds: QAGateThresholds
    metrics: QAGateMetrics


@dataclass(frozen=True)
class AgentRuntimeConfig:
    enabled: bool
    provider_modes_available: tuple[ProviderMode, ...]
    model_role_slots: dict[TaskRole, dict[str, Any]]
    fallback_order: tuple[ProviderMode, ...]
    qa_thresholds: QAGateThresholds
    config_path: str
