# Agent Runtime Contract (Scaffold)

## Contract Surface

Package: `agent_runtime/`

### Core Types

- `ProviderMode`: `openai_api | codex_auth | clawbot`
- `TaskRole`: `summarize | research | writer | qa`
- `ProviderCall` dataclass
- `ProviderResult` typed dict
- `SelectedProvider` dataclass
- `QAGateThresholds` dataclass
- `QAGateMetrics` dataclass
- `QAGateResult` dataclass
- `AgentRuntimeConfig` dataclass

### Core Functions

- `load_agent_runtime_config(config_path)`
- `select_provider_for_role(role, config)`
- `evaluate_qa_gates(metrics, thresholds)`
- `status_snapshot(config_path)`
- `invoke_provider_stub(selection, call)` (placeholder only)

## Config Shape

```json
{
  "enabled": false,
  "provider_modes": {
    "openai_api": { "available": true },
    "codex_auth": { "available": true },
    "clawbot": { "available": false }
  },
  "model_role_slots": {
    "summarize": { "model": "gpt-4.1-mini", "preferred_providers": ["openai_api", "codex_auth"] },
    "research": { "model": "gpt-4.1", "preferred_providers": ["codex_auth", "openai_api"] },
    "writer": { "model": "gpt-4.1", "preferred_providers": ["openai_api", "codex_auth"] },
    "qa": { "model": "gpt-4.1-mini", "preferred_providers": ["codex_auth", "openai_api"] }
  },
  "fallback_order": ["codex_auth", "openai_api", "clawbot"],
  "qa_thresholds": {
    "min_sources": 2,
    "min_signals": 3,
    "min_contact_coverage_pct": 50.0
  }
}
```

## Status Contract Addition

`status --json` now includes:

```json
{
  "agent_runtime": {
    "enabled": false,
    "config_path": "/abs/path/to/config/agent_runtime.json",
    "provider_modes_available": ["openai_api", "codex_auth"],
    "qa_thresholds": {
      "min_sources": 2,
      "min_signals": 3,
      "min_contact_coverage_pct": 50.0
    },
    "last_error": ""
  }
}
```

## Error Handling

If config load/parsing fails:

- status block remains present
- `enabled` is `false`
- defaults are surfaced for thresholds
- `last_error` contains the parse/load failure string

## Stability Expectations

- Names in this scaffold are intended to be stable for the upcoming real integration step.
- Additional fields may be appended without removing existing ones.
