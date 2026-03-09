# OpenClaw Agent Runtime Spec (Scaffold)

## Purpose

This document defines the scaffold contract for integrating OpenClaw-style agent execution into CannaRadar without changing existing pipeline behavior.

The scope is limited to:

- provider selection and routing contracts
- QA gate thresholds and pass/fail semantics
- status visibility in CLI output
- config-driven enablement

Not in scope for this scaffold:

- real provider API calls
- prompt engineering workflows
- orchestration of agent-generated artifacts

## Runtime Roles

The agent runtime exposes four task roles:

- `summarize`
- `research`
- `writer`
- `qa`

Each role resolves to:

- a selected provider mode
- a model identifier
- an attempted provider order (role preference + fallback)

## Provider Modes

Supported mode identifiers:

- `openai_api`
- `codex_auth`
- `clawbot`

Each mode is independently marked available/unavailable in config.

## Selection Rules

Provider selection uses:

1. role-specific `preferred_providers`
2. global `fallback_order`
3. availability filter from `provider_modes.*.available`

The first available mode in this merged order is selected.

If no mode is available, runtime raises an explicit error.

## Backward Compatibility

This scaffold is additive only.

- Existing `sync`, `status`, `search`, `sql`, `control`, and export behavior remains unchanged.
- Agent runtime does not execute provider calls in current state.
- `status` now includes an extra `agent_runtime` block for visibility.

## Config Inputs

Scaffold default read path:

- `config/agent_runtime.json` (optional at runtime)
- `config/agent_runtime.example.json` (reference template)

If runtime config does not exist, defaults are used and runtime is disabled.

## Future Integration Hooks

- Replace stub call surface in `agent_runtime/providers.py`.
- Attach role routing to research/export pathways when approved.
- Persist runtime invocation telemetry in DB or run-state snapshots.
