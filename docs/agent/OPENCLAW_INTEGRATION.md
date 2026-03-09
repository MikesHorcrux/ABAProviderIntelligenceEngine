# OpenClaw Integration Guide (Scaffold)

## What This Adds

This scaffold introduces OpenClaw-ready runtime contracts without turning on production agent execution.

Added components:

- `agent_runtime/` package (routing, types, QA evaluator, provider TODO stub)
- config template at `config/agent_runtime.example.json`
- `status --json` visibility block: `data.agent_runtime`
- focused tests for routing, QA gates, and status contract presence

## Enable Locally

1. Create runtime config from template:
   - `cp config/agent_runtime.example.json config/agent_runtime.json`
2. Edit values:
   - set `enabled` to `true`
   - mark provider availability under `provider_modes`
   - tune `model_role_slots`, `fallback_order`, and `qa_thresholds`
3. Check status:
   - `python3.11 cannaradar_cli.py status --json`
   - verify `data.agent_runtime` fields

## Current Operational Behavior

- Runtime provider calls are wired for:
  - `codex_auth` via local `codex exec`
  - `openai_api` via Responses API (when `OPENAI_API_KEY` is set)
  - `clawbot` remains a placeholder adapter
- Existing pipeline stages remain backward compatible.
- External-research execution is available via `agent:external-research` command.

## Integration Roadmap (Next Step)

1. Implement real provider clients in `agent_runtime/providers.py`.
2. Add guarded invocation points behind feature flag checks.
3. Capture invocation telemetry + errors in run state.
4. Add end-to-end tests with mock provider responses.

## Rollback

To disable quickly:

- set `enabled` to `false` in `config/agent_runtime.json`
- or remove `config/agent_runtime.json` (status will show disabled defaults)
