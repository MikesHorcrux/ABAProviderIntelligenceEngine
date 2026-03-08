# AI Agent Reference: CannaRadar v1.5

Use this document when proposing code changes or operating live runs.

## Primary Contract

The canonical agent surface is the CLI plus run-state files:

- `cannaradar_cli.py init`
- `cannaradar_cli.py doctor`
- `cannaradar_cli.py sync`
- `cannaradar_cli.py tail`
- `cannaradar_cli.py status`
- `cannaradar_cli.py search`
- `cannaradar_cli.py sql`
- `cannaradar_cli.py export`
- `cannaradar_cli.py control`

Prefer `--json` for agent workflows.

Read these first:

- `/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md`
- `/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md`
- `/Users/horcrux/Development/CannaRadar/SKILL.md`

## Focus Areas

- CLI and checkpoint behavior: `cli/`, `pipeline/run_state.py`
- Fetch/runtime behavior: `pipeline/fetch_backends/`, `pipeline/config.py`
- Parse: `pipeline/stages/parse.py`
- Resolve: `pipeline/stages/resolve.py`
- Score: `pipeline/stages/score.py`
- Export contracts: `pipeline/stages/export.py`, `jobs/export_changes.py`
- Lead research/enhancement: `pipeline/stages/research.py`

## Safe Edit Principles

- One stage per patch when possible.
- Keep SQL migrations additive when possible.
- Preserve existing output column contracts unless a migration/version change is included.
- Add tests for changed behavior in `tests/`.
- Preserve `deleted_at=''` soft-delete semantics unless a schema-wide change is intentional.

## Live-Run Operating Rules

- Start uncertain environments with `init` and `doctor`.
- Prefer bounded live runs before full inventory runs.
- Use `status --json` and checkpoint files to diagnose progress instead of relying on terminal noise.
- Use `out/agent_research_queue.csv` and `search --preset research-needed` to decide which leads still need agent follow-up.
- Resume interrupted runs with `sync --resume latest` when the checkpoint is still valid.
- Treat repeated asset/static/blog churn in fetch as URL-filter debt, not as proof the pipeline is healthy.
- Treat malformed seed domains as seed-quality issues first.

## Fetch Notes

- Do not change robot or denylist behavior without documenting the operational/legal scope.
- Keep per-domain delay, backoff, and cache semantics explicit.
- Use `seed_telemetry` to reason about blocked, failed, and cooling-off domains.

## Testing Expectations

Baseline validation:

- `PYTHONPATH=$PWD python3.11 tests/test_agent_cli.py`
- `PYTHONPATH=$PWD python3.11 tests/test_run_state.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_dispatch.py`
- `PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py`
- `PYTHONPATH=$PWD python3.11 tests/test_resolve_stage.py`

When fetch/runtime behavior changes:

- `CANNARADAR_RUN_FETCH_INTEGRATION=1 PYTHONPATH=$PWD python3.11 tests/test_fetch_integration.py`

Run `./run_smoke_tests.sh` only when the workspace has the generated artifact set required by `tests/smoke_v1.py`.
