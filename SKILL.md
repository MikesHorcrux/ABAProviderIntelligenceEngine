---
name: canna-radar-ai-agents
description: Use for CannaRadar repository operations involving the agent-operable crawler, canonical CLI workflows, resumable runs, live-run diagnostics, and stage-safe pipeline edits.
---

# CannaRadar Repo Skill

Use this when modifying or operating CannaRadar in this repository.

## Read First

1. `/Users/horcrux/Development/CannaRadar/README.md`
2. `/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md`
3. `/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md`

If the task is code-specific, then read only the files for the stage you are changing:

- CLI or agent ops: `cli/app.py`, `cli/doctor.py`, `cli/query.py`, `cli/sync.py`, `pipeline/run_state.py`
- Fetch/runtime behavior: `pipeline/fetch_backends/crawlee_backend.py`, `pipeline/fetch_backends/common.py`, `pipeline/config.py`
- Parse: `pipeline/stages/parse.py`
- Resolve: `pipeline/stages/resolve.py`
- Score: `pipeline/stages/score.py`
- Lead research/enhancement: `pipeline/stages/research.py`
- Exports/contracts: `pipeline/stages/export.py`, `jobs/export_changes.py`
- Schema/migrations: `db/schema.sql`, `jobs/ingest_sources.py`

## Canonical Operator Contract

Prefer the canonical CLI, not legacy wrapper scripts:

```bash
python3.11 cannaradar_cli.py init --json
python3.11 cannaradar_cli.py doctor --json
python3.11 cannaradar_cli.py sync --json --crawl-mode growth --max 50
python3.11 cannaradar_cli.py status --json
python3.11 cannaradar_cli.py export --json --kind all
```

For interruption recovery:

```bash
python3.11 cannaradar_cli.py status --json
python3.11 cannaradar_cli.py sync --json --resume latest
```

For local diagnostics during or after a run:

```bash
python3.11 cannaradar_cli.py search --json --preset failed-domains
python3.11 cannaradar_cli.py search --json --preset blocked-domains
python3.11 cannaradar_cli.py search --json --preset research-needed
python3.11 cannaradar_cli.py sql --json --query "SELECT seed_domain, last_status_code FROM seed_telemetry ORDER BY updated_at DESC LIMIT 20"
```

## Live-Run Workflow

- Start with `init` and `doctor` if DB/runtime state is uncertain.
- Prefer bounded live runs first: `25`, then `50`, then `100`, then full seed inventory.
- Treat `data/state/agent_runs/run_<run_id>.json` as the primary checkpoint for resumability and runtime diagnostics.
- Use `status --json` instead of guessing whether a run is progressing.
- On macOS, leave browser escalation in isolated subprocess mode unless the task is explicitly debugging inline Playwright behavior.
- Use `out/agent_research_queue.csv` as the first artifact for agent follow-up once a run scores leads.
- If a live run is interrupted, resume from the checkpoint before starting a new run unless the checkpoint is clearly corrupt.

## Live-Run Failure Heuristics

When a run stalls in fetch:

- If logs show `_next/static`, `wp-content`, fonts, images, `xmlrpc.php`, terms/privacy, or blog/category pages, treat it as URL-filter debt in `pipeline/fetch_backends/crawlee_backend.py`, not as a parsing failure.
- If seed domains are malformed or obviously broken, quarantine/fix the seed rows in `seeds.csv` before retrying.
- If the DB is zero-byte or schema checks fail, run `init` or `PYTHONPATH=$PWD python3.11 jobs/ingest_sources.py` before any crawl retry.
- Use `seed_telemetry` as the source of truth for repeated `403/429`, DNS failures, and backoff behavior.

Do not declare a live run healthy unless it reaches `enrich`/`score`/`export` and produces output artifacts.

## Stage-Safe Guardrails

- Keep edits stage-local when possible.
- Preserve output contracts unless an explicit migration or versioned change is included.
- Keep segment purity for `outreach_dispensary_100.csv`.
- Keep score range in `[0,100]`.
- Do not auto-merge entities; suggestions only.
- Preserve evidence flow for every new extraction or scoring path.
- Keep `run_id` and `stage` in logs, checkpoints, and operator-visible outputs.

## Validation Matrix

Run from repo root with Python 3.11:

- `PYTHONPATH=$PWD python3.11 tests/test_agent_cli.py`
- `PYTHONPATH=$PWD python3.11 tests/test_run_state.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py`
- `PYTHONPATH=$PWD python3.11 tests/test_lead_research.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_dispatch.py`
- `PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py`
- `PYTHONPATH=$PWD python3.11 tests/test_resolve_stage.py`

When changing live fetch/runtime behavior, also run:

- `CANNARADAR_RUN_FETCH_INTEGRATION=1 PYTHONPATH=$PWD python3.11 tests/test_fetch_integration.py`

Run `tests/smoke_v1.py` only when the expected generated artifact set exists in the workspace.

## Commit Messages

Include:

- what behavior changed
- why it changed
- rollback hint if external runtime behavior or output contracts changed

## References

- Repo AI guide: `/Users/horcrux/Development/CannaRadar/README_AI_AGENTS.md`
- Installed skill: `/Users/horcrux/.codex/skills/cannaradar-ai-agents/SKILL.md`
