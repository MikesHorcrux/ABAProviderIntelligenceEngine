---
name: aba-provider-intelligence-engine-repo
description: Use for Provider Intelligence repository operations involving the agent-operable crawler, canonical CLI workflows, resumable runs, live-run diagnostics, and stage-safe pipeline edits.
---

# ABAProviderIntelligenceEngine Repo Skill

Last verified against commit `0c5e92b`.

## Read First

1. [`README.md`](README.md)
2. [`docs/architecture.md`](docs/architecture.md)
3. [`docs/runtime-and-pipeline.md`](docs/runtime-and-pipeline.md)
4. [`docs/cli-reference.md`](docs/cli-reference.md)
5. [`docs/operations.md`](docs/operations.md)

## Canonical Workflow

```bash
cd <repo-root>
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

## Useful Diagnostics

```bash
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py search --json --preset contradictions
python3.11 provider_intel_cli.py search --json --preset blocked-domains
python3.11 provider_intel_cli.py control --json --run-id latest show
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Validation

```bash
PYTHONPATH=$PWD python3.11 tests/test_agent_cli.py
PYTHONPATH=$PWD python3.11 tests/test_run_state.py
PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py
PYTHONPATH=$PWD python3.11 tests/test_fetch_dispatch.py
PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py
PYTHONPATH=$PWD python3.11 tests/test_resolve_stage.py
PYTHONPATH=$PWD python3.11 tests/test_lead_research.py
```

## Rules

- Do not document or implement behavior that the code does not have.
- Treat `--crawl-mode`, `--crawlee-headless`, and `--db-timeout-ms` carefully; parts of that surface are currently metadata-only.
- Keep evidence gating intact.
- Prefer bounded live validation over broad reruns.
