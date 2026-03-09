---
name: provider-intel-repo
description: Use for Provider Intelligence repository operations involving the agent-operable crawler, canonical CLI workflows, resumable runs, live-run diagnostics, and stage-safe pipeline edits.
---

# Provider Intel Repo Skill

Use this when modifying or operating the provider-intelligence runtime in this repository.

## What The Repo Is For

The runtime builds evidence-backed New Jersey provider intelligence for autism and ADHD diagnosis capability, prescribing classification, provider profiles, and outreach-ready exports.

## Read First

1. [README.md](/Users/horcrux/Development/CannaRadar/README.md)
2. [docs/RUNBOOK_V1.md](/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md)
3. [docs/AGENT_OPS_PLAYBOOK.md](/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md)

## Canonical Flow

```bash
cd /Users/horcrux/Development/CannaRadar
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

## Useful Commands

```bash
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py sql --json --query "SELECT provider_name_snapshot, record_confidence FROM provider_practice_records ORDER BY updated_at DESC LIMIT 20"
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Live Validation

```bash
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
```

## Validation

- `PYTHONPATH=$PWD python3.11 tests/test_agent_cli.py`
- `PYTHONPATH=$PWD python3.11 tests/test_run_state.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py`
- `PYTHONPATH=$PWD python3.11 tests/test_lead_research.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_dispatch.py`
- `PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py`
- `PYTHONPATH=$PWD python3.11 tests/test_resolve_stage.py`

## Operating Rules

- Preserve evidence flow for critical fields.
- Do not conflate factual confidence with sales priority.
- Use bounded runs before broadening seed inventory.
- Keep review queue routing intact when certainty is weak.
