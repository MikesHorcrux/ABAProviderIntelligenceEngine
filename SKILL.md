---
name: provider-intel-repo
description: Use for Provider Intelligence repository operations involving the agent-operable crawler, canonical CLI workflows, resumable runs, and stage-safe pipeline edits.
---

# Provider Intel Repo Skill

Use this when modifying or operating the provider-intelligence runtime in this repository.

## Read First

1. `/Users/horcrux/Development/CannaRadar/README.md`
2. `/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md`
3. `/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md`

## Canonical Flow

```bash
cd /Users/horcrux/Development/CannaRadar
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

## Validation

- `PYTHONPATH=$PWD python3.11 tests/test_agent_cli.py`
- `PYTHONPATH=$PWD python3.11 tests/test_run_state.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py`
- `PYTHONPATH=$PWD python3.11 tests/test_lead_research.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_dispatch.py`
- `PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py`
- `PYTHONPATH=$PWD python3.11 tests/test_resolve_stage.py`
