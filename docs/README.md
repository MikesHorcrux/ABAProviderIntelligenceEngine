# Provider Intel Docs

This docs set explains how to install, operate, validate, and modify the provider-intelligence runtime.

## Start Here

1. [README.md](/Users/horcrux/Development/CannaRadar/README.md)
2. [RUNBOOK_V1.md](/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md)
3. [AGENT_OPS_PLAYBOOK.md](/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md)
4. [schemas/cli/v1/](/Users/horcrux/Development/CannaRadar/docs/schemas/cli/v1/)

## What Each Doc Is For

- [README.md](/Users/horcrux/Development/CannaRadar/README.md)
  Human-facing overview, install, quick start, outputs, and scope.
- [RUNBOOK_V1.md](/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md)
  Step-by-step operator instructions for init, doctor, sync, export, recovery, and validation.
- [AGENT_OPS_PLAYBOOK.md](/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md)
  Concise operating contract for agents making bounded runtime changes or running interventions.
- [README_AI_AGENTS.md](/Users/horcrux/Development/CannaRadar/README_AI_AGENTS.md)
  Short AI-agent entrypoint for the active runtime surface.
- [SKILL.md](/Users/horcrux/Development/CannaRadar/SKILL.md)
  Repo-local skill card with canonical commands and validation steps.

## Runtime Surface

- CLI: `provider_intel_cli.py`
- DB: `data/provider_intel_v1.db`
- Outputs: `out/provider_intel/`
- State: `data/state/agent_runs/`
- Stage order: `seed_ingest -> crawl -> extract -> resolve -> score -> qa -> export`
