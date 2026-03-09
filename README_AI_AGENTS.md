# AI Agent Reference: Provider Intelligence

The active runtime in this repository is the provider-intelligence engine.

## Canonical Commands

- `provider_intel_cli.py init`
- `provider_intel_cli.py doctor`
- `provider_intel_cli.py sync`
- `provider_intel_cli.py tail`
- `provider_intel_cli.py status`
- `provider_intel_cli.py search`
- `provider_intel_cli.py sql`
- `provider_intel_cli.py export`
- `provider_intel_cli.py control`

## Read First

1. `/Users/horcrux/Development/CannaRadar/README.md`
2. `/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md`
3. `/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md`
4. `/Users/horcrux/Development/CannaRadar/SKILL.md`

## Operating Notes

- The pipeline is checkpointed and resumable.
- The canonical scope is New Jersey provider intelligence.
- Critical fields must have source evidence before export.
- Review queue routing is part of the expected output, not a failure mode.
- Use bounded sync runs before broad crawl changes.
