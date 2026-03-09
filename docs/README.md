# Provider Intel Docs

This docs set now tracks the provider-intelligence runtime only.

## Start Here

1. [AGENT_OPS_PLAYBOOK.md](./AGENT_OPS_PLAYBOOK.md)
2. [RUNBOOK_V1.md](./RUNBOOK_V1.md)
3. [`docs/schemas/cli/v1/`](./schemas/cli/v1/)

## Current System Shape

- CLI entrypoint: `provider_intel_cli.py`
- DB: `data/provider_intel_v1.db`
- Output root: `out/provider_intel/`
- Stage order: `seed_ingest -> crawl -> extract -> resolve -> score -> qa -> export`

The old numbered docs were removed because they no longer describe the runtime that ships in this repository.
