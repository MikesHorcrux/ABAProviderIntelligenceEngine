# Agent Ops Playbook

Last verified against commit `0c5e92b`.

This file is the short operating contract for agents. Use the main docs set for detail.

## Release Status

- This repository is source-available public code, not OSI-open-source.
- Read `../LICENSE` and `../NOTICE.md` before recommending reuse or redistribution.
- Describe dependency licensing accurately in operator- or agent-facing output.
- Treat `Rethink Autism, Inc.` and `RethinkFirst` as excluded from the project license.

## Read First

1. [`../README.md`](../README.md)
2. [`../AGENTS.md`](../AGENTS.md)
3. [`architecture.md`](architecture.md)
4. [`runtime-and-pipeline.md`](runtime-and-pipeline.md)
5. [`cli-reference.md`](cli-reference.md)
6. [`operations.md`](operations.md)

## Canonical Flow

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py export --json --limit 100
```

Tenant-scoped agent flow:

```bash
python3.11 provider_intel_cli.py --json --tenant acme init
python3.11 provider_intel_cli.py --json --tenant acme doctor
python3.11 provider_intel_cli.py --json --tenant acme agent run --goal "Run a bounded provider-intel loop"
python3.11 provider_intel_cli.py --json --tenant acme agent status
```

Runtime note:

- No `--tenant`: use the legacy shared local runtime.
- With `--tenant`: DB, config, checkpoints, outputs, and agent memory are isolated under `storage/tenants/<tenant_id>/`.

## Non-Negotiable Rules

- Never claim ASD/ADHD diagnostic capability, license status, or prescribing authority without evidence.
- Treat `review_queue` as a safety lane, not a failure.
- Do not weaken QA to increase export counts.
- Do not let the agent layer write provider truth directly; it must orchestrate the deterministic runtime instead.
- Keep `record_confidence` and `outreach_fit_score` separate.
- Use bounded runs before broadening seeds or page caps.
- Keep fixtures synthetic; do not add copied third-party site captures.

## First Diagnostic Commands

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py search --json --preset blocked-domains
python3.11 provider_intel_cli.py search --json --preset contradictions
python3.11 provider_intel_cli.py control --json --run-id latest show
```
