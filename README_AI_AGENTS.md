# AI Agent Reference

Last verified against commit `0c5e92b`.

This repository is an evidence-first provider intelligence runtime for New Jersey ASD/ADHD provider discovery, verification, and export.

## Read In This Order

1. [`README.md`](README.md)
2. [`docs/architecture.md`](docs/architecture.md)
3. [`docs/runtime-and-pipeline.md`](docs/runtime-and-pipeline.md)
4. [`docs/cli-reference.md`](docs/cli-reference.md)
5. [`docs/operations.md`](docs/operations.md)
6. [`docs/security-and-safety.md`](docs/security-and-safety.md)

## Canonical Commands

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py control --json --run-id latest show
python3.11 provider_intel_cli.py export --json --limit 100
```

## Agent Rules

- Evidence beats inference.
- Unknown is acceptable; fabricated certainty is not.
- Approved exports and sales briefs are downstream of QA, not a substitute for QA.
- Use domain controls before changing extraction logic when the issue is crawl noise.
