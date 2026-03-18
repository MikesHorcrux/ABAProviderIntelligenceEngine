# AI Agent Reference

Last verified against commit `0c5e92b`.

This repository is an evidence-first provider intelligence runtime for New Jersey ASD/ADHD provider discovery, verification, and export, with an optional tenant-scoped local agent control plane layered on top.

## Release Status

- This repository is source-available public code, not OSI-open-source.
- Read `LICENSE` and `NOTICE.md` before recommending reuse or redistribution.
- Describe dependency licensing accurately: project code uses the custom repository license, dependencies keep their own licenses.
- `Rethink Autism, Inc.` and `RethinkFirst` are excluded from the project license.

## Read In This Order

1. [`README.md`](README.md)
2. [`AGENTS.md`](AGENTS.md)
3. [`LICENSE`](LICENSE)
4. [`NOTICE.md`](NOTICE.md)
5. [`docs/architecture.md`](docs/architecture.md)
6. [`docs/runtime-and-pipeline.md`](docs/runtime-and-pipeline.md)
7. [`docs/cli-reference.md`](docs/cli-reference.md)
8. [`docs/operations.md`](docs/operations.md)
9. [`docs/security-and-safety.md`](docs/security-and-safety.md)

## Canonical Commands

```bash
python provider_intel_cli.py init --json
python provider_intel_cli.py doctor --json
python provider_intel_cli.py sync --json --max 10 --limit 25
python provider_intel_cli.py status --json
python provider_intel_cli.py search --json --preset outreach-ready
python provider_intel_cli.py control --json --run-id latest show
python provider_intel_cli.py export --json --limit 100
python provider_intel_cli.py --json --tenant acme agent run --goal "Run a bounded review and export loop"
python provider_intel_cli.py --json --tenant acme agent status
```

For human operators, the repo-local `./ae` wrapper can shorten the same
workflows. Agents should still prefer the canonical `provider_intel_cli.py`
surface because it is the stable tool contract.

Tenant runtime note:

- No `--tenant`: the CLI uses the legacy default runtime under `data/`, `out/`, and `data/state/`.
- With `--tenant <id>`: DB, config, checkpoints, outputs, and agent memory move under `storage/tenants/<id>/`.

## Agent Rules

- Evidence beats inference.
- Unknown is acceptable; fabricated certainty is not.
- Approved exports and sales briefs are downstream of QA, not a substitute for QA.
- The agent layer may orchestrate tools and bounded controls, but it must not write provider truth directly.
- Use domain controls before changing extraction logic when the issue is crawl noise.
- Keep test fixtures synthetic; do not add copied third-party HTML to the repo.
