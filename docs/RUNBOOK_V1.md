# Runbook V1

Last verified against commit `0c5e92b`.

This compatibility runbook now points to the authoritative docs:

- [`operations.md`](operations.md)
- [`cli-reference.md`](cli-reference.md)
- [`runtime-and-pipeline.md`](runtime-and-pipeline.md)

Fast path:

```bash
python provider_intel_cli.py init --json
python provider_intel_cli.py doctor --json
python provider_intel_cli.py sync --json --max 10 --limit 25
python provider_intel_cli.py status --json
```

Tenant-scoped fast path:

```bash
python provider_intel_cli.py --json --tenant acme init
python provider_intel_cli.py --json --tenant acme doctor
python provider_intel_cli.py --json --tenant acme sync --max 10 --limit 25
python provider_intel_cli.py --json --tenant acme status
```

Recovery path:

```bash
python provider_intel_cli.py sync --json --resume latest
```
