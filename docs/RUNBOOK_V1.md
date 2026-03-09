# Runbook V1

Last verified against commit `0c5e92b`.

This compatibility runbook now points to the authoritative docs:

- [`operations.md`](operations.md)
- [`cli-reference.md`](cli-reference.md)
- [`runtime-and-pipeline.md`](runtime-and-pipeline.md)

Fast path:

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
```

Recovery path:

```bash
python3.11 provider_intel_cli.py sync --json --resume latest
```
