# Contributing

Thanks for contributing to ABAProviderIntelligenceEngine.

## Before You Open A PR

- Read `README.md`, `AGENTS.md`, `LICENSE`, and `NOTICE.md`.
- Confirm that you have the right to contribute the code, docs, fixtures, or
  other material you are submitting.
- By submitting a contribution, you agree that it will be licensed under the
  repository license in `LICENSE`.

## Repository Hygiene

- Keep test fixtures synthetic. Do not commit copied third-party HTML, PDFs, or
  other proprietary source captures.
- Do not commit generated crawl outputs, SQLite databases, or local run-state
  artifacts.
- Do not commit proxy credentials, secrets, or local environment files.

## Validation

From the repository root:

```bash
PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py
PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py
PYTHONPATH=$PWD python3.11 tests/test_run_state.py
```
