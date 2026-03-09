# ADR 0001: Hard Pivot To Provider Intel

Last verified against commit `0c5e92b`.

## Status

Accepted

## Context

The repository previously served a different product surface. The active code now uses `provider_intel_cli.py`, a fresh SQLite schema in `db/schema.sql`, and provider-intelligence-oriented pipeline stages.

## Decision

Make a hard break to a provider-intel product surface instead of preserving compatibility with the prior business schema or CLI.

## Consequences

- The canonical operator entrypoint is `provider_intel_cli.py`.
- The active DB is `data/provider_intel_v1.db`.
- Business tables are provider/practice/evidence oriented, not legacy outreach entities.
- Historical data migration is intentionally out of scope.

## Evidence In Code

- `provider_intel_cli.py`
- `db/schema.sql`
- `jobs/ingest_sources.py`
- `cannaradar_cli.py`
