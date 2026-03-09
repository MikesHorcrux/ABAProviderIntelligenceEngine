# ADR 0004: SQLite As Canonical Runtime Store

Last verified against commit `0c5e92b`.

## Status

Accepted

## Context

The runtime is local-first, file-oriented, and designed for a single operator or small team working from a shared repo.

## Decision

Use SQLite as the canonical store for crawl results, evidence, canonical records, and exportable state.

## Consequences

- Setup remains simple and local.
- Operators can inspect state with `sql`, `search`, and external SQLite tools.
- Concurrency and permission controls are intentionally limited compared with a server database.

## Evidence In Code

- `pipeline/db.py`
- `db/schema.sql`
- `cli/query.py`
