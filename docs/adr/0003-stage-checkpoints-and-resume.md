# ADR 0003: Stage Checkpoints And Resume

Last verified against commit `0c5e92b`.

## Status

Accepted

## Context

Live crawling is failure-prone. Operators need a bounded way to recover from interruptions without rerunning every stage.

## Decision

Persist run checkpoints as JSON state files, one per run, and resume from the next incomplete stage rather than from raw crawl state reconstruction.

## Consequences

- Runs are resumable with `sync --resume`.
- Stage details are inspectable without opening the DB.
- Partial progress survives Python exceptions and process interruptions.

## Evidence In Code

- `cli/sync.py`
- `pipeline/run_state.py`
- `pipeline/run_control.py`
