# ADR 0002: Evidence-First Export Gate

Last verified against commit `0c5e92b`.

## Status

Accepted

## Context

The product goal is decision-grade provider intelligence, not volume. Crawled pages can contain noisy or ambiguous service language.

## Decision

Only export provider records after QA confirms evidence for every critical field. Records missing evidence or carrying unresolved ambiguity are routed to `review_queue`.

## Consequences

- High recall is deliberately traded for higher precision.
- Review queue is a normal operational output.
- Exports and sales briefs depend on `field_evidence`, not just extracted field values.

## Evidence In Code

- `pipeline/stages/qa.py`
- `pipeline/stages/resolve.py`
- `pipeline/stages/export.py`
