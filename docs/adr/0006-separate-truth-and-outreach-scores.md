# ADR 0006: Separate Truth And Outreach Scores

Last verified against commit `0c5e92b`.

## Status

Accepted

## Context

Sales value and factual certainty are different concerns. A provider might be commercially attractive but weakly evidenced, or vice versa.

## Decision

Maintain separate scores:

- `record_confidence` for factual trust
- `outreach_fit_score` for sales ranking

Only approved records may become outreach-ready.

## Consequences

- Sales ranking cannot bypass evidence requirements.
- Operators can distinguish “true but not a strong target” from “strong target idea but not yet trusted enough.”

## Evidence In Code

- `pipeline/stages/score.py`
- `pipeline/stages/qa.py`
- `pipeline/stages/export.py`
