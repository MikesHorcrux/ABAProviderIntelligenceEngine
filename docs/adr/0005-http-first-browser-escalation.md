# ADR 0005: HTTP-First Browser Escalation

Last verified against commit `0c5e92b`.

## Status

Accepted

## Context

Many provider pages are static enough for HTTP fetches, but some are JS-heavy or actively block basic crawling. Browser crawling is more expensive and should be bounded.

## Decision

Default to HTTP crawling, then escalate to Playwright/Crawlee browser crawling only when domain policy and block handling permit it.

## Consequences

- Simpler domains stay cheap.
- JS-heavy or blocked domains can still be attempted.
- Operators need fetch policies and controls for noisy sites.

## Evidence In Code

- `pipeline/fetch_backends/crawlee_backend.py`
- `pipeline/fetch_backends/browser_worker.py`
- `pipeline/fetch_backends/domain_policy.py`
