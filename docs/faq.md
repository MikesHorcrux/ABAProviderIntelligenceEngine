# FAQ

Last verified against commit `0c5e92b`.

## What does this project actually do?

It crawls provider sources, extracts evidence of ASD/ADHD diagnostic capability plus license and prescribing signals, scores confidence, queues uncertain records for review, and exports approved profiles and outreach briefs.

## Is this a lead scraper?

Not by design. The code is built around evidence gating in `pipeline/stages/qa.py`, so raw discovery is not enough to become an approved export.

## What geography is supported today?

New Jersey only. The seed pack, metro lookup, and prescriber rules are all NJ-specific.

## What is the canonical record?

One provider-practice-location-state affiliation row in `provider_practice_records`.

## What makes a record exportable?

All critical fields need evidence, confidence cannot be too low, contradictions cannot remain unresolved enough to block, and QA must mark `export_status='approved'`.

## What makes a record outreach-ready?

It must already be approved, then also meet the outreach thresholds in `pipeline/stages/qa.py`: enough record confidence and outreach fit, active license, explicit ASD or ADHD diagnostic signal, and a public contact channel.

## Why do I sometimes get `0` approved records even when pages were crawled?

Because crawl success is not the same as export success. Common reasons are missing official license evidence, ambiguous diagnostic language, or noisy provider extraction.

## Can I use `search --preset outreach-ready` as my sales list?

Yes, that is the closest thing to a sales list in the current runtime. It still assumes you are comfortable with the current extraction quality on the domains that produced those rows.

## Does `--crawl-mode refresh` change behavior today?

Not yet. It is stored in run metadata, but the current sync path does not branch on it.

## Does `--crawlee-headless off` force visible browser crawling today?

Not directly. The flag is parsed and stored, but current effective headless behavior still comes from config or environment.

## How do I stop a noisy domain without editing code?

Use `control` commands such as `suppress-prefix`, `cap-domain`, `stop-domain`, or `quarantine-seed`.

## Where do I see why a record was blocked?

Check:

- `review_queue`
- `provider_practice_records.blocked_reason`
- `provider_practice_records.conflict_note`
- `contradictions`

## Does the project store raw HTML?

Yes. `source_documents.content` stores raw fetched HTML for auditability.

## Are the PDFs fully rendered with Playwright?

No. Current PDF output is a minimal fallback writer in `pipeline/stages/export.py`.

## Is there a remote service or API?

No. This is a local CLI and file-backed runtime.

## What should a new engineer read first?

Start with:

1. `README.md`
2. `docs/architecture.md`
3. `docs/runtime-and-pipeline.md`
4. `docs/cli-reference.md`
