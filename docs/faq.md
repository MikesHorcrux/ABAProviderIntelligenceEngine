# FAQ

Last verified against commit `0c5e92b`.

## What does this project actually do?

It crawls provider sources, extracts evidence of ASD/ADHD diagnostic capability plus license and prescribing signals, scores confidence, queues uncertain records for review, and exports approved profiles and outreach briefs. It also has an optional tenant-scoped local agent control plane that can orchestrate those same deterministic workflows.

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

Yes. It narrows the crawl stage by using `monitorMaxPagesPerDomain`,
`monitorMaxTotalPages`, and `monitorMaxDepth` from `crawler_config.json`.
It still runs the same stage order and still rebuilds the active provider-intel
tables on a fresh run.

## Does `--crawlee-headless off` force visible browser crawling today?

Yes for the current sync run. It overrides the effective browser headless mode
without requiring you to rewrite `crawler_config.json`.

## What is a tenant?

A tenant is an isolated local workspace for one client, account, or operator context. When you run with `--tenant acme`, the runtime DB, config, checkpoints, outputs, and agent memory move under `storage/tenants/acme/` instead of using the shared default paths.

## Is a tenant the same thing as a seed pack?

No. A tenant answers “whose workspace is this?” A seed pack answers “what sources should this run crawl?”

## Does the project now have an agent command?

Yes. `provider_intel_cli.py agent run|status|resume` adds a tenant-scoped local agent control plane. It can orchestrate `doctor`, `sync`, `status`, `search`, `control`, `export`, and read-only `sql`, but it does not write provider truth directly.

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

There is no hosted service built into the repo. This is still a local CLI and file-backed runtime. The `agent run` and `agent resume` commands can make outbound calls to the OpenAI Responses API when `OPENAI_API_KEY` is set, but the runtime itself remains local-first.

## What should a new engineer read first?

Start with:

1. `README.md`
2. `docs/architecture.md`
3. `docs/runtime-and-pipeline.md`
4. `docs/cli-reference.md`
