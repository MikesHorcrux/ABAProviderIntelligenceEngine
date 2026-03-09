# Documentation Index

Last verified against commit `0c5e92b`.

This repository now has one documentation surface for three audiences: developers, operators, and non-technical stakeholders.

## Start Here By Audience

- Developer: [`architecture.md`](architecture.md), [`data-model.md`](data-model.md), [`runtime-and-pipeline.md`](runtime-and-pipeline.md), [`testing-and-quality.md`](testing-and-quality.md)
- Operator: [`operations.md`](operations.md), [`cli-reference.md`](cli-reference.md), [`faq.md`](faq.md)
- Stakeholder: [`../README.md`](../README.md), [`architecture.md`](architecture.md), [`security-and-safety.md`](security-and-safety.md), [`faq.md`](faq.md)
- Agent/operator working inside the repo: [`AGENT_OPS_PLAYBOOK.md`](AGENT_OPS_PLAYBOOK.md), [`../README_AI_AGENTS.md`](../README_AI_AGENTS.md), [`../SKILL.md`](../SKILL.md)

## Core Docs

- [`architecture.md`](architecture.md)
  System overview, responsibilities, module boundaries, and architecture visuals.
- [`data-model.md`](data-model.md)
  Database tables, schemas, relationships, and versioning notes.
- [`runtime-and-pipeline.md`](runtime-and-pipeline.md)
  Stage execution, retries, browser escalation, checkpoints, and run-state lifecycle.
- [`cli-reference.md`](cli-reference.md)
  Full command reference with examples, presets, controls, and troubleshooting.
- [`operations.md`](operations.md)
  Day-1 setup, day-2 ops, monitoring, incident handling, and recovery steps.
- [`security-and-safety.md`](security-and-safety.md)
  Evidence gates, safe defaults, data persistence, and risk boundaries.
- [`testing-and-quality.md`](testing-and-quality.md)
  Test suite map, optional integration coverage, and release-readiness checklist.
- [`faq.md`](faq.md)
  Action-oriented answers for common runtime questions.

## Decision Records

- [`adr/0001-hard-pivot-to-provider-intel.md`](adr/0001-hard-pivot-to-provider-intel.md)
- [`adr/0002-evidence-first-export-gate.md`](adr/0002-evidence-first-export-gate.md)
- [`adr/0003-stage-checkpoints-and-resume.md`](adr/0003-stage-checkpoints-and-resume.md)
- [`adr/0004-sqlite-as-canonical-runtime-store.md`](adr/0004-sqlite-as-canonical-runtime-store.md)
- [`adr/0005-http-first-browser-escalation.md`](adr/0005-http-first-browser-escalation.md)
- [`adr/0006-separate-truth-and-outreach-scores.md`](adr/0006-separate-truth-and-outreach-scores.md)

## Compatibility Docs

- [`RUNBOOK_V1.md`](RUNBOOK_V1.md)
  Thin compatibility entrypoint for existing operator references.
- [`AGENT_OPS_PLAYBOOK.md`](AGENT_OPS_PLAYBOOK.md)
  Concise operating contract for repo agents.
