# Developing Ratatoskr Knowledge

> Status: Proposed  
> Last reviewed: 2026-08-17

Architecture bootstrap: analysis workers, prompts, providers, schemas, search indices, and evaluations are not implemented.

## Intended toolchain

Rust/Tokio, SQLx/PostgreSQL, PostgreSQL FTS and pgvector, JSON Schema validation, versioned prompt resources, provider adapters, BlobStore, tracing/OpenTelemetry, deterministic fixtures, and evaluation tooling.

## Code size limits

There is no code here yet, so no limit is enforced yet. The commit that brings the first manifest brings the configuration that carries the limits with it: `clippy.toml` beside a `Cargo.toml`, `eslint.config.js` beside a `package.json`. `fleet.yml` fails the gate when a manifest arrives without one, so the rule has a check behind it and not only this paragraph.

`ratatoskr-workspace/docs/QUALITY_GATES.md` holds the numbers the repositories with code use today, the command that measured each one, and the limits that were rejected with the reason. Read it before you choose numbers, then measure this tree. Each limit is set at the worst case the tree already has, so that the check fails on a regression and not on work that has not been done yet.

## Workflow

1. Identify the analysis family, contract, source authority, privacy policy, and evaluation set.
2. Version prompt, context builder, contract, model policy, and source hash independently.
3. Keep provider adapters thin and validate structured output before persistence.
4. Add grounding/citation, injection, cost, latency, and authorization tests.
5. Reindex or backfill through explicit versioned jobs, never hidden request-time mutation.

The first scaffold PR must define exact format/lint/test/eval/migration/local-provider commands. Tests use fakes or explicitly enabled test providers; production API keys are never required for default CI.
