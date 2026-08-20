# Developing Ratatoskr Knowledge

> Status: Proposed  
> Last reviewed: 2026-08-20

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

## What a clone needs before you plan a change

A change is planned with OpenSpec, which is a CLI a clone installs for itself. Use the version
`.github/workflows/openspec.yml` pins, so your terminal and the gate answer the same:

```bash
npm install --global @fission-ai/openspec@1.10.0
```

Cross-repository behaviour lives in a store, and registering one is per-machine state that no
repository can turn on for you — the same kind of step as `git config core.hooksPath .githooks`:

```bash
git clone git@github.com:po4yka/ratatoskr-workspace.git <path>
openspec store register <path> --id ratatoskr-workspace
```

`openspec doctor` reports whether both are in place.

## The Rust skills in this repository

`.agents/skills/` holds eighteen Rust skills vendored from `po4yka/rust-skills`, and
`.claude/skills/` symlinks to them. Unlike the steps above this needs nothing from your machine: the
files are in the tree, so a fresh clone already has them.

Update them with the catalogue and never by hand:

```bash
npx skills update
```

That rewrites `.agents/skills/` and `skills-lock.json` from the catalogue. Run it in one repository,
read the diff, then apply the same change to every Ratatoskr repository whose stack is Rust.
`ratatoskr-workspace/.github/workflows/drift.yml` fails when one copy differs from the others.
