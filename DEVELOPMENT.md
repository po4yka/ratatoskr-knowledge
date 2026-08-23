# Developing Ratatoskr Knowledge

> Status: Active
> Last reviewed: 2026-08-21

The first article-analysis slice uses a scripted provider for offline gates, an `OpenRouter` adapter
for real inference behind timeout, size-cap, rate, retry, cancellation, and budget controls, and a
disposable `PostgreSQL` 17 database. Search indices and external message handling are not
implemented.

## Toolchain and gate

`rust-toolchain.toml` pins Rust 1.97. Every command uses the committed lock file.

### Rust - the CI gate

```bash
cargo fetch --locked
cargo deny --locked check
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked
cargo test --workspace --locked
cargo test --workspace --locked --doc
cargo build --workspace --locked --release
```

The file-size ratchet is the one check that Cargo cannot express:

```bash
git ls-files -z "*.rs" | xargs -0 -r wc -l | awk '$2 != "total" && $1 > 850 { print; bad = 1 } END { exit bad }'
```

## Code size limits

`clippy.toml` carries the function, nesting, and signature limits. CI also rejects a tracked Rust
source file above 850 lines.

`ratatoskr-workspace/docs/QUALITY_GATES.md` holds the numbers the repositories with code use today, the command that measured each one, and the limits that were rejected with the reason. Read it before you choose numbers, then measure this tree. Each limit is set at the worst case the tree already has, so that the check fails on a regression and not on work that has not been done yet.

## Workflow

1. Identify the analysis family, contract, source authority, privacy policy, and evaluation set.
2. Version prompt, context builder, contract, model policy, and source hash independently.
3. Keep provider adapters thin and validate structured output before persistence.
4. Add grounding/citation, injection, cost, latency, and authorization tests.
5. Reindex or backfill through explicit versioned jobs, never hidden request-time mutation.

Default tests use the scripted provider, recorded fixtures, and a loopback fake transport; they need
no inference credentials and never reach the live API. Database tests create disposable databases
from the current `schema.sql`; this development repository has no migrations. The live `OpenRouter`
smoke check (`cargo run --locked -p ratatoskr-knowledge --example live_openrouter_smoke`) spends
real credit and is never part of the gate.

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
