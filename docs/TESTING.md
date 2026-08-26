# Knowledge testing and evaluation

## Current coverage

The test suite covers strict configuration, admin lifecycle, an idempotent PostgreSQL 17 schema,
source and run identity, legal state transitions, two-attempt durability, article schema drift,
citation validation, complete-block context selection, prompt drift, scripted provider order, raw
blob preservation, timeout and transient retry, invalid-output repair, permanent failure, atomic
result persistence, and replay.

The real-provider slice adds recorded-fixture wire-contract tests for the `OpenRouter` request body,
success envelope, and error classification; a loopback fake HTTP transport proving the response byte
cap, per-try deadline, bounded jittered retry, credential header placement, and rate-limiter
spacing; durable budget-ledger window totals with pre-call daily, monthly, and cost enforcement;
attempts that carry adapter identity, model, latency, status, and failure class; cancellation
consistency with idempotent replay; and retry/repair bounds under a permanently or intermittently
failing transport. Ordinary logs are asserted to contain neither credentials nor source content.

The process test starts the real binary with a disposable database and blob root. It proves that
`check-config` does not bind, readiness arrives without inference credentials, `/analyze` is absent,
and `SIGTERM` finishes within the configured bound.

Default tests use the scripted provider, recorded fixtures, and the loopback fake transport; none of
them makes an external inference request. Database tests create disposable databases from the
current [`../schema.sql`](../schema.sql); this repository has no database migrations. The live
`OpenRouter` smoke check is a manually run example (`crates/knowledge/examples/
live_openrouter_smoke.rs`) that spends real credit and is excluded from every gate.

## Gate

Run the commands in [`../DEVELOPMENT.md`](../DEVELOPMENT.md) in their listed order. CI runs the same
Cargo command list and the Rust source-length ratchet.

## Test-first rule

Each behavior task first adds and runs a failing test. The next task adds the smallest implementation
that makes it pass, then runs formatting and Clippy. Configuration, documentation, and generated
artifacts state why they do not start with a behavior test.

The committed evaluation harness loads strict non-sensitive fixtures and recorded response sets,
then scores article structural validity, citation grounding, summary bound, key-point cardinality,
and required evidence coverage. It renders byte-deterministic, side-by-side label reports without
credentials, clocks, transports, or network access. Deletion tests enumerate source references,
runs, attempts, outputs, projection-input snapshots, lexical rows, chunks, failure rows, and owned
blob bytes; they also prove tenant isolation, reference-safe blob sweep, and one-transaction audit
visibility. Process tests cover the operator deletion/reindex receipts, deterministic progress, and
the missing-embeddings fail-fast path.

Future real-provider recordings are deliberate manual artifacts, not gate inputs. Repository,
social, AI-archive, and workspace end-to-end retrieval remain separate analysis-family work.
