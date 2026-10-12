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

The process test starts the real binary with a disposable database, blob root, JetStream durable,
and fake authenticated digest source. It proves that `check-config` does not bind, scripted recap
needs no inference credentials, readiness waits for both recap dependencies, `/analyze` is absent,
`SIGTERM` drains within the bound, and a restart reopens the same durable and reprobes the source.
The result-reader matrix uses disposable PostgreSQL rows and the real router to prove completed-only
reads, exact digest/schema revalidation, uniform missing/wrong authorization, scoped absence,
finite no-store responses, safe `502`/`503` failures, disabled/enabled process startup, restart, and
bounded drain. All identifiers, secrets, and recap text are synthetic.

The ingest worker is proven against a real `JetStream` in one test binary, `services/knowledge/tests/ingest_bus/`.
`lifecycle.rs`: a document, a social capture and its removal, an AI-archive conversation and its
tombstone become searchable and disappear; wrong-producer and undecodable deliveries are terminated
and never looped; a redelivery after a crash between the inbox commit and the analysis resumes it; the completion facts
reach the stream once, even across a restart. `repository.rs`: a repository request reads its README
over the authenticated loopback route and publishes one completed fact; an unavailable README source
is redelivered and the fifth delivery settles the request as a retryable failed fact; bytes that fail
the digest settle it as a non-retryable failure on the first delivery. `tenant.rs`: a social
capture, a social removal, an archive conversation, an archive tombstone and a repository request
whose payload owner differs from the envelope `tenant_id` are terminated and leave no row in any
table. `redelivery.rs`: a document, a social capture, a social removal and an AI-archive conversation
survive a database fault of eight deliveries, a provider outage of eight deliveries and a provider
outage followed by a database outage, with no termination, the fact applied once and
`knowledge_ingest_redeliveries_total` counted; a permanent failure and an undecodable message are
terminated at the first delivery; the repository family still settles its failed fact on the fifth.
`ingest_outage.rs` proves that readiness follows the broker.

`authorized.rs` boots a broker that enforces exactly the Knowledge stanza of
`deploy/nats/identity.conf` and runs the real recap and ingest workers under that identity. It feeds
each of the five durables (the recap command and the document, social, AI-archive and repository
events) one valid message and requires every durable to show one delivery and nothing pending or
unacknowledged, the process to report ready, and the five completion facts to reach the stream once;
a second test observes the identity's refusals. Removing any one durable's grants, its acknowledgement
grant, its pull grant or a completion subject from the stanza turns the first test red. The test
proves the workers run under the stanza, not that the stanza equals Platform's copy; the workspace
compares the two.

The recap pipeline is killed in every durable state and resumed, and a second executor on a held run
is refused without touching it. Privacy deletion is proven not to touch another tenant's recap or
snapshot bytes, to heal a crash between the row commit and the byte collection, and to remove every
content-bearing inbox, tombstone, request and outbox row of its scope.

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
blob bytes; they also prove tenant isolation, that bytes are collected only from the erasure queue the
deleting transaction filled (nothing sweeps unreferenced bytes), and one-transaction audit
visibility. Process tests cover the operator deletion/reindex receipts, deterministic progress, and
the missing-embeddings fail-fast path.

`channel_recap_eval` adds eight synthetic cases: empty, partial/full multi-channel, edited,
repeated/conflicting, context-budget, malformed-manifest, and prompt-injection windows. It runs
canonical manifest verification, deterministic context preparation, and strict result validation,
then reports schema, citation, unsupported-claim, coverage, context-digest, and budget metrics. It
contains no real channel text and opens no socket.

Future real-provider recordings are deliberate manual artifacts, not gate inputs. Repository,
social, AI-archive, and workspace end-to-end retrieval remain separate analysis-family work.
