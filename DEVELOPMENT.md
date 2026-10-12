# Developing Ratatoskr Knowledge

> Status: Active
> Last reviewed: 2026-08-21

The first article-analysis slice uses a scripted provider for offline gates, an `OpenRouter` adapter
for real inference behind timeout, size-cap, rate, retry, cancellation, and budget controls, and a
disposable `PostgreSQL` 17 database. PostgreSQL full-text search, pgvector embeddings with hybrid
ranking, and explicit reindex jobs are implemented; external message handling is not.

## Toolchain and gate

`rust-toolchain.toml` pins Rust 1.97. Every command uses the committed lock file.

### Rust - the CI gate

```bash
cargo fetch --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked
cargo test --workspace --locked
cargo test --workspace --locked --doc
cargo build --workspace --locked --release
```

`cargo deny --locked check` runs in its own `deny` job in the same workflow, not in the gate above, so
a new RustSec advisory cannot hide a clippy or test failure behind it.

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

Default tests use the scripted provider, recorded fixtures, loopback fake transports, and a
disposable local `JetStream`; they need no inference credentials and never reach the live API.
Database tests create disposable databases from the current `schema.sql`; this development
repository has no migrations. The live `OpenRouter`
smoke check (`cargo run --locked -p ratatoskr-knowledge --example live_openrouter_smoke`) spends
real credit and is never part of the gate.

To dry-run the result-reader configuration without binding a listener, pass the value through the
environment and use the production parser:

```bash
RATATOSKR__CHANNEL_RECAP__RESULT_READER_SERVICE_SECRET=test-only-reader-secret \
  cargo run --locked -p ratatoskr-knowledge-service -- check-config
```

The service binds two loopback listeners: the operator listener (`RATATOSKR__ADMIN__LISTEN_ADDRESS`,
default `127.0.0.1:9081`) and the API listener (`RATATOSKR__API__LISTEN_ADDRESS`, default
`127.0.0.1:8091`). The result route above is on the API listener. A process test that starts the real
binary must give each listener its own ephemeral port.

The keys a deployment states because the flow is not enabled by default are in
`deploy/systemd/knowledge.conf.example`: `RATATOSKR__API__LISTEN_ADDRESS`,
`RATATOSKR__INGEST__ENABLED` (ingest is dormant unless it is `true`),
`RATATOSKR__INGEST__BUS_CREDENTIALS_FILE`, `RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET`,
`RATATOSKR__CHANNEL_RECAP__ENABLED`, `RATATOSKR__CHANNEL_RECAP__PROVIDER_MODE` (default `openrouter`;
`scripted` is accepted only with an anonymous loopback `nats://` broker),
`RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE` and the digest-source and result-reader secrets.
`services/knowledge/tests/deployment_profile.rs` loads the example through the production
configuration loader, so it cannot drift from the code. The recap worker's `FETCH_BATCH` defaults to
one and its redelivery is bounded (`min(2 s * 2^(delivered-1), 60 s)`).

The route test uses an in-process router and disposable database; the boot test starts the real
binary twice and verifies enabled/disabled routing plus bounded `SIGTERM` drain. No gate command
uses a production credential or a real recap.

## Offline evaluation and one-shot jobs

Run the committed quality corpus without credentials, sockets, or live provider requests:

```bash
cargo run --locked -p ratatoskr-knowledge --example eval_harness
cargo run --locked -p ratatoskr-knowledge --example channel_recap_eval
```

The output is a timestamp-free report ordered by response-set label and fixture id, so it can be
compared byte-for-byte between prompt or model recordings. It reads only
`crates/knowledge/fixtures/eval/`. The channel recap evaluator similarly uses only its committed
synthetic manifest/result corpus and opens no socket.

The service also exposes non-listening one-shot jobs:

```bash
cargo run --locked -p ratatoskr-knowledge-service -- delete-source <tenant> <owner_context> <document_id>
cargo run --locked -p ratatoskr-knowledge-service -- delete-tenant <tenant>
cargo run --locked -p ratatoskr-knowledge-service -- reindex-search-documents [--tenant <tenant> [--source-doc <owner_context>:<document_id>]]
cargo run --locked -p ratatoskr-knowledge-service -- reindex-embeddings [--tenant <tenant> [--source-doc <owner_context>:<document_id>]]
```

Successful jobs exit `0`; invalid invocation, missing required embeddings configuration, connection
failure, or a job with failed sources exits nonzero. Deletion stdout is a machine-readable receipt
with all deleted derived-row and blob counts. Reindex stdout prints one committed source result in
ascending source id order and a final `processed`/`failed` total. These jobs do not start the admin
listener; embeddings reindex refuses a missing credentials/configuration before database access.

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

## Database prerequisite

Database tests create disposable databases from `schema.sql`, which requires PostgreSQL 17 with the
pgvector extension available (`create extension if not exists vector` runs as part of the schema).
CI runs a digest-pinned `pgvector/pgvector:pg17` service container. For local runs, start the same
kind of database and point the test URL at it:

```bash
docker run -d --name ratatoskr-knowledge-pgvector \
  -e POSTGRES_USER=knowledge -e POSTGRES_PASSWORD=knowledge -e POSTGRES_DB=knowledge \
  -p 127.0.0.1:15434:5432 pgvector/pgvector:pg17
export KNOWLEDGE_TEST_DATABASE_URL=postgres://knowledge:knowledge@127.0.0.1:15434/knowledge
```

A stock `postgres:17` image fails schema application with "extension vector is not available"; that
error means the image, not the code. A deployed host differs in one way: the service role is not a
superuser and cannot install pgvector, so a PostgreSQL administrator runs `deploy/postgres/role.sql`
once before the first start (it creates the `ratatoskr_knowledge` role, installs the extension and
grants the role `usage` on schema `public`, where the `vector` type lives); see `deploy/README.md`.

The recap process boot test also expects a disposable JetStream endpoint. Start the digest-pinned
fixture on the documented local port before the full gate:

```bash
docker run -d --name ratatoskr-knowledge-nats -p 127.0.0.1:14223:4222 \
  nats@sha256:d4ac35882ac65aff236cd65b9d3fa4d24332c681e1a85f94eedccd3cdd65b1da -js
export KNOWLEDGE_TEST_NATS_URL=nats://127.0.0.1:14223
```

The test provisions only its fixed command stream and durable on this disposable broker. Production
topology remains an operator-owned precondition; Knowledge never creates or widens it.

The ingest tests (`services/knowledge/tests/ingest_bus/`) recreate the `ratatoskr_events` stream and
the four ingest durables on that same broker before every test, because the names are compiled
constants of the service; they run one at a time. Two kinds of test start their own broker process on
a port the kernel picks: the outage test, and the authorized-broker tests, which boot an
authorization-enabled server with exactly the stanza in `deploy/nats/identity.conf` and run the
recap and ingest workers under the Knowledge nkey. They need a `nats-server` binary on `PATH`, or
`KNOWLEDGE_TEST_NATS_SERVER=/absolute/path/to/nats-server`. CI extracts the binary from the same
digest-pinned image as the fixture above, so the test broker and the gate broker are one version.

## Bus credentials and durables

One nkey seed, `/etc/ratatoskr/knowledge.nkey`, serves both workers. The seed is a file read at
runtime and never logged; the public key belongs in `ratatoskr-platform/deploy/nats/ratatoskr.conf`,
where `deploy/nats/identity.conf` is the reviewed copy of this service's stanza.

```bash
RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE=/etc/ratatoskr/knowledge.nkey
RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS=30
RATATOSKR__INGEST__ENABLED=true
RATATOSKR__INGEST__BUS_CREDENTIALS_FILE=/etc/ratatoskr/knowledge.nkey
RATATOSKR__INGEST__ACK_WAIT_SECONDS=120
RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET=<the GitHub internal reader secret>
```

The two ack-wait keys only state the values Edge fixes on the durables (120 and 30 seconds,
XR-021 CONTRACTS.md S04): the loader refuses any other value, because `verify_consumer` would
refuse the durable and the worker would never consume.

`RATATOSKR__INGEST__FETCH_BATCH` defaults to 1. A worker handles one delivery at a time while the broker
starts each message's ack-wait clock when it delivers it, so a batch above 1 prefetches messages whose
120 s can run out behind a slow analysis; the expired deliveries come back with a higher delivery count
and can use up the repository family's five-delivery budget early. Raise it only when the batch size times the
worst-case handler time (provider timeout times attempts) stays below the ack wait.

A redelivered analysis continues the same run instead of failing on a state that has already advanced
(XR-021 CONTRACTS.md S02 rule 7). Each delivery makes at most two provider calls, one retry or one repair,
and a run may spend ten calls over all its deliveries; a call that failed transiently is recorded and is not
spent. A run whose last call failed transiently is reopened by the next delivery, however many times that has
happened; a run left mid-call by a stopped worker records that call as interrupted and
continues, and fails after two such calls; a response that was stored but never validated is validated again
without a provider call. A run that failed with an invalid response or a permanent provider error stays failed.

`RATATOSKR__LIMITS__PROVIDER_REQUESTS_PER_MINUTE` bounds the process: the recap and ingest workers
share one limiter that `main` builds, and the spend ledger is shared through the database.

The ack waits must equal the durables Edge provisions (30 s for the recap command, 120 s for the four
ingest durables); a worker verifies its durable at startup and stays not ready on any difference.
`RATATOSKR__INGEST__PROVIDER_MODE=fixture` builds a deterministic first-paragraph provider for the
article family only and is accepted only on a loopback broker; it exists so a captured URL becomes
searchable without an inference credential.

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
