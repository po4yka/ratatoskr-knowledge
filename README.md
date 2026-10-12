# Ratatoskr Knowledge

`ratatoskr-knowledge` is the interpretation bounded context for Ratatoskr. The current first slice
accepts canonical Document IR, prepares a bounded article prompt, validates a small structured
analysis, and persists the evidence and result.

> **Status:** the first article-analysis slice is implemented, first against a scripted provider and
> now with a real `OpenRouter` adapter behind timeout, size-cap, rate, retry, cancellation, and
> budget controls, alongside a tenant-scoped full-text projection and operator-plane retrieval.
> Versioned pgvector embeddings and hybrid ranking are implemented behind an optional embeddings
> credential; lexical search remains the offline default. The optional channel-recap role consumes
> one exact pre-provisioned JetStream durable and publishes terminal recap facts, and the optional
> ingest role consumes four more (documents, social sources, AI archives, repository requests) and
> publishes the completion facts of what it analysed; there is no public analysis endpoint.

> [!IMPORTANT]
> Ratatoskr is in development. The API, database, and contracts keep their first version. The
> database has no migrations. [`schema.sql`](schema.sql) is the one editable schema definition.

## Implemented slice

- two Rust packages: `crates/knowledge` and `services/knowledge`;
- exact shared Document IR and identifier contract pins;
- immutable source revisions and idempotent analysis-run identities;
- monotonic run states and at most two recorded provider attempts;
- `ArticleAnalysis` with a summary and source-block-backed key points;
- deterministic complete-block context selection and a versioned prompt;
- a hand-written scripted provider that makes no external request;
- an `OpenRouter` chat-completions adapter with a per-try deadline, streaming response byte cap,
  transient-only jittered retry, fixed-spacing rate limiting, and cancellation-safe requests;
- a durable daily/monthly token and estimated-cost budget ledger enforced before each call;
- attempts that record provider identity, concrete model, latency, HTTP status, and failure class;
- content-addressed raw-response bytes owned by Knowledge;
- structural, typed, citation, retry, repair, and replay checks;
- one transaction for an accepted result and the `persisted` transition;
- a tenant-scoped full-text projection (`knowledge.search_documents`) written inside that
  transaction with a latest-wins output guard;
- ranked retrieval over the projection with bounded snippets, deterministic ordering, and strict
  page bounds validated before any database work;
- deterministic versioned chunking (`article-chunks.v1`) with configurable target size and overlap;
- a scripted embeddings provider plus an `OpenAI`-compatible `/embeddings` adapter behind timeout,
  byte-cap, rate, retry, cancellation, and budget controls mirroring the chat adapter;
- pgvector storage of every vector with full model identity — provider, model, dimensions,
  chunking version, prompt version — bound explicitly at query time so versions never mix;
- a bounded background indexing step that embeds accepted analyses through the durable
  `persisted -> indexed` run transition;
- hybrid Reciprocal Rank Fusion (k=60) over equally tenant-scoped lexical and semantic legs, with
  deterministic tiebreakers and graceful fallback to pure lexical ranking without a credential;
- committed offline evaluation fixtures and a deterministic labeled report runner;
- a dormant `knowledge.channel_digest_recap.requested.v1` consumer with owner-scoped inbox
  convergence, authenticated immutable-manifest retrieval, complete-revision context selection,
  strict grounded recap validation, terminal outbox publication, and synthetic evaluations;
- a dormant ingest worker (`RATATOSKR__INGEST__ENABLED`) with one supervised consumer per durable:
  `content.document.extracted.v1` becomes a registered source, an article analysis and a searchable
  document; `social.source.captured|updated|removed.v1` and the eight `ai_archive.*` facts go
  through the durable source inbox, are analysed when they are the head of their source, and a
  removal or tombstone deletes the derived state; `knowledge.repository_analysis.requested.v1` is
  analysed with the README bytes resolved from GitHub's internal reader route. Completion and
  failure facts are committed to `knowledge.event_outbox` in the transaction that produced them and
  published once to `evt.knowledge.*`;
- a channel-recap chain that conforms to the channel-digests manifest contract (the path and the
  three headers come from the contract crate, the bytes are verified by the contract type), asks
  the model only for what a model knows (headline, overview, topics, notable items, warnings), and
  composes the language, manifest digest and coverage itself, and that resumes a run from its
  durable state under a per-run advisory lock after a kill;
- atomic, audited `delete-source` and `delete-tenant` operator jobs that remove all owned derived
  data, the inbox, tombstone, request and outbox state of the scope, and only reference-safe
  Knowledge blob bytes through a durable erasure queue;
- idempotent `reindex-embeddings` and `reindex-search-documents` subcommands for explicit
  regeneration after a model, chunking, or lexical-projection change;
- two loopback listeners: an operator listener with `/live`, `/ready`, `/metrics` and `/version`, and
  an API listener (default `127.0.0.1:8091`, where Edge routes the Knowledge domain API) with the
  capability document, the `/internal/search` adapter, the user-content routes and an
  independently authenticated channel-recap result reader;
- tenant-scoped user content over accepted analyses: normalized tags and transactional tag merge,
  ordered collections of analysis outputs or immutable source revisions, read/favorite state,
  typed feedback, and Document-IR block anchored highlights.

The process has an optional `OpenRouter` inference credential setting. Default tests and CI do not
set it and make no live inference request.

## Data ownership

Knowledge owns only `knowledge.*` PostgreSQL objects and its blob root. The current tables are:

```text
source_refs
analysis_runs
analysis_attempts
analysis_outputs
provider_usage
repository_analysis_requests
event_outbox
social_tombstones
blob_erasure_queue
source_analysis_inbox
source_analysis_heads
ai_archive_object_inbox
ai_archive_tombstones
search_projection_inputs
search_documents
embedding_chunks
embedding_failures
deletion_records
tags
analysis_taggings
collections
collection_items
analysis_user_states
highlights
analysis_feedback
channel_recap_inbox
channel_recap_runs
channel_recap_manifests
channel_recap_attempts
channel_recap_results
channel_recap_outbox
```

Source bytes stay with the source-owning service. Knowledge stores the immutable Document IR
identity and source `BlobRef`. Raw model-response bytes stay in the Knowledge blob root under their
SHA-256 address.

## Run the process

The process needs PostgreSQL 17 with the pgvector extension and a writable blob directory. A
PostgreSQL administrator provisions the `ratatoskr_knowledge` role and installs the extension once,
before the first start, with `deploy/postgres/role.sql` (see `deploy/README.md`): pgvector is not a
trusted extension, so the service role cannot run the `create extension` statement in `schema.sql`
itself and startup fails with `permission denied to create extension "vector"` without that step. The
same file grants the role `usage` on schema `public`, where the `vector` type lives; without it
`schema.sql` fails with `type "vector" does not exist`.
These settings have finite defaults and can be changed with strict environment keys:

```text
RATATOSKR__ADMIN__LISTEN_ADDRESS
RATATOSKR__API__LISTEN_ADDRESS
RATATOSKR__STORAGE__DATABASE_URL
RATATOSKR__STORAGE__BLOB_ROOT
RATATOSKR__LIMITS__DATABASE_CONNECTIONS
RATATOSKR__LIMITS__DATABASE_ACQUIRE_TIMEOUT_MS
RATATOSKR__LIMITS__PROVIDER_TIMEOUT_MS
RATATOSKR__LIMITS__CONTEXT_CHARACTERS
RATATOSKR__LIMITS__RAW_RESPONSE_BYTES
RATATOSKR__LIMITS__SHUTDOWN_TIMEOUT_MS
RATATOSKR__LIMITS__BLOB_BYTES
RATATOSKR__PROVIDER__OPENROUTER__API_KEY
RATATOSKR__PROVIDER__OPENROUTER__BASE_URL
RATATOSKR__PROVIDER__OPENROUTER__MODEL
RATATOSKR__PROVIDER__OPENROUTER__INPUT_MICRO_USD_PER_MTOKEN
RATATOSKR__PROVIDER__OPENROUTER__OUTPUT_MICRO_USD_PER_MTOKEN
RATATOSKR__LIMITS__PROVIDER_MAX_OUTPUT_TOKENS
RATATOSKR__LIMITS__PROVIDER_REQUESTS_PER_MINUTE
RATATOSKR__LIMITS__PROVIDER_DAILY_TOKEN_BUDGET
RATATOSKR__LIMITS__PROVIDER_MONTHLY_TOKEN_BUDGET
RATATOSKR__LIMITS__PROVIDER_DAILY_COST_MICRO_USD
RATATOSKR__LIMITS__PROVIDER_MONTHLY_COST_MICRO_USD
RATATOSKR__LIMITS__CHUNK_TARGET_CHARACTERS
RATATOSKR__LIMITS__CHUNK_OVERLAP_CHARACTERS
RATATOSKR__LIMITS__EMBEDDINGS_TIMEOUT_MS
RATATOSKR__LIMITS__EMBEDDINGS_MAX_INPUT_CHARACTERS
RATATOSKR__LIMITS__EMBEDDINGS_BATCH_SOURCES
RATATOSKR__LIMITS__EMBEDDINGS_POLL_INTERVAL_MS
RATATOSKR__LIMITS__EMBEDDINGS_REQUESTS_PER_MINUTE
RATATOSKR__LIMITS__EMBEDDINGS_MAX_FAILURE_ATTEMPTS
RATATOSKR__LIMITS__EMBEDDINGS_DAILY_TOKEN_BUDGET
RATATOSKR__LIMITS__EMBEDDINGS_MONTHLY_TOKEN_BUDGET
RATATOSKR__LIMITS__EMBEDDINGS_DAILY_COST_MICRO_USD
RATATOSKR__LIMITS__EMBEDDINGS_MONTHLY_COST_MICRO_USD
RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY
RATATOSKR__PROVIDER__EMBEDDINGS__BASE_URL
RATATOSKR__PROVIDER__EMBEDDINGS__MODEL
RATATOSKR__PROVIDER__EMBEDDINGS__DIMENSIONS
RATATOSKR__PROVIDER__EMBEDDINGS__PROMPT_VERSION
RATATOSKR__PROVIDER__EMBEDDINGS__INPUT_MICRO_USD_PER_MTOKEN
RATATOSKR__CHANNEL_RECAP__ENABLED
RATATOSKR__CHANNEL_RECAP__PROVIDER_MODE
RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_BASE_URL
RATATOSKR__CHANNEL_RECAP__DIGEST_SOURCE_SERVICE_SECRET
RATATOSKR__CHANNEL_RECAP__RESULT_READER_SERVICE_SECRET
RATATOSKR__CHANNEL_RECAP__BUS_ENDPOINT
RATATOSKR__CHANNEL_RECAP__BUS_STREAM
RATATOSKR__CHANNEL_RECAP__BUS_DURABLE
RATATOSKR__CHANNEL_RECAP__BUS_SUBJECT
RATATOSKR__CHANNEL_RECAP__BUS_CREDENTIALS_FILE
RATATOSKR__CHANNEL_RECAP__FETCH_BATCH
RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS
RATATOSKR__INGEST__ENABLED
RATATOSKR__INGEST__PROVIDER_MODE
RATATOSKR__INGEST__BUS_ENDPOINT
RATATOSKR__INGEST__BUS_CREDENTIALS_FILE
RATATOSKR__INGEST__FETCH_BATCH
RATATOSKR__INGEST__ACK_WAIT_SECONDS
RATATOSKR__INGEST__GITHUB_README_BASE_URL
RATATOSKR__INGEST__GITHUB_README_SERVICE_SECRET
```

Unknown or invalid `RATATOSKR__` keys stop startup without printing their values. Without
`RATATOSKR__PROVIDER__OPENROUTER__API_KEY` the process stays offline and the real adapter cannot
make a request; without `RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY` the process serves lexical
search only and performs no embedding calls.
Without `RATATOSKR__CHANNEL_RECAP__RESULT_READER_SERVICE_SECRET`, the result-reader route is not
installed. Supplying it enables only that route; it does not enable the recap worker and it is not
interchangeable with the digest-source credential used in the opposite direction.
The credential redacts itself in diagnostics and serialization and is never persisted to the
database; ordinary logs carry only bounded facts such as provider, model, outcome class, status,
token counts, and latency. Validate
configuration without opening a listener:

```bash
cargo run --locked -p ratatoskr-knowledge-service -- check-config
```

Start the listeners:

```bash
cargo run --locked -p ratatoskr-knowledge-service
```

Readiness becomes successful after the blob directory, database connection, and current schema are
ready. When channel recap is enabled it additionally requires the recap worker's bus connection and
the exact pre-provisioned durable: a missing or different durable is Knowledge's own
misconfiguration. It does not require the digest source. Whether the channel-digests API answers an
authenticated `GET /ready` is reported by the gauge `knowledge_channel_recap_source_up` (1 or 0), so
ingest and search keep serving while it is down. The recap worker does not pull commands while the
source is down, so they wait on the durable; it probes again after 1 second, doubling to 30
seconds, and logs one warning when the source becomes unreachable and one line when it answers
again. `SIGINT` or `SIGTERM` makes readiness fail, drains the consumer, and joins it within the
configured shutdown bound. Readiness also requires both listeners bound.

The recap provider mode defaults to `openrouter`, which requires the
existing controlled provider configuration and spend limits; `scripted` mode answers every recap
with fixed text, is accepted only with an anonymous loopback `nats://` broker, and logs a warning
naming the mode at startup, so an operator who only sets `RATATOSKR__CHANNEL_RECAP__ENABLED=true`
cannot serve it to real users. The recap worker fetches one message at a time, acknowledges
progress every third of its ack wait while a message is being processed, and redelivers a
transiently failed message after `min(2 s * 2^(delivered-1), 60 s)`; each redelivery is counted in
`knowledge_channel_recap_redeliveries_total`.

## Ingest worker

The ingest worker is dormant unless `RATATOSKR__INGEST__ENABLED=true`. Stream and durable names are
compiled constants, provisioned by the platform and verified, never created, by the worker:

```text
ratatoskr_events / ratatoskr_knowledge_documents           evt.content.document.extracted.v1
ratatoskr_events / ratatoskr_knowledge_social_sources      evt.social.source.>
ratatoskr_events / ratatoskr_knowledge_ai_archive          evt.ai_archive.>
ratatoskr_events / ratatoskr_knowledge_repository_requests evt.knowledge.repository_analysis.requested.v1
```

Each durable is explicit-ack, deliver-all, unlimited redelivery, with an ack wait of 120 seconds
that Edge fixes (XR-021 S04) and that Knowledge verifies. `RATATOSKR__INGEST__ACK_WAIT_SECONDS` and
`RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS` exist only to state that value (120 and 30); any other
value stops startup, because a service that disagrees with its durable never starts consuming. A
delivery from the wrong producer, without a tenant,
undecodable, of an unsupported type, or whose tenant differs from the payload owner is terminated on
first delivery, and an AI-archive tombstone for a subject that belongs only to another tenant is
terminated with the class `foreign_subject`. A failed analysis, a refused write and a provider outage
are redelivered, with no delivery budget, after `min(2 s * 2^(delivered - 1), 60 s)`, so a database
outage of any length never drops a document, a social capture, an X removal or an AI conversation;
the ingestion is applied once the database recovers. A provider outage is no exception: a call that
failed transiently is recorded and not spent, so it never counts against the run's ten spent calls, and
the run is reopened by every delivery until the provider answers. A refusal by the provider spend budget is a wait in the same way: the daily and monthly ceilings clear at their rollover or when the operator raises them, so the call is recorded as a non-spent refusal and the capture is redelivered until the window clears (the events stream keeps a message for seven days, which bounds a very long refusal). Each redelivery is counted by the label-free
`knowledge_ingest_redeliveries_total` on the operator listener, and from the second delivery on it
logs the family, the class and the delivery count, never content. A delivery that can never succeed
(a response that stayed invalid, a permanent provider failure, a source the schema cannot hold) is
terminated at its first delivery with a closed class. Repository requests keep a budget of five
deliveries, because their terminal delivery settles a `knowledge.repository_analysis.failed.v1` fact
so the requester is told. A redelivered tombstone also drains the blob erasure queue
before it is acknowledged, so a delivery that committed the row deletion and then failed finishes
the byte erasure. A redelivery whose inbox
row was committed before the analysis ran resumes the analysis when that delivery is still the head
of its source; a delayed older delivery is kept as a receipt and never regresses the head.

`knowledge.analysis.completed.v1`, `knowledge.ai_archive_analysis.completed.v1` and the two
repository facts are rows of `knowledge.event_outbox`, committed with the state change that produced
them and published to `evt.<subject>` with `Nats-Msg-Id` equal to the outbox id, so a crash between
the broker's acknowledgement and the `published_at` update republishes a message the broker drops.
`/ready` additionally requires the four consumers to be verified and connected and the publisher not
to have failed for more than 30 seconds. If a worker task ends for any reason before shutdown, the
process begins drain and exits non-zero.

`RATATOSKR__INGEST__PROVIDER_MODE=fixture` is accepted only with a loopback broker. It answers the
article analysis with the document's first paragraph so a captured URL is searchable without an
inference credential, and it answers no other family.

## Internal channel-recap result surface

`GET /internal/channel-digest-results/{analysis_id}` is installed on the loopback API listener
only when the dedicated result-reader secret is configured. It accepts `Authorization: Bearer
<secret>` and one UUID from the published recap completion fact. A successful response contains
only `analysis_id`, its exact SHA-256 `result_digest`, and the closed typed `recap`; it is bounded to
64 KiB and uses `Cache-Control: no-store`. Missing, incomplete, failed, foreign-family, and random
identifiers are all the same scoped `404`. Stored digest/schema failure is `502`; database
unavailability is `503`; no failure returns partial recap content.

For deployment, keep the listener on loopback and inject this secret from the service secret source
into Knowledge and the channel-digest reader only. Rotate it by installing a new value in both
secret sources, restarting Knowledge first and its reader second, and probing a random UUID: the
new credential must receive scoped `404` while the old credential receives `401`. Rollback reverses
the consumer first, then Knowledge. Never print either value or pass it on a command line captured
by process listings.

## Internal user-content surface

`POST /internal/user-content/command` accepts a bounded JSON command with a required `tenant`.
Supported operations are `create_tag`, `merge_tags`, `tag_analysis`, `create_collection`,
`add_collection_item`, `move_collection_item`, `set_analysis_state`, `set_read_state`,
`record_feedback`, and `create_highlight`. `set_read_state` changes only the effective read state
and preserves favorite. `GET /internal/user-content/collection?tenant=<tenant>&collection_id=<uuid>`
lists collection items in durable order. All responses use `Cache-Control: no-store`; a foreign
identifier is reported as the same scoped absence as a missing one.

`GET /internal/search` requires `tenant` and accepts optional `q`, `read_state=read|unread`,
`limit`, and `offset`. It returns the accepted `analysis_id`, effective `read_state`, and
`has_more`; a missing state row is unread. Tenant and state filtering happen before ranking and
pagination. This is a loopback adapter for Platform, not a public client API. Its fleet-visible
contract is `library-search-read-state` in the `ratatoskr-workspace` OpenSpec store.
`GET /v1/capabilities` is the bounded loopback document Platform samples; it names both
`library.search` and `library.read_state`, and a partial document is not sufficient for either
public capability.

Highlights include only source revision identity, a stable Document IR block id, Unicode-scalar
offsets, and style (`yellow`, `green`, `blue`, `pink`, `purple`, or `underline`). The command
validates supplied Document IR against the accepted analysis source digest and does not persist
the supplied block text. Source and tenant deletion receipts include dependent user-content rows.

This first slice intentionally excludes multi-user collaboration and invites, public collection
links, saved searches, goals or streaks, highlight rebasing, and import of legacy user content.

## Development

[`DEVELOPMENT.md`](DEVELOPMENT.md) is the exact local and CI gate. Database tests create disposable
databases from `schema.sql`; wire-format contract tests run against recorded fixtures and a local
fake transport, never the live API. To spend real credit deliberately, run the manual smoke check:

```bash
RATATOSKR__PROVIDER__OPENROUTER__API_KEY=... \
  RATATOSKR__PROVIDER__OPENROUTER__MODEL=openai/gpt-oss-20b \
  cargo run --locked -p ratatoskr-knowledge --example live_openrouter_smoke
```

The committed result schema is
[`schemas/article-analysis.v1.schema.json`](schemas/article-analysis.v1.schema.json), and prompt
artifacts are under [`prompts/article-analysis.v1`](prompts/article-analysis.v1).
The recap prompt is independently fixed under
[`prompts/channel-digest-recap.v1`](prompts/channel-digest-recap.v1); source labels and untrusted
complete revisions remain separate request fields, and external fetch is always disabled.

## Offline evaluation and operator jobs

The default quality gate never calls a live provider. It scores the committed, non-sensitive source
fixtures and recorded response sets, grouping results by provider/prompt label:

```bash
cargo run --locked -p ratatoskr-knowledge --example eval_harness
cargo run --locked -p ratatoskr-knowledge --example channel_recap_eval
```

Operator jobs use the configured database and blob root. `delete-source` removes every revision of
the logical source, and `delete-tenant` removes all owned source revisions. Both print a per-table
receipt and create a deletion audit record in the same database transaction. Their owned raw-output
blobs are removed only after reference checks; externally owned source blobs are never removed.

```bash
cargo run --locked -p ratatoskr-knowledge-service -- delete-source <tenant> <owner_context> <document_id>
cargo run --locked -p ratatoskr-knowledge-service -- delete-tenant <tenant>
```

Reindex jobs accept no scope, `--tenant <tenant>`, or `--tenant <tenant> --source-doc
<owner_context>:<document_id>`. They print committed source progress in source-id order followed by
totals. A rerun converges without rewriting unchanged rows; an interrupted run resumes from durable
per-source work. Embedding reindex requires a usable embeddings configuration and otherwise fails
before opening the database.

```bash
cargo run --locked -p ratatoskr-knowledge-service -- reindex-search-documents [--tenant <tenant> [--source-doc <owner_context>:<document_id>]]
cargo run --locked -p ratatoskr-knowledge-service -- reindex-embeddings [--tenant <tenant> [--source-doc <owner_context>:<document_id>]]
```

## Boundaries

Knowledge does not fetch web pages, run Chromium, synchronize provider accounts, execute Git, own
source records, accept public analysis requests, or expose retrieval anywhere but the loopback API
listener's `/internal/search` (default `127.0.0.1:8091`, never the operator listener). It accepts
the internal, contract-bound repository-analysis request on the ingest worker's
`ratatoskr_knowledge_repository_requests` durable, analyses the repository README it fetches from
GitHub's loopback byte endpoint, and publishes the completion or failure fact through the
transactional outbox; the worker is dormant unless `RATATOSKR__INGEST__ENABLED=true`. Real `OpenRouter`
inference is implemented through the library adapter and the manual smoke example; legacy import
remains a separate planned change.

## Workspace integration

`ratatoskr-workspace` pins this repository as `repos/knowledge` (profiles `core`, `social` and
`ai-archive`) together with the producers and contracts it consumes, and its harness checks the
parts of this repository that other services depend on: `deploy/nats/identity.conf` against the
KNOWLEDGE stanza of Platform's ACL, the five durables this service verifies against the table Edge
provisions, and the ports in `deploy/systemd/knowledge.conf.example` against the deployment target.
The workspace's Telegram library profile starts Knowledge for search and read-state; it needs Docker
and has not been run for this snapshot. The only cross-service run of Knowledge so far is the native
smoke of the channel-digest recap path (Platform Edge, the bus, channel-digests, Knowledge with the
scripted provider, back to the Edge operation), which passed with Knowledge's bus credential
removed, so the deployed ACL was not in force for it; ingestion from the extractor, social
captures, tombstone delivery and the repository analysis loop have no cross-service run. This
repository remains independently buildable and testable.
