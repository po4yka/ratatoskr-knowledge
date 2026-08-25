# Ratatoskr Knowledge

`ratatoskr-knowledge` is the interpretation bounded context for Ratatoskr. The current first slice
accepts canonical Document IR, prepares a bounded article prompt, validates a small structured
analysis, and persists the evidence and result.

> **Status:** the first article-analysis slice is implemented, first against a scripted provider and
> now with a real `OpenRouter` adapter behind timeout, size-cap, rate, retry, cancellation, and
> budget controls, alongside a tenant-scoped full-text projection and operator-plane retrieval.
> Versioned pgvector embeddings and hybrid ranking are implemented behind an optional embeddings
> credential; lexical search remains the offline default. There is no message-bus consumer or
> public analysis endpoint yet.

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
- an idempotent `reindex-embeddings` subcommand for explicit regeneration after a model or
  chunking-version change;
- an admin-only process with `/live`, `/ready`, `/metrics`, `/version`, and `/internal/search`.

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
search_documents
embedding_chunks
embedding_failures
```

Source bytes stay with the source-owning service. Knowledge stores the immutable Document IR
identity and source `BlobRef`. Raw model-response bytes stay in the Knowledge blob root under their
SHA-256 address.

## Run the admin-only process

The process needs PostgreSQL 17 and a writable blob directory. These settings have finite defaults
and can be changed with strict environment keys:

```text
RATATOSKR__ADMIN__LISTEN_ADDRESS
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
```

Unknown or invalid `RATATOSKR__` keys stop startup without printing their values. Without
`RATATOSKR__PROVIDER__OPENROUTER__API_KEY` the process stays offline and the real adapter cannot
make a request; without `RATATOSKR__PROVIDER__EMBEDDINGS__API_KEY` the process serves lexical
search only and performs no embedding calls.
The credential redacts itself in diagnostics and serialization and is never persisted to the
database; ordinary logs carry only bounded facts such as provider, model, outcome class, status,
token counts, and latency. Validate
configuration without opening a listener:

```bash
cargo run --locked -p ratatoskr-knowledge-service -- check-config
```

Start the admin listener:

```bash
cargo run --locked -p ratatoskr-knowledge-service
```

Readiness becomes successful after the blob directory, database connection, and current schema are
ready. `SIGINT` or `SIGTERM` starts drain and uses the configured shutdown bound.

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

## Boundaries

Knowledge does not fetch web pages, run Chromium, synchronize provider accounts, execute Git, own
source records, accept public analysis requests, or expose retrieval anywhere but the operator
plane's `/internal/search`. Real `OpenRouter` inference is implemented through the library adapter
and manual smoke example; wiring analysis to events, embeddings, hybrid ranking, more analysis
families, and legacy import remain separate planned changes.

## Workspace integration

The planned `ratatoskr-workspace` topology will pin Knowledge with compatible source contracts and
producers. No workspace repository pins or cross-service Knowledge integration profile exist yet;
this repository remains independently buildable and testable.
