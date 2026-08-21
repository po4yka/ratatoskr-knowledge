# Ratatoskr Knowledge

`ratatoskr-knowledge` is the interpretation bounded context for Ratatoskr. The current first slice
accepts canonical Document IR, prepares a bounded article prompt, validates a small structured
analysis, and persists the evidence and result.

> **Status:** the first article-analysis slice is implemented with a scripted provider. There is no
> real model adapter, message-bus consumer, public analysis endpoint, search index, or embedding
> pipeline.

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
- content-addressed raw-response bytes owned by Knowledge;
- structural, typed, citation, retry, repair, and replay checks;
- one transaction for an accepted result and the `persisted` transition;
- an admin-only process with `/live`, `/ready`, `/metrics`, and `/version`.

The process has no inference credential setting. Default tests and CI make no inference request.

## Data ownership

Knowledge owns only `knowledge.*` PostgreSQL objects and its blob root. The current tables are:

```text
source_refs
analysis_runs
analysis_attempts
analysis_outputs
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
```

Unknown or invalid `RATATOSKR__` keys stop startup without printing their values. Validate
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
databases from `schema.sql`. The committed result schema is
[`schemas/article-analysis.v1.schema.json`](schemas/article-analysis.v1.schema.json), and prompt
artifacts are under [`prompts/article-analysis.v1`](prompts/article-analysis.v1).

## Boundaries

Knowledge does not fetch web pages, run Chromium, synchronize provider accounts, execute Git, own
source records, expose search, or accept public analysis requests. Real inference, eventing, FTS,
embeddings, more analysis families, and legacy import remain separate planned changes.
