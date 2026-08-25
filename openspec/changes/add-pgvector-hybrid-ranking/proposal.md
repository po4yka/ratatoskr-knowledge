# Add pgvector embeddings, chunk and model versioning, and hybrid ranking

## Why

Retrieval today matches only lexical form: a query succeeds when its words appear in the projected text, and fails when meaning differs but vocabulary does not. Plan item 7 adds dense semantic recall over analyses while keeping the lexical path, fuses both into one deterministic ranking, and standardizes vectors on PostgreSQL + pgvector so no second stateful service is added to the single-host deployment target.

## What Changes

- Add a versioned, deterministic chunking policy over the accepted analysis projection, with chunk target size and overlap under strict configuration.
- Add a narrow embeddings provider seam beside the chat-completions adapter: one OpenAI-compatible `/embeddings` endpoint adapter behind timeout, byte cap, rate, retry, cancellation, and budget controls, plus a scripted provider that makes no external request for tests and offline operation.
- Persist every vector with full identity: source revision, output reference, tenant, ordinal, chunking version, provider, model, dimensions, and embedding prompt version; queries bind the active model identity explicitly so vectors from different model versions are never mixed in one result set.
- Store embeddings as a pgvector column on Knowledge-owned tables with an HNSW cosine index; `schema.sql` gains the extension and remains the one editable schema definition.
- Add a bounded background indexing step over persisted analyses: poll-driven by durable run state (`persisted` -> `indexed`), batched, rate-limited, budget-checked, cancellation-safe, with explicit bounded failure records; indexing failure never erases a valid analysis result.
- Extend operator-plane retrieval with hybrid ranking that combines the existing weighted FTS score and cosine vector similarity through documented Reciprocal Rank Fusion with fixed parameter k=60, identical tenant scoping on both legs, deterministic tiebreakers, and graceful fallback to pure FTS when no embeddings provider is configured.
- Add an explicit `reindex-embeddings` service subcommand that deterministically regenerates vectors after a chunking or model version change and prunes superseded identities; it is idempotent and never mutates historical analyses silently.
- **BREAKING** to local development environments only: test databases now require PostgreSQL 17 with the pgvector extension available; CI switches its service container to a digest-pinned pgvector-enabled PostgreSQL 17 image. The database contract stays first-version and migration-free per the binding development status.

## Capabilities

### New Capabilities

- `embedding-search`: versioned chunking, pgvector persistence with per-vector model identity, the bounded background indexing step over persisted analyses, explicit reindex jobs on version changes, and hybrid FTS+vector ranking over the tenant-scoped search projection.

### Modified Capabilities

## Impact

- `schema.sql`: `create extension if not exists vector`, two new `knowledge.*` tables (embedding chunks, indexing failure records), HNSW index; edited in place, no migrations.
- `crates/knowledge`: new chunking module; new embeddings provider trait, scripted fake, and OpenAI-compatible adapter mirroring the item-5 adapter pattern; reuse of `RateLimiter`, `BudgetLedger`, and the controlled-provider composition; new indexing worker functions; hybrid branch in the ranked retrieval query.
- `services/knowledge`: new strict `RATATOSKR__PROVIDER__EMBEDDINGS__*` and chunk/index limit keys; one background task wired into the existing lifecycle and drain path; `/internal/search` hybrid behavior; new `reindex-embeddings` subcommand beside `check-config`.
- `.github/workflows/ci.yml`: PostgreSQL service image becomes pgvector-enabled; the gate command list itself does not change.
- `DEVELOPMENT.md` and `README.md`: document the pgvector prerequisite, new settings, and the reindex command.
- Dependencies: one new Rust dependency (`pgvector` crate with its sqlx integration) for typed vector binding; licenses checked through the existing deny gate.
- Deployment note: the shared PostgreSQL container on the deployment host must offer the pgvector extension before this slice ships; that check belongs to workspace deployment documentation, not this repository.
