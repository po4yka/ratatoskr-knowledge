# Design

## Context

Item 6 delivered the tenant-scoped lexical projection (`knowledge.search_documents`, written inside the persist transaction) and operator retrieval (`rank_matches`, `browse_recent`, strict page validation before any database work). Item 5 delivered the `LlmProvider` seam, the `OpenRouterProvider` adapter, and the reusable control composition (`RateLimiter`, `BudgetLedger` with per-provider windows, `ControlledProvider`). The service currently has no background tasks; startup, readiness, and graceful drain are linear in `services/knowledge/src/main.rs`. Tests run against disposable databases created from `schema.sql`; CI uses a stock digest-pinned `postgres:17` container, which does not ship pgvector. The deployment target (`ratatoskr-workspace/docs/DEPLOYMENT_TARGET.md`) is one Raspberry Pi 5 whose services measure resident memory in single-digit MiB under 256–768M ceilings, and whose port table already lists 6333/6334 — the legacy Qdrant ports — as held by other software.

## Goals / Non-Goals

Goals: a deterministic versioned chunking policy; complete per-vector identity with strict query-time version isolation; pgvector persistence with cosine indexing inside the owned schema; a bounded, crash-safe background indexing step driven by the existing run state machine; documented Reciprocal Rank Fusion over equally tenant-scoped legs; an explicit idempotent reindex command.

Non-Goals: repository, social, or AI-archive embeddings (item 9); message-bus consumption or public analysis endpoints; an evaluation harness or privacy deletion (item 8); serving more than one embedding identity simultaneously; a persistent cross-restart query-embedding cache; any second vector store.

## Decisions

### D1: pgvector in the owned PostgreSQL schema; remote OpenAI-compatible embeddings; no local model

The legacy monolith kept vectors in Qdrant behind synchronous fast-path writes plus a reconciler. The new target standardizes on PostgreSQL + pgvector: the deployment host cannot offer another stateful service (Qdrant's default ports 6333/6334 are already occupied there), the repository rules name PostgreSQL FTS + pgvector with hybrid ranking as the default architecture, and co-locating vectors with the tenant-scoped projection keeps authorization filters structurally identical on both legs instead of reconciling two systems.

The embedding computation itself goes to one remote OpenAI-compatible `/embeddings` endpoint behind the item-5 adapter pattern. A local ONNX sentence-transformers model was considered and rejected against `DEPLOYMENT_TARGET.md`: an ONNX runtime plus a MiniLM-class model adds hundreds of MiB of resident memory and sustained four-core CPU load to a shared single-board host whose services today use 2–7 MiB, complicates the frozen debian:12 arm64 ABI story with native `ort` builds, and adds a model-artifact distribution path — while a remote endpoint reuses the proven timeout, byte-cap, rate, retry, cancellation, and budget controls and prices spend through the durable ledger. Offline behavior (tests, CI, no credential) is covered by the scripted provider, so nothing in the gate reaches the network.

### D2: A separate narrow embeddings seam beside the chat seam

New trait in the library crate, same RPITIT style as `LlmProvider`:

```rust
pub trait EmbeddingProvider: Send + Sync {
    fn identity(&self) -> EmbeddingIdentity; // provider, model, dimensions, prompt_version
    fn embed(&self, inputs: Vec<String>) -> impl Future<Output = Result<EmbeddingResponse, ProviderError>> + Send;
}
```

`EmbeddingIdentity.provider` is a stable string (for the ledger and records, e.g. `openai-compatible`); `prompt_version` is an opaque reviewed label recorded per row (embeddings endpoints take no instruction, so the label documents any prefixing policy, initially `none.v1`). Implementations: `ScriptedEmbeddingProvider` (ordered outcomes + capture, deterministic hash-derived unit vectors, zero network) and `OpenAiCompatibleEmbeddings` (POST `{base_url}/embeddings`, per-try deadline, connect timeout, response byte cap, transient-only jittered retry, error classification into the existing closed vocabulary). A generic `ControlledEmbeddings<P>` composes limiter admission, `BudgetLedger::ensure_within_budget` (projected tokens from characters/4, output tokens 0), call, tracing, and `record_usage` — mirroring `ControlledProvider`. The ledger keys windows on the provider string, so embedding spend is accounted separately from chat spend against its own configured ceilings. Forcing embeddings through `generate_json` was rejected: the response shapes, validation, and cost model differ, and a wide interface would hide that.

Configuration joins the strict loader with finite defaults; unknown keys still abort startup. New keys: `RATATOSKR__PROVIDER__EMBEDDINGS__{API_KEY, BASE_URL, MODEL, DIMENSIONS, PROMPT_VERSION, INPUT_MICRO_USD_PER_MTOKEN}` (credential absent means offline, exactly like chat), `RATATOSKR__LIMITS__{EMBEDDINGS_TIMEOUT_MS, EMBEDDINGS_MAX_INPUT_CHARACTERS, EMBEDDINGS_BATCH_SOURCES, EMBEDDINGS_POLL_INTERVAL_MS, EMBEDDINGS_REQUESTS_PER_MINUTE, EMBEDDINGS_MAX_FAILURE_ATTEMPTS, EMBEDDINGS_DAILY_TOKEN_BUDGET, EMBEDDINGS_MONTHLY_TOKEN_BUDGET, EMBEDDINGS_DAILY_COST_MICRO_USD, EMBEDDINGS_MONTHLY_COST_MICRO_USD}`, and `RATATOSKR__LIMITS__{CHUNK_TARGET_CHARACTERS, CHUNK_OVERLAP_CHARACTERS}`. Startup validation: overlap < target, dimensions equal the storage dimensionality, model required when the credential is present; violations stop startup before listening.

### D3: Chunking policy `article-chunks.v1`

Pure function over the projected `(title, lead, body)`: normalize line endings, split body into paragraphs on blank lines, greedily pack paragraphs into windows up to the configured target, hard-split any single paragraph larger than the target on char boundaries (v1 accepts mid-sentence splits; changing this bumps the version), and prepend the final `overlap` characters of the previous window to the next. Every stored chunk text is `title + "\n\n" + window`, giving short chunks title context; the chunk SHA-256 digest is recorded. Below-target inputs produce exactly one chunk. Determinism comes from operating on the already-projected text with no clock, randomness, or map iteration order; the policy identifier is a constant compiled into the writer and stamped on every row.

### D4: Storage and identity isolation

`schema.sql` gains, edited in place: `create extension if not exists vector;`, a `knowledge.embedding_chunks` table keyed `unique (source_ref_id, chunking_version, provider, model, prompt_version, ordinal)` with `tenant_ref, owner_context, document_id, output_id, chunk_text, chunk_digest_hex, dimensions, embedding vector(1536)`, and `created_at`; an HNSW index on the vector column with `vector_cosine_ops`; and a `knowledge.embedding_failures` table holding at most one row per `(source_ref_id, chunking_version, provider, model, prompt_version)` with `error_class` (same closed vocabulary as attempts), `attempt`, `detail_code`, and timestamps, upserted in place so failure storage stays bounded. Dimensionality is pinned at 1536 (the declared default model class) because a similarity index requires a fixed typmod; the column check and the startup validation both enforce it, and a differently dimensioned model during the migration-free development status means editing `schema.sql` in place — consistent with the binding rules. Version isolation is structural: every read binds the full identity tuple from typed configuration, so superseded rows are invisible rather than filtered after scoring.

Writes happen in one transaction per source: upsert all chunk rows for the active identity (`on conflict ... do update` covering embedding, texts, digests, output reference), delete stale ordinals beyond the new count, delete the source's failure row, and perform the guarded `persisted -> indexed` transition requiring exactly one affected row. Any failure rolls back the whole source, leaving the previous state intact.

### D5: Background indexing as a poll over durable state

One worker task spawned beside the listener: select accepted-output runs in `persisted` ordered oldest-first up to `EMBEDDINGS_BATCH_SOURCES`, embed each as described, and repeat until quiet, then sleep `EMBEDDINGS_POLL_INTERVAL_MS` (immediate first pass at startup). Selection reads only durable state, so crashes and restarts converge with no queue infrastructure; this replaces the legacy synchronous fast-path deliberately — the legacy reconciler existed to repair drift between two stores, which one-store persistence eliminates, and the poll bounds worst-case indexing lag explicitly. Sources lacking a search projection are counted and skipped (they cannot be chunked under the policy). On drain, the worker finishes the current source or abandons it mid-call (the run simply stays `persisted` and is retried next boot) and exits within the existing shutdown bound; the provider call itself remains cancellation-safe through its deadline.

### D6: Hybrid ranking with documented fusion

For non-blank queries with an embeddings provider configured: embed the query text once, then one SQL statement computes two bounded candidate legs — lexical ranked by the existing weighted `ts_rank_cd` ordering, semantic ranked by cosine distance over `embedding_chunks` bound to the active identity tuple and joined to the tenant-filtered projection, taking each document's best chunk. Candidate depth is `min(200, offset + limit + 25)` per leg, a documented deterministic function of the requested page. Fusion scores each candidate `sum(1 / (60 + rank))` across legs (Reciprocal Rank Fusion, `k = 60`, constants named and tested), orders by fused score descending with `updated_at desc, search_document_id desc` tiebreakers, applies `limit`/`offset` last, and renders snippets with the existing headline settings for every returned row. Both legs apply `tenant_ref = $1` inside their own `where` clause before limiting, so authorization is equivalent by construction. Without a configured provider, or when query embedding fails after retries, retrieval falls back to the untouched lexical path and reports the degradation through metrics and logs rather than failing the request; blank queries keep `browse_recent`.

### D7: Reindex as an explicit subcommand

`reindex-embeddings` joins `check-config` as a service subcommand. It resolves the active identity from configuration, enumerates sources whose latest accepted output lacks complete active-identity coverage or carries rows under other identities, and processes them one source at a time with the same chunk-embed-persist-prune transaction as the worker, deleting superseded-identity rows only inside the successful transaction. It prints per-source and total counts, leaves `analysis_outputs` and run history untouched, exits nonzero on provider or database failure with completed work persisted, and is safe to rerun: a fully converged database yields an empty plan and zero provider calls. Startup and search never adopt a new identity silently — they either bind the configured identity read-only (search) or process only fresh `persisted` runs (worker).

### D8: Environment and CI parity

CI's service container becomes a digest-pinned `pgvector/pgvector:pg17` image with identical credentials, ICU initdb arguments, and health check; the gate command list itself does not change. `DEVELOPMENT.md` documents the local prerequisite (any PostgreSQL 17 offering the `vector` extension) and points at the container recipe used locally. The Rust side binds vectors through the `pgvector` crate's sqlx integration rather than hand-rolled text casting.

## Risks / Trade-offs

- [Deployment host's shared PostgreSQL may lack the pgvector extension] -> startup fails fast with a specific readiness error; enabling it is a workspace deployment concern already flagged in the proposal Impact.
- [Query-time embedding adds provider latency to `/internal/search`] -> bounded deadline plus automatic lexical fallback; metrics record which path served each page.
- [Fusion sees only bounded candidate legs] -> depth rule is documented, deterministic, and centralized; adequate at current corpus scale and revisitable by changing one tested constant.
- [Hard paragraph splits cut sentences] -> accepted v1 simplicity; fixing it is a versioned policy change, not a silent edit.
- [Dimensionality pinned to 1536] -> adopting a different-dimension model family edits `schema.sql` in place while the development status holds; startup validation names the mismatch precisely.
- [First background task in this process] -> selection is purely durable-state-driven and writes are transactional per source, so interruption at any point converges on restart.

## Migration Plan

None beyond editing `schema.sql` in place: test databases are created fresh from the definition, CI's image swap lands in this change, and no environment holds data that must survive (binding development status). Rollback is reverting the branch; no data preservation obligations exist.

## Open Questions

None. The deployment-host extension enablement is explicitly a workspace concern, recorded in the proposal Impact section.
