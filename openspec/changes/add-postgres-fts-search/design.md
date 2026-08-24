# Design: add-postgres-fts-search

## Context

The pipeline persists accepted article analyses in one transaction (`ArticlePipeline::persist`) that inserts into `knowledge.analysis_outputs` and transitions the run to `persisted`. Source identity (`source_refs`), tenant and ownership columns, and the canonical Document IR supplied to each run already exist; nothing reads them back for retrieval. The admin router currently holds only lifecycle state and serves `/live`, `/ready`, `/metrics`, `/version`; the database handle stays in `main`. See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**

- One durable, inspectable search projection row per source identity, written atomically with the accepted output.
- Ranked full-text retrieval over the weighted projection with bounded snippets and explicit pagination semantics.
- Tenant scoping enforced identically in the library query path and the HTTP path.
- Everything testable against disposable databases created from `schema.sql`.

**Non-Goals:**

- Embeddings, pgvector, hybrid ranking (separate planned change).
- Event-driven or asynchronous index maintenance, tombstones, deletion propagation.
- Language-specific text configurations, saved searches, counts/facets, public API exposure.

## Decisions

### D1: Projection writes inside the existing persist transaction

`persist()` extends to insert or upsert the search document on the same `Transaction<'_, Postgres>` it already opens. Alternatives considered: an after-commit hook (leaves a window where the output exists but the document does not, and failure would need retry machinery we do not have yet) and a transactional outbox plus future event consumer (correct long-term shape once events exist, but heavy for a single-row upsert today; adopting events later can replace this writer without changing the projection contract).

### D2: Latest-wins upsert guarded by output identity

`search_documents.source_ref_id` carries a unique constraint; the writer uses `INSERT ... ON CONFLICT (source_ref_id) DO UPDATE ... WHERE excluded.latest_output_id > search_documents.latest_output_id`. Output ids are UUIDv7, so byte-wise comparison equals time ordering. This makes redelivery of the same output idempotent and makes stale replays lose. Alternative considered: keeping every revision in the projection table and selecting the max at read time — rejected because `analysis_outputs` already preserves history and the projection should answer "current view" cheaply.

### D3: Deterministic weighted extraction from Document IR

Title = `Document.title`, else the first Heading block's text, else empty string. Lead = the first Paragraph block's text (weight B). Body = remaining Paragraph blocks plus the text of any later Heading blocks (weight C). The tsvector is a stored generated column combining weights A/B/C, indexed with GIN. Extraction lives in the new `crates/knowledge/src/search.rs` as a pure function over `&Document`, unit-tested without a database. Alternatives considered: computing tsvector at query time (repeats work, cannot encode weights declaratively in the table) and storing pre-flattened text only (loses title emphasis).

### D4: Fixed `'english'` text-search configuration in version one

Both the generated column and the query side use the same fixed regconfig so matching and ranking are consistent. Per-document language selection needs an evaluation corpus and belongs with the embeddings/hybrid-ranking change; the choice is recorded here so switching later is a deliberate schema-and-replay decision, not drift.

### D5: Query shape: `websearch_to_tsquery`, `ts_rank_cd`, bounded headline

Queries compile user text with `websearch_to_tsquery('english', q)` (operator-friendly, never raises on odd input). Ranking uses `ts_rank_cd` descending with `(updated_at DESC, id)` tie-breaks for determinism. Snippets use `ts_headline` over `lead || ' ' || body` with bounded word counts and one fragment; when the query is empty the reader orders by `updated_at DESC` and returns no snippet or score. Pagination bounds (limit 1..=100, offset >= 0) are validated in the typed `SearchQuery` constructor before any SQL executes.

### D6: The execution entry point passes Document IR through

`ArticlePipeline::execute` takes the `Document` it prepares context from, so `persist()` can derive the projection from the exact revision analyzed. All current call sites (tests, smoke example) are updated in the same commit; the repository keeps no compatibility shims while in development. Alternative considered: extending `PreparedContext` with extracted fields — rejected because context preparation is a versioned prompt concern and mixing index extraction into it couples two independently versioned behaviors.

### D7: The admin router receives the database handle

`admin_router` (and `serve_admin`) gain the `Database` handle; `main.rs` passes what it already built. `/internal/search` parses `tenant` (required), `q` (optional), `limit` (optional, default finite value), `offset` (optional), maps typed validation failures to 400 JSON responses, and serializes the library page. Responses inherit the admin plane's no-store policy. No new dependency enters the workspace.

## Risks / Trade-offs

- [Fixed English stemming mismatches non-English sources] → Documented limitation; snippets still surface raw text; language-aware configuration is explicitly deferred to the hybrid-ranking change rather than half-done here.
- [Projection work grows the persist transaction] → Single conditional upsert against a uniquely constrained row; contention is negligible at current scale and the atomicity guarantee outweighs micro-latency.
- [Tenant scoping accidentally dropped in a future query variant] → Isolation is pinned by a dedicated failing-first test and the endpoint refuses to run without a tenant parameter; the library type requires tenant at construction.
- [UUIDv7 ordering assumed for latest-wins] → Ids come from one generator (`Uuid::now_v7`) in this codebase; the guard also tolerates replay of the identical output id, so re-delivery cannot regress the row.
- [Snippet exposes more text than intended] → Headline is bounded by word count and fragment count, and only projected lead/body text is ever headlined, never raw blobs.

## Migration Plan

None beyond editing `schema.sql` in place, as required while the development status holds: disposable databases are created from the definition, so tests pick the table up automatically. Rollback is reverting the branch; no data survives that must be preserved.

## Open Questions

None.
