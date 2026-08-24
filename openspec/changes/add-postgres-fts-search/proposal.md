# Proposal: add-postgres-fts-search

## Why

Knowledge persists accepted article analyses, but nothing can retrieve them. Search over captured sources is a core responsibility of this bounded context, the repository mission names PostgreSQL full-text search as part of the default architecture, and the first slice left it absent. This change adds the smallest complete retrieval slice: a tenant-scoped `knowledge.search_documents` projection maintained by the analysis pipeline and one ranked query path behind the admin plane.

## What Changes

- Add a `knowledge.search_documents` table to the single editable `schema.sql`: one row per source revision identity, weighted generated tsvector column, GIN and recency indexes, unique per source reference.
- Write the projection inside the same transaction that commits an accepted analysis output, so a failed run never touches search documents and an accepted output always lands with its document row.
- Derive title, lead, and body text deterministically from the canonical Document IR supplied to the run; upsert latest-wins guarded by the newest accepted output id.
- Add a `search` module in `crates/knowledge` with a bounded query API: `websearch_to_tsquery` matching, `ts_rank_cd` ranking with stable tie-breaks, bounded `ts_headline` snippets, explicit pagination bounds, and an explicit empty-query browse order.
- Expose `GET /internal/search` on the existing loopback admin listener, scoped by mandatory tenant parameter, returning safe identifiers, titles, snippets, and match explanation.
- Update README data-ownership and boundaries wording to include search documents.
- No embeddings, pgvector columns, hybrid ranking, saved searches, or public API surface are introduced; those remain planned changes.

## Capabilities

### New Capabilities

- `search-documents`: the tenant-scoped search projection derived from accepted analyses and its ranked, authorized retrieval behavior, including pagination bounds and empty-query semantics.

### Modified Capabilities

None. Existing requirements stay true: the schema remains one editable definition owned by Knowledge, and the admin listener keeps its current guarantees while gaining one more route.

## Impact

- `schema.sql` gains the `search_documents` table and indexes; disposable test databases pick it up automatically because they are created from the definition.
- `crates/knowledge` gains a `search` module (projection writer plus page queries); the article pipeline's persistence step extends within its existing transaction and its execution entry point takes the Document IR it already prepares context from, updating every call site directly.
- `services/knowledge` threads the database handle into the admin router and serves `/internal/search`; invalid or missing parameters return 400 responses on the same no-store contract as the rest of the admin plane.
- Tests extend `schema`, projection, query, and admin suites per the task list.
- No other repository consumes these rows or this endpoint yet, so no workspace changeset is triggered by this change alone.
