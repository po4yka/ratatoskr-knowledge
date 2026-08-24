# Tasks: add-postgres-fts-search

## 1. Projection schema

- [x] 1.1 Add a failing test `search_documents_projection` to `crates/knowledge/tests/schema.rs` asserting the database contains the `knowledge.search_documents` table with its columns, the unique constraint on the source-reference column, and the GIN index on the weighted text vector; verify it fails with `cargo test -p knowledge --test schema search_documents_projection` because `schema.sql` defines no such table.
- [x] 1.2 Define the `knowledge.search_documents` table, generated weighted tsvector column, unique constraint, GIN index, and recency btree index in `schema.sql`; verify the schema test passes and `cargo test -p knowledge --test schema` stays fully green.

## 2. Deterministic text extraction

- [x] 2.1 Create `crates/knowledge/src/search.rs` exporting an extraction function stub that returns an empty title/lead/body triple so the crate compiles; verify with `cargo build --workspace --locked`. Cannot start from a failing test: this is type scaffolding for a brand-new module, its behaviour is covered by the next pair.
- [x] 2.2 Add failing unit tests in `crates/knowledge/src/search.rs`: title comes from the Document title field, falls back to the first Heading block, else is empty; lead is the first Paragraph; body is the remaining paragraphs plus later heading texts. Verify they fail with `cargo test -p knowledge --lib search::` because the stub yields empty strings.
- [x] 2.3 Implement the extraction over `DocumentBlock` iteration so the unit tests pass; verify with `cargo test -p knowledge --lib`.

## 3. Transactional projection lifecycle

- [x] 3.1 Extend `ArticlePipeline::execute` to accept the analyzed `&Document` and update every call site (`crates/knowledge/tests/pipeline.rs`, smoke example) with no behaviour change; verify with `cargo build --workspace --locked && cargo test -p knowledge --test pipeline`. Cannot start from a failing test: signature plumbing only; existing tests must stay green.
- [x] 3.2 Add failing integration tests to `crates/knowledge/tests/pipeline.rs`: an accepted run projects exactly one searchable row carrying the derived fields (P1); a later failed run leaves an already-projected row untouched (P2); a second accepted run replaces the projected fields and references the newest output while an older replay cannot regress it (P3). Verify they fail with `cargo test -p knowledge --test pipeline search_document` because persisting never touches the projection.
- [x] 3.3 Implement the guarded latest-wins upsert executed on the persist transaction after the output insert and state transition; verify P1-P3 pass with `cargo test -p knowledge --test pipeline`.

## 4. Ranked retrieval

- [x] 4.1 Add `SearchQuery`, `SearchResult`, `SearchPage`, a typed error variant, and a `search_page` stub returning that unavailable error in `crates/knowledge/src/search.rs`; wire re-exports and verify `cargo build --workspace --locked`. Cannot start from a failing test: scaffolding so the next pair's tests compile.
- [x] 4.2 Add failing tests in `crates/knowledge/tests/search.rs`: another tenant's document is invisible even when matching (Q1); a title match outranks a body-only match with deterministic ordering (Q2); snippets stay word-bounded with score above zero (Q3); an absent/blank query browses by descending update time without snippet or score (Q5). Verify they fail with `cargo test -p knowledge --test search` because the reader reports unavailability.
- [x] 4.3 Implement the reader SQL: web-syntax tsquery compilation, cover-density ranking with `(updated_at DESC, id)` tie-break, bounded single-fragment headline over lead and body, and the empty-query recency browse, all filtered by tenant; verify Q1/Q2/Q3/Q5 pass with `cargo test -p knowledge --test search`.
- [x] 4.4 Add a failing test asserting page sizes of zero and above the maximum bound and a negative offset are rejected as an explicit invalid-parameter error before any database work (Q4); verify it fails with `cargo test -p knowledge --test search rejects_out_of_bounds_pages` because construction currently accepts anything.
- [x] 4.5 Enforce the bounds in `SearchQuery` construction; verify the whole file passes with `cargo test -p knowledge --test search`.

## 5. Admin HTTP surface

- [x] 5.1 Add a failing test to `services/knowledge/tests/admin.rs`: `GET /internal/search` with a valid tenant and query returns JSON results exposing owner context, document identifier, title, snippet, and rank, and a request without a tenant parameter receives a 400 client error. Verify it fails because the route does not exist yet.
- [x] 5.2 Thread the database handle through `admin_router`/`serve_admin` and their call sites in `services/knowledge/src/`, then implement the handler mapping parameters onto the typed query and invalid parameters to client-error responses; verify with `cargo test -p knowledge-service`.

## 6. Documentation and gate

- [x] 6.1 Update `README.md` wording so tables and boundaries mention the search documents projection and admin-plane retrieval. Cannot start from a failing test: documentation only.
- [x] 6.2 Run the full gate from DEVELOPMENT.md (`cargo fetch --locked`, `cargo deny --locked check`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo build --workspace --locked --release`) and confirm every command exits zero.
