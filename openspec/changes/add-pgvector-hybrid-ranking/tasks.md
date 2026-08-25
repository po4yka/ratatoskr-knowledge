# Tasks

## 1. Environment and schema foundation

- [ ] 1.1 Switch the CI PostgreSQL service in `.github/workflows/ci.yml` to a digest-pinned pgvector-enabled PostgreSQL 17 image with unchanged credentials, ICU initdb arguments, ports, and health check. Cannot start from a failing test: infrastructure configuration only.
- [x] 1.2 Add a failing test `pgvector_embedding_schema_objects` to `crates/knowledge/tests/schema.rs` asserting that after `apply_schema` the vector extension, `knowledge.embedding_chunks`, `knowledge.embedding_failures`, and the cosine similarity index exist, and that applying the schema a second time succeeds unchanged; verify it fails with `cargo test -p ratatoskr-knowledge --test schema pgvector_embedding_schema_objects` because `schema.sql` defines none of them.
- [x] 1.3 Extend `schema.sql` in place with `create extension if not exists vector`, the two tables from design D4 including the identity unique key and column checks, and the HNSW cosine index; verify `pgvector_embedding_schema_objects` passes and `cargo test -p ratatoskr-knowledge --test schema` stays fully green.

## 2. Versioned chunking policy

- [ ] 2.1 Add a failing test `chunking_same_input_yields_identical_sequence` in the unit tests of a new `crates/knowledge/src/chunking.rs`: applying the policy twice to the same title, lead, body yields equal chunk counts, texts, ordinals, and digests; text below the target yields exactly one chunk whose text begins with the title. Verify it fails with `cargo test -p ratatoskr-knowledge --lib chunking` because the module does not exist.
- [ ] 2.2 Implement policy `article-chunks.v1` per design D3 (newline normalization, blank-line paragraph split, greedy packing, char-boundary-safe hard split, overlap carry, title prefix, SHA-256 chunk digest, compiled version constant); make 2.1 pass, then run format and Clippy.
- [ ] 2.3 Add a failing test `chunking_respects_target_and_overlap_bounds` asserting every chunk stays within the configured target plus overlap allowance, consecutive windows share exactly the carried overlap text, and a many-paragraph document splits at configured boundaries when the target shrinks; verify it fails against the current policy constants.
- [ ] 2.4 Make 2.3 pass by completing the windowing and overlap mechanics; keep the policy pure and clock-free; run format and Clippy.

## 3. Embeddings provider seam

- [ ] 3.1 Add a failing test `scripted_embedding_provider_is_deterministic` in `crates/knowledge/src/provider_tests.rs` (or the existing provider test location): the scripted provider returns the declared dimensions and one vector per input, identical values for identical inputs across instances, captures requests, and replays scripted failures; verify it fails because no such type exists.
- [ ] 3.2 Introduce `EmbeddingIdentity`, the narrow `EmbeddingProvider` trait, and `ScriptedEmbeddingProvider` beside the chat seam per design D2; make 3.1 pass; run format and Clippy.
- [ ] 3.3 Add a failing test `openai_compatible_embeddings_maps_wire` using the fake loopback transport: request path, authorization header, model field, ordered inputs, and byte-capped response are as designed; a successful envelope yields one vector per input with usage extracted, an HTTP 429 maps to the rate-limited failure class, and an oversized reply maps to the size-limit class; verify it fails because the adapter does not exist.
- [ ] 3.4 Implement `OpenAiCompatibleEmbeddings` with per-try deadline, connect timeout, response byte cap, transient-only jittered retry, and error classification mirroring the chat adapter; make 3.3 pass; run format and Clippy.
- [ ] 3.5 Add a failing test `controlled_embeddings_refuses_exhausted_budget`: with the durable ledger driven past its ceiling, calling through the controls wrapper sends no transport request, returns the budget-exhausted failure class, and records nothing as usage; verify it fails because the wrapper does not exist.
- [ ] 3.6 Implement `ControlledEmbeddings<P>` composing limiter admission, pre-call budget projection, bounded tracing, and usage recording over any `EmbeddingProvider`; make 3.5 pass; run format and Clippy.

## 4. Persistence, state transition, and background indexing

- [ ] 4.0 Declare the `pgvector` crate dependency with its sqlx integration in `Cargo.toml` and confirm `cargo deny --locked check` accepts the licenses. Cannot start from a failing test: dependency declaration only.
- [ ] 4.1 Add a failing database test `embedding_rows_carry_full_identity` in `crates/knowledge/tests/`: storing vectors for a seeded source and accepted output persists every identity and provenance field readable back exactly (tenant, owner context, document, output, ordinal, digest, chunking version, provider, model, dimensions, prompt version), upserting the same identity replaces values in place, stale higher ordinals are pruned, and a wrong-dimension vector fails validation leaving no partial row; verify it fails because no persistence function exists.
- [ ] 4.2 Implement the transactional per-source persistence function per design D4 (identity-keyed upsert, ordinal prune, failure-row delete, guarded single-row `persisted -> indexed` transition requiring exactly one affected row); make 4.1 pass; run format and Clippy.
- [ ] 4.3 Add a failing database test `indexing_pass_transitions_persisted_runs_once`: seeding two sources in `persisted` with projections, one worker pass with the scripted provider moves both to `indexed` with vectors stored once, a second pass performs no provider calls and changes nothing, and a source without a projection is skipped and counted; verify it fails because the worker pass does not exist.
- [ ] 4.4 Implement the worker selection-and-process pass per design D5 (oldest-first batch over durable state, chunking under configured bounds, budget admission, embed, persist via 4.2, quiet detection); make 4.3 pass; run format and Clippy.
- [ ] 4.5 Add a failing database test `indexing_failure_is_explicit_and_bounded`: a permanently failing scripted provider keeps the analysis output intact and the run in `persisted`, upserts one failure row per identity with incremented attempts and the mapped error class, stops calling after the configured attempt bound, and a successful later pass clears the failure row; verify it fails because failure recording does not exist.
- [ ] 4.6 Implement bounded failure recording and the attempt bound per design D4; make 4.5 pass; run format and Clippy.
- [ ] 4.7 Change the pipeline persist path to leave accepted runs in `persisted` instead of transitioning straight to `completed`, so the indexer owns the only `persisted -> indexed` transition per design D5, and update existing pipeline/runs test expectations accordingly; add a failing assertion first that an accepted run rests at `persisted` immediately after acceptance. `completed` remains a legal state for future use.
- [ ] 4.8 Wire the poll loop into the service lifecycle per design D5 (immediate first pass, interval sleep, drain-aware stop inside the shutdown bound) in `services/knowledge`. Cannot start from a failing test: composition-root wiring verified by the gate's build, config, and database suites.

## 5. Hybrid ranking

- [ ] 5.1 Add a failing database test `hybrid_ranking_orders_by_reciprocal_rank_fusion`: fixtures where the lexical leg and the semantic leg rank shared documents differently plus documents exclusive to each leg produce the exact fused order computed independently in the test at k=60, with ties broken by recency then document identity, correct snippets on every row, and stable identical pages across repeated queries with pagination; verify it fails because the hybrid path does not exist.
- [ ] 5.2 Implement the hybrid retrieval branch per design D6 (single statement, two bounded tenant-scoped candidate legs bound to the active identity tuple, documented depth rule, RRF fusion, deterministic tiebreakers, limit/offset last, headline snippets) with the query embedded through the controlled seam; make 5.1 pass; run format and Clippy.
- [ ] 5.3 Add a failing database test `hybrid_legs_are_equally_tenant_scoped`: matching text and vectors belonging to another tenant never contribute candidates or results to this tenant's hybrid page even when they would rank first on either leg alone; verify it fails before the tenant filter exists on the semantic leg.
- [ ] 5.4 Make 5.3 pass; run format and Clippy.
- [ ] 5.5 Add a failing test `search_degrades_to_lexical_without_provider` proving that with no embeddings configured the retrieval path serves recent-browse and lexical pages identically to today's behavior and surfaces no provider-dependent failure, and that a query-embedding failure at runtime falls back to lexical results rather than erroring; verify it fails because the degradation path is not implemented.
- [ ] 5.6 Implement the fallback selection per design D6; make 5.5 pass; run format and Clippy.

## 6. Configuration and operator surface

- [ ] 6.1 Add a failing unit test `embeddings_configuration_parses_strictly` to the config tests: every new key from design D2 parses with its finite default, the credentials redact themselves, unknown embedding keys abort loading, missing model with a credential aborts, overlap >= target aborts, and dimensions unequal to the storage dimensionality abort startup; verify it fails because the keys are unrecognized.
- [ ] 6.2 Implement the strict loader extensions; make 6.1 pass; run format and Clippy.
- [ ] 6.3 Extend `/internal/search` wiring so the handler uses the hybrid-or-fallback retrieval selection, and extend `/metrics` with bounded counters for indexing passes, indexed sources, failure classes, and served ranking paths. Cannot start from a failing test: handler composition verified through the gate suites and existing endpoint tests.

## 7. Explicit reindex command

- [ ] 7.1 Add a failing database test `reindex_converges_idempotently_and_leaves_history`: seeding vectors under a superseded identity plus sources lacking coverage, running the reindex with the newly active configuration regenerates every affected source under the active identity, prunes superseded rows, clears failure entries, performs zero provider calls on an immediate second run, and leaves `analysis_outputs` bytes untouched throughout; a worker-only startup with the changed identity mutates nothing; verify it fails because no reindex exists.
- [ ] 7.2 Implement the reindex planning and execution functions and the `reindex-embeddings` subcommand beside `check-config` per design D7, printing per-source and total counts and exiting nonzero with completed work persisted on failure; make 7.1 pass; run format and Clippy.

## 8. Documentation and evidence

- [ ] 8.1 Update `README.md` status and settings table, `DEVELOPMENT.md` prerequisites with the pgvector-capable PostgreSQL 17 requirement and the local container recipe, and tick plan item 7 in `docs/IMPLEMENTATION_PLAN.md`. Cannot start from a failing test: documentation only.
- [ ] 8.2 Measure resource usage of the release build while indexing a representative local fixture set (resident memory peak and wall-clock per source) and record the numbers with their measurement command in the change summary, confirming they fit the deployment-target ceilings. Cannot start from a failing test: recorded measurement evidence, not an automatable assertion.
- [ ] 8.3 Run the full gate exactly as listed in `DEVELOPMENT.md` including the file-size ratchet, plus `openspec validate --all --strict`; tick every task above only after its verification ran, and fix anything the gate rejects.
