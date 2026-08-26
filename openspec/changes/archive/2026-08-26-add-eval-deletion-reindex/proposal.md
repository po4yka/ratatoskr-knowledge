# Add an evaluation harness, privacy deletion, and explicit reindex jobs

## Why

Plan items 5 through 7 delivered real inference, lexical projection, and versioned pgvector embeddings, but the stack still has no trustworthy way to measure analysis quality, no way to erase a tenant's or a document's derived data, and no wired operator command to regenerate projections after a model or policy change. The legacy monolith answered deletion with purge jobs and reconciliation; this change must exceed it with complete, auditable, verifiable erasure rather than match it.

## What Changes

- Add a committed evaluation fixture set (Document IR inputs plus expected analysis qualities) and a deterministic scoring engine that validates contract compliance, grounding, bounds, and cardinality of any analysis response against its case - never exact wording equality.
- Add an offline eval runner that scores recorded response sets grouped by provider and prompt-version labels and writes a byte-deterministic report artifact; the default gate makes no live API call and spends no credit.
- Add privacy deletion paths for a whole tenant and for one logical source (every revision): analyses, attempts, accepted and rejected outputs, projection-input snapshots, search documents, embedding chunks, failure records, and every referenced raw-response blob removed atomically in one transaction with a persisted deletion audit row and post-commit verified blob collection.
- Extend the blob store with a reference-checked delete operation so content-addressed deduplication cannot have one tenant's erasure destroy another tenant's evidence.
- Wire the previously documented but unwired `reindex-embeddings` service subcommand (library engine and tests exist since item 7; the dispatch beside `check-config` does not) with tenant and source scoping, per-source progress output, and defined exit codes.
- Add an immutable calculated projection-input snapshot at persistence time and an explicit idempotent `reindex-search-documents` job that rebuilds the lexical projection from it without source refetch, with the same scoping, progress, and resumability contract.
- `schema.sql` gains a `knowledge.deletion_records` audit table, edited in place per the binding development status.

## Capabilities

### New Capabilities

- `evaluation-harness`: committed fixture cases with expected qualities, a deterministic pure scoring engine, labeled response-set comparison reports, and an offline-by-default runner excluded from live-API use.
- `privacy-deletion`: delete-by-tenant and delete-by-source semantics covering every derived row and blob reference, atomic execution with an audit record, reference-safe blob garbage collection, and verifiability.
- `reindex-jobs`: explicit operator subcommands that rebuild search documents and embeddings under the active configured identity, scoped by tenant or source, idempotent and resumable, with bounded concurrency and reported progress.

### Modified Capabilities

## Impact

- `schema.sql`: new `knowledge.deletion_records` and `knowledge.search_projection_inputs` tables; edited in place, no migrations, first version preserved.
- `crates/knowledge`: `blob_store.rs` gains a digest-based removal operation; new `deletion.rs` and `evaluation.rs` modules; `reindex.rs` planning gains scope filters; new committed fixtures under `crates/knowledge/fixtures/eval/`; a new `eval_harness` example; expanded integration tests.
- `services/knowledge/src/main.rs`: three new subcommands beside `check-config` (`delete-tenant`, `delete-source`, `reindex-search-documents`) plus the missing `reindex-embeddings` dispatch; manual argument parsing style preserved, no new dependencies.
- Documentation: `README.md`, `DEVELOPMENT.md`, `docs/TESTING.md` coverage paragraph, and ticking plan item 8 in `docs/IMPLEMENTATION_PLAN.md`.
- Deletion honors the workspace store spec `blob-references`: knowledge removes only bytes it owns under its own content-addressed root; `source_refs.source_blob` references owned by other services are never touched. Progress reporting stays on the operator CLI; the store's `operation-progress` event contract is out of scope until a message-bus consumer exists (item 9).
- The gate command list in `DEVELOPMENT.md` and `.github/workflows/ci.yml` does not change; examples are linted and compiled by the existing steps but never executed by the gate.
