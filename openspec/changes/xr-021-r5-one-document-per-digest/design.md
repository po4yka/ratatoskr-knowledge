## Context

XR-021 CONTRACTS.md R5-02 rule 4 is the contract; the extractor derives a stable id (rules 1 to 3) in its own repository. This change makes Knowledge independently safe.

## Decisions

### The digest is the identity for the extractor owner context

For `owner_context = 'ratatoskr-extractor'` the document body is content addressed, so `(tenant, digest)` identifies the document. The identity key keeps covering every other owner context because Knowledge's own snapshots reuse a digest across archives on purpose (two archives that carry the same conversation stay two rows).

### Insert, then resolve to the existing row

`Database::register_source` inserts with `on conflict do nothing` and, when no row came back, selects the existing row: by `(tenant, owner, digest)` for the extractor owner context, by the full identity key otherwise. The resolution lives in the one registration function, so every caller is covered, and the unique index is also what makes two concurrent deliveries of the same document converge on one row. `DocumentIntake::accept` then reads the stored `source_document_id` and analyses the document under that id, so the analysis context and the search document keep the first document id even when the first analysis had not finished and the second event carries another id. The run identity is keyed by the source row, so a second event creates no second run.
