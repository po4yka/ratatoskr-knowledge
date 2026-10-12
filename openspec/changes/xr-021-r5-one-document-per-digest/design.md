## Context

XR-021 CONTRACTS.md R5-02 rule 4 is the contract; the extractor derives a stable id (rules 1 to 3) in its own repository. This change makes Knowledge independently safe.

## Decisions

### The digest is the identity for the extractor owner context

For `owner_context = 'ratatoskr-extractor'` the document body is content addressed, so `(tenant, digest)` identifies the document. The identity key keeps covering every other owner context because Knowledge's own snapshots reuse a digest across archives on purpose (two archives that carry the same conversation stay two rows).

### Resolve first, then insert

`DocumentIntake::accept` asks the database for the row by `(tenant, owner, digest)` before it registers. The unique index is the guard against a race between two deliveries: `insert_source` uses `on conflict do nothing` followed by a select of the existing row, which resolves a conflict on either constraint. The search document stays keyed by the first `source_ref_id`, so `search_documents.document_id` stays the first id.
