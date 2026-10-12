## Why

Round 5 of changeset XR-021 (CONTRACTS.md R5-02 rule 4) verified that a document captured twice (plain, then forwarded) reaches Knowledge as two `content.document.extracted.v1` events with equal content digest and different `document_id`. `DocumentIntake::accept` registers a second `source_refs` row under the identity key `(tenant, owner_context, ai_archive_id, source_document_id, digest)`, and library search returns two documents. Knowledge must be safe on its own, whatever the extractor sends.

## What Changes

- `DocumentIntake::accept` resolves an existing `source_refs` row by `(tenant, 'ratatoskr-extractor', digest)` first and reuses it: no new row, no new run, outcome `AlreadyAnalysed`.
- `schema.sql` gains the partial unique index `source_refs_document_digest_key on knowledge.source_refs (tenant_ref, content_digest_hex) where owner_context = 'ratatoskr-extractor'`; `insert_source` resolves a conflict on either constraint to the existing row.
- Other owner contexts (Knowledge's own snapshots, which carry an archive id) are untouched.
- Deliberate break: a development database is recreated; two extractor documents with equal digest under one tenant are one document.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: one document per tenant and digest.

## Impact

`crates/knowledge/src/document_intake.rs`, `runs.rs`, `schema.sql`, the tests and `docs/DATA_MODEL.md`. No contract version, no migration file and no shim.
