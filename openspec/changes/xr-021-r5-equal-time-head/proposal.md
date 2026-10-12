## Why

Round 5 of changeset XR-021 (CONTRACTS.md R5-08 rule 2) verified that `SourceInbox::accept` moves `source_analysis_heads` only when `excluded.observed_at > head.observed_at`. A survivor re-announcement after a privacy deletion names the retained archive and carries that archive's original `imported_at`, the same instant as the stale head, so Knowledge records the corrected conversation as historical and keeps the text of the deleted export.

## What Changes

- The head moves when `observed_at` is later, or when it is equal and the inbox event id is greater (UUID comparison; event ids are UUIDv7, so time ordered).
- `accept` keeps its admission vocabulary: `AcceptedCurrent` for such a delivery, `AcceptedHistorical` for a strictly older one or an equal one with a smaller event id, `Duplicate` for a repeated event.
- The head rule is described in `DEVELOPMENT.md` and `docs/`.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: the head order.

## Impact

`crates/knowledge/src/source_inbox.rs`, the tests and the documentation.
