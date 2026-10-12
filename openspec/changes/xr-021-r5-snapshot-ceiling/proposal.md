## Why

Round 5 of changeset XR-021 (CONTRACTS.md R5-03) verified that `open_storage` and `connect_job_stack` build the one `BlobStore` with `min(limits.blob_bytes, limits.raw_response_bytes)`, which is 1 MiB by default. `FamilyPipeline::snapshot_source` stores the whole conversation or project snapshot in that store, and ChatGPT and Claude may publish a fact of up to 8,000,000 bytes (at most the 8 MiB broker payload). A larger conversation fails with a store size error, is classified `unanalysable` and Termed, so the archive is never searchable. The cross-repository rule is defined once in CONTRACTS.md R5-03; this change cites it.

## What Changes

- The blob store is built with `limits.blob_bytes` only, through one helper that `open_storage` and `connect_job_stack` both call. `limits.raw_response_bytes` stays the cap on one provider HTTP response, which the provider adapter already enforces before anything is stored.
- Configuration validation refuses `RATATOSKR__LIMITS__BLOB_BYTES` below `MIN_SNAPSHOT_BLOB_BYTES = 8_388_608` (the broker `max_payload`) and below `RATATOSKR__LIMITS__RAW_RESPONSE_BYTES`; the message names both limits. The default stays 16 MiB.
- The search projection indexes at most 300000 bytes of title, lead and body (found while verifying the 7.9 million byte conversation: PostgreSQL refuses a `tsvector` of 1 MiB or more).
- `README.md` documents `RATATOSKR__LIMITS__BLOB_BYTES` as the snapshot and blob ceiling and `RATATOSKR__LIMITS__RAW_RESPONSE_BYTES` as the provider response cap.
- Deliberate break: a deployment that set `RATATOSKR__LIMITS__BLOB_BYTES` below 8 MiB no longer starts.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: the snapshot ceiling.

## Impact

`crates/knowledge/src/config.rs`, `services/knowledge/src/main.rs`, the tests and `README.md`. No contract version, no migration and no shim.
