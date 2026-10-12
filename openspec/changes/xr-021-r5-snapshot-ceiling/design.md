## Context

XR-021 CONTRACTS.md R5-03 is the contract. This file records the choices local to Knowledge.

## Decisions

### One ceiling, one helper

The store exists to hold three kinds of bytes: provider responses, Knowledge's own source snapshots and recap raw responses. Only the snapshots can be large, and their size is bounded upstream by the broker payload. The ceiling is therefore `limits.blob_bytes`, and the provider response is bounded by the adapter's `response_byte_cap` before it reaches the store, so no provider response can be stored above `raw_response_bytes`. `blob_store_for(&Config)` builds the store; `open_storage` and `connect_job_stack` call it, so the two cannot drift again.

### Validation lives beside the other limit rules

`validate_limits` in `config.rs` refuses `blob_bytes < MIN_SNAPSHOT_BLOB_BYTES` and `blob_bytes < raw_response_bytes` with one rule text that names both keys. The check runs after all entries are applied, so the order of the environment entries does not matter.

### Out-of-store bound for provider responses

`run_driver.rs` stores the raw provider response with `store_raw`. A test feeds a response above `raw_response_bytes` to the adapter and asserts it is cut before it is stored, so widening the store does not widen the provider response.

### The search projection is bounded too

Verifying the 7.9 million byte scenario found a second barrier behind the blob ceiling: the projection of a conversation carries its whole text, and PostgreSQL refuses a `tsvector` of 1 MiB or more (`string is too long for tsvector`), which failed the analysis after the snapshot was stored. `record_search_document` and `record_search_projection_input` therefore index at most `SEARCH_TEXT_MAX_BYTES = 300_000` bytes of title, lead and body together, cut deterministically at a character boundary (title first, then lead, then body). The constant is measured against the widest vector English text can make (every two-, three- and four-letter word, about 2.3 times its text), which stays near 750 KB. The analysis reads its own bounded context and is not affected; the text beyond the cut is not lexically searchable.
