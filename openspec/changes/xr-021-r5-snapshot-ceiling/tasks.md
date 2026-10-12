## 0. OpenSpec change and pin

- [x] 0.1 Write this change and run `openspec validate --all --strict`. Documentation cannot start from a failing test.
- [x] 0.2 Move every `ratatoskr-*` git dependency to the round 5 contracts commit and refresh `Cargo.lock`. A dependency pin cannot start from a failing test; the full gate runs on the result.

## 1. Configuration (R5-03 rule 2)

- [x] 1.1 RED: add `crates/knowledge/tests/config.rs::blob_bytes_below_the_snapshot_floor_or_the_response_cap_is_refused` (8388607 refused with a message naming both keys, 8388608 accepted, a `blob_bytes` below `raw_response_bytes` refused, the default validates); run it against a validation stub and confirm 8388607 is accepted.
- [x] 1.2 GREEN: add `MIN_SNAPSHOT_BLOB_BYTES` and the two rules beside the other `Limits` validation, and document the two keys in `README.md`.

## 2. The store takes blob_bytes only (R5-03 rule 1)

- [x] 2.1 RED: add `crates/knowledge/tests/family_pipeline.rs` cases (real PostgreSQL, scripted provider) for a 2 MiB and a 7.9 million byte conversation snapshot stored, analysed and searchable with the default `blob_bytes` and a 1 MiB `raw_response_bytes`, and a snapshot above `blob_bytes` rejected with a size class and no partial row, and the widest capped search text fitting a `tsvector`; add `services/knowledge/tests/ingest_bus/` (real broker, deployed ACL) for a 2 MiB conversation fact published with the ChatGPT identity reaching `search_documents`; add a `services/knowledge` unit test that the store built from a default configuration accepts 2 MiB. Run them against the min-capped store and confirm the size class assertion fails.
- [x] 2.2 GREEN: build the store with `limits.blob_bytes` only through one helper (`BlobStore::for_config`), cap the indexed search text at `SEARCH_TEXT_MAX_BYTES` (the 7.9 million byte conversation then failed with `string is too long for tsvector`), and add the test that a provider response above `raw_response_bytes` is cut by the adapter before it is stored.
