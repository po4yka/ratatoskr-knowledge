## Context

The cross-repository decisions are fixed in XR-021 CONTRACTS.md (S01 to S04, S08, S09, S12, S13); this design records only the choices internal to Knowledge.

## Decisions

### New code goes in new files to respect the 850-line cap

`deletion.rs` already holds 803 lines, so queue handling lives in `deletion_blobs.rs` and inbox state deletion in `deletion_state.rs`. The recap pipeline recovery stays in `channel_digest_recap_pipeline.rs` if it fits, otherwise in a sibling `channel_digest_recap_recovery.rs`. The ingest worker lives in `services/knowledge/src/ingest.rs` with the shared bus connect and verify helpers extracted from `channel_recap.rs` into `bus.rs` so both workers use one implementation.

### Recovery is driven by durable state, not by in-memory flags

`ChannelRecapPipeline::execute` takes a session advisory lock on a dedicated connection for the whole call and then resumes from the persisted state exactly as the S08 recovery table states. A lock held elsewhere is a `State` error, which the worker already maps to a Nak. Provider calls remain bounded at two attempts per run because the starting ordinal derives from `attempt_count`.

### Model output is a strict subset of the stored recap

`ChannelRecapModelOutput` carries only fields a model can know. `compose` adds contract version, prompt version, context version, language, manifest digest and coverage, then the existing `validate_channel_digest_recap` validates the composed value, so the stored result and its digest are unchanged.

### The outbox row is the durable completion fact

`EventOutbox::enqueue_in` runs inside the caller's transaction and is idempotent on `dedupe_key`. The publisher claims unpublished rows, publishes with `Nats-Msg-Id` equal to the outbox id and sets `published_at` only after the PubAck, so a crash between publish and mark re-publishes the same message id and the broker deduplicates.

### The outbox has no per-row failure state, because a stored row cannot be a poison row

A publish batch stops at its first failing row, so a row that failed for ever would hold back every later fact. The outbox keeps no attempt count or backoff because such a row cannot be written: `enqueue_in` is the only writer, it takes typed refs and a serialised contract fact, rejects a payload that is not an object, and the table checks the subject against the four known values, so `decode_row` and the envelope construction in `ingest_publish.rs` see only values that round-trip. The bus tests publish a fact of every subject and read it back as an envelope. The failures a batch can meet are the broker and the database, which are transient and affect every row alike, so ordering is kept and the whole batch is retried. If a fifth subject or a hand-written row ever makes this false, per-row bookkeeping with backoff is the answer, not skipping rows.

### Blob collection is queue driven

Deleting rows enqueues the Knowledge-owned digests of the scope in the same transaction. A drain removes a digest only when no BlobRef-bearing column still references it, so content-addressed dedupe between tenants stays safe, and a blob stored before its row is registered is never visible to deletion at all because nothing enumerates the blob root.
