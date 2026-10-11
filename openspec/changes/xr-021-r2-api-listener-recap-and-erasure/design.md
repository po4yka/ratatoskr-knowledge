## Context

Knowledge has one HTTP listener today, the operator listener, and Platform's `KnowledgeClient` targets the Edge route address `127.0.0.1:8091`. XR-021 CONTRACTS.md R2-01 decides that services bind the allocated ports. The same round fixes recap defaults (R2-13), recap redelivery (R2-05), recap deletion (R2-06) and the digest lock protocol (R2-07), and asks for an operator example (R2-10).

## Decisions

### Two routers, one state

`admin_router` keeps `/live`, `/ready`, `/metrics` and `/version`. A new `api_router` mounts the capability document, search, user-content command and collection routes, and the result-read route only when the reader secret is configured. Both routers carry the same `no_store` layer. `Lifecycle` gains a listeners-bound bit that `main` sets after both sockets bind, and `/ready` requires it, so readiness cannot pass while the API port is not served. The API configuration lives in `config_api.rs` because `config.rs` is near the 850 line cap.

### Recap defaults

The recap provider default becomes `openrouter`; enabling the recap then requires the provider key, as ingest does. `scripted` is accepted only with a loopback `nats://` endpoint and no credentials file, and the process logs the mode at startup.

### Redelivery and progress

`Disposition::Nak` carries a delay computed by a pure `redelivery_delay(delivered)` of `min(2 s * 2^(delivered-1), 60 s)` from the message's delivery count, and a counter records each redelivery. The handler future is never dropped: the worker polls the handler and a progress ticker (`AckKind::Progress` every `ack_wait / 3`) in one `select!` over the pinned handler, so only the ticker side is cancelled.

### Recap deletion

A new `deletion_recap.rs` deletes outbox, results, attempts, manifests, runs and inbox rows of the tenant inside `run_child_first_deletion`'s transaction and queues the attempts' raw-response digests. `deletion.rs` gains only a call site. `DeletionCounts.channel_recap_rows` and `deletion_records.channel_recap_rows` carry the count and `verify_scope_empty` checks it. Source and archive scope do not call it.

### Digest lock protocol

`blob_lock(tx, digest_hex)` takes `pg_advisory_xact_lock(hashtextextended('blob:' || digest, 0))`. `collect_pending_blobs` holds it from the re-check through the erase and the queue-row delete in one transaction. Every path that registers a Knowledge-owned blob reference takes it before the insert and verifies the blob is present, re-storing the bytes it still holds. Acquisition order is tenant lock, then digest locks in ascending order.

## Risks

Moving four routes between listeners is a deliberate break for any caller that still targets the operator address; Platform already targets 8091 and the workspace profile is changed by the workspace work order.
