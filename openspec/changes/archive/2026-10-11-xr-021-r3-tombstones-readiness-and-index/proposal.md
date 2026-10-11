## Why

Round 3 of changeset XR-021 re-verified the Round 2 smoke defects against `origin/main`. Knowledge's NATS connections share the default `_INBOX` prefix, so a compromised service could read Knowledge's deliveries; an AI-archive deletion tombstone is dead-lettered after five transient failures, a redelivered one skips the byte erasure, and a delivery that can never succeed burns five deliveries; `/ready` is 503 for as long as the channel-digests API is unreachable although ingest and search do not need it; the ack-wait keys accept values Edge's durables refuse; a conversation is indexed as its prompt scaffold instead of its text; and the pgvector prerequisite and the database role are neither shipped nor documented. The cross-repository behaviour is defined once in XR-021 CONTRACTS.md sections R3-01, R3-08, R3-09, R3-11 and R3-19; this change cites it and does not restate it.

## What Changes

- Every Knowledge NATS connection uses the inbox prefix `_INBOX_knowledge`, and `deploy/nats/identity.conf` allows that prefix only (R3-01).
- A tombstone transient failure is redelivered without a budget, a duplicate tombstone still drains the blob erasure queue, and a foreign, undecodable or unsupported archive fact is rejected at the first delivery (R3-08).
- The reachability of the digest source is the gauge `knowledge_channel_recap_source_up` and no longer gates `/ready`; the recap worker does not pull while the source is down and logs each transition once (R3-09).
- `RATATOSKR__INGEST__ACK_WAIT_SECONDS` must be 120 and `RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS` must be 30, the values Edge fixes (R3-09).
- A conversation is indexed with its title, the plain text of its messages and a lead cut from the first message, with no ids, roles or scaffold (R3-09).
- Ship `deploy/postgres/role.sql`, name the shared `ratatoskr` database in the example and document the superuser prerequisite and the start order; rewrite the stale README paragraph (R3-11, R3-19).
- Deliberate breaks, named in the commits that make them: a service built before the inbox prefix cannot complete JetStream requests under the new ACL; an ingest or recap ack wait other than 120 or 30 no longer loads; a malformed or foreign archive fact is terminated at the first delivery instead of the fifth.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: inbox prefix, tombstone delivery semantics and reject classes.
- `service-foundation`: readiness independent of the digest source, the source gauge, ack-wait validation, role provisioning.
- `search-documents`: conversations are indexed as text.

## Impact

Affected surfaces are configuration, the bus connection function, the archive handler of the ingest worker, the recap worker, the archive search document builder, `deploy/` and the documentation. No new contract version, no migration file and no compatibility shim is introduced. Cross-repository behaviour is defined in XR-021 CONTRACTS.md.
