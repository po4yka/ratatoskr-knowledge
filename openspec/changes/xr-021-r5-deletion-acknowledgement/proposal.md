## Why

Round 5 of changeset XR-021 (CONTRACTS.md R5-11 rules 1 and 2, the owner stream policy) found that the events stream drops its oldest message at 7 days or 1 GiB and that a producer marks a deletion fact published at the broker acknowledgement. Nothing tells a producer that Knowledge applied an `ai_archive.subject.tombstoned.v1` or a `social.source.removed.v1`, so a deletion Knowledge did not consume in time would be lost silently. Knowledge must acknowledge every applied, duplicate or refused deletion fact so the producer can re-send it until the acknowledgement arrives.

## What Changes

- Knowledge publishes `knowledge.deletion.acknowledged.v1` through its transactional outbox for every applied, duplicate or foreign-subject tombstone and removal, after the rows are deleted and `collect_pending_blobs` succeeded.
- `OutboxSubject::DeletionAcknowledged`, the `event_outbox` subject CHECK, `EventOutbox::enqueue_rearm_in` (a duplicate re-arms a row published more than one minute ago), the envelope builder for the typed payload, and the new subject in the publisher's allowed list.
- A dedicated `SourceInboxError::ForeignSubject` replaces the overloaded `InvalidArchiveFact` for a tombstone whose subject belongs only to another tenant; it is acknowledged `refused` and Termed.
- `deploy/nats/identity.conf` gains the publish allow `evt.knowledge.deletion.acknowledged.v1`.
- An undecodable message, a wrong producer and a missing tenant are Termed with no acknowledgement.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: deletion facts are acknowledged.

## Impact

`event_outbox.rs`, `source_inbox.rs`, `services/knowledge/src/ingest_handlers.rs`, `ingest_publish.rs`, `schema.sql`, `deploy/nats/identity.conf`, the tests, `README.md` and `docs/`. The ACL copy must stay byte-equal to Platform's stanza.
