## 1. Plumbing (R5-11 rules 1 and 2)

- [x] 1.1 RED: add `crates/knowledge/tests/event_outbox.rs` cases: the subject `knowledge.deletion.acknowledged.v1` is accepted by `OutboxSubject::parse`, the schema CHECK and the publisher's envelope builder (event type, producer `ratatoskr-knowledge`, `aggregate_id` the subject, `causation_id` `event:<acknowledged id>`, `tenant_id` the owner, payload validates); `EventOutbox::enqueue_rearm_in` inserts a new row, re-arms a row published more than one minute ago (`Rearmed`) and changes nothing for a younger or an unpublished one (`Unchanged`). Add the variant and the function as stubs and confirm the failures are on assertions.
- [x] 1.2 GREEN: `OutboxSubject::DeletionAcknowledged`, the schema CHECK, `enqueue_rearm_in`, the envelope builder, the publisher's allowed subject and the ACL stanza in `deploy/nats/identity.conf`.

## 2. Handlers (R5-11 rule 2)

- [x] 2.1 RED: add `services/knowledge/tests/ingest_bus/tombstone.rs` cases (real broker, PostgreSQL, deployed ACL): an applied tombstone and an applied social removal produce exactly one acknowledgement after the blobs are erased, none while the erasure fails; a redelivery after the acknowledgement was published produces a second publication after the re-arm window and none inside it; a foreign-subject tombstone is acknowledged `refused` and Termed; an undecodable message, a wrong producer and a missing tenant are Termed with no acknowledgement; a tombstone for a subject never held is `applied`. Run them with the acknowledgement enqueue disabled and confirm seven of the eight time out waiting for the acknowledgement (the eighth, the unattributable message, already holds).
- [x] 2.2 GREEN: `SourceInboxError::ForeignSubject`, the acknowledgement enqueue after `collect_pending_blobs` in the `ai_archive` tombstone branch (admission `Tombstone` and `TombstoneDuplicate`) and in `social_removed` (including `Duplicate`), with a failure to enqueue mapped to `Outcome::Retry`.

## 3. Documentation

- [x] 3.1 Describe the acknowledgement, the re-arm rule and the one-minute window in `README.md` and `docs/`. Documentation cannot start from a failing test.
