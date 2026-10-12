## Context

XR-021 CONTRACTS.md R5-11 is the contract (the payload, the envelope, the durables and the producer side). This file records the choices local to Knowledge.

## Decisions

### Acknowledge after the blobs are gone

The acknowledgement is enqueued only after `collect_pending_blobs` succeeded in the same handler pass; while the blob erasure fails the delivery is redelivered and nothing is acknowledged, so an acknowledgement always means the content is gone.

### The outbox row is the acknowledgement, and a duplicate re-arms it

`dedupe_key = deletion_ack:<acknowledged_event_id>`. A first delivery inserts the row. A redelivery of an already acknowledged fact means the producer did not see the acknowledgement (the stream dropped it), so `enqueue_rearm_in` sets `published_at = null` when the row was published more than one minute ago; inside the minute it changes nothing, so a producer burst cannot make a publication storm. The publisher already selects every unpublished row.

### A foreign subject is refused, not hidden

A tombstone whose subject belongs only to another tenant surfaced as `InvalidArchiveFact`, the variant also used for other invalid facts. `SourceInboxError::ForeignSubject` is dedicated, mapped to a `refused` acknowledgement with refusal `foreign_subject` and `Outcome::Reject("foreign_subject")`. The data stays and an operator looks.

### Failure to enqueue is a retry

If the enqueue fails the handler returns `Outcome::Retry`; a silent success would lose the acknowledgement.
