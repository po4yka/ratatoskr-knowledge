## Context

XR-021 CONTRACTS.md R3-01, R3-08, R3-09, R3-11 and R3-19 are the contract. This file records only the choices local to Knowledge.

## Decisions

### Inbox prefix

`bus::connect` is the one function both workers use, so `custom_inbox_prefix` is applied there on both branches (with and without a credential). The literal lives beside the durable names in `bus.rs`; `deploy/nats/identity.conf` carries the matching `subscribe` entry.

### Tombstone outcomes

The handler reads the event type from the decoded envelope before `accept`. For `ai_archive.subject.tombstoned.v1` every transient failure (an `accept` storage error, a `collect_pending_blobs` error) is `Outcome::Redeliver`, which has no delivery budget; `TombstoneDuplicate` runs `collect_pending_blobs` exactly as `Tombstone` does, so a redelivery finishes an erasure the first delivery did not. `SourceInboxError::InvalidArchiveFact` on a tombstone is `Reject("foreign_subject")`, a decode error is `Reject("undecodable")` and an unknown type is `Reject("unsupported")`. The other archive facts keep `Retry` for storage errors and reject a contradictory fact at once.

### Recap readiness

`consume_once` verifies the durable and sets recap readiness before the source is probed. The source is a separate, non-readiness dependency: a probe failure sets the gauge to 0, is logged on the transition only, and the worker waits (1 s doubling to 30 s) without creating the pull stream, so no command is prefetched while the source is down. The gauge lives in the operator `Metrics` next to the redelivery counter.

### Ack waits

The two required values are named constants in `bus.rs` and the configuration crate validates against them, so the configuration, the durable verification and the documentation have one definition of 120 s and 30 s.

### Search document of a conversation

`SearchFields` for an archive conversation is built from the title and the text and markdown parts of its messages; `archive_context` stays the prompt context only.

## Risks

A malformed archive fact that used to dead-letter after five deliveries is now terminated at the first, which loses the `ingest_failures` row for it; the log line carries the closed class. A service built before the inbox prefix cannot complete JetStream requests under the new ACL, so the services of one snapshot deploy together.
