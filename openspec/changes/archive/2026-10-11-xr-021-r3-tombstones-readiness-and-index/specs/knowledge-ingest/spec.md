## ADDED Requirements

### Requirement: Knowledge connections use their own inbox prefix

Every NATS connection Knowledge opens SHALL use the inbox prefix `_INBOX_knowledge`, and the Knowledge stanza SHALL allow subscribing to that prefix and to nothing else.

#### Scenario: A JetStream request completes under the stanza

- **WHEN** the ingest and recap workers run against a broker that enforces the Knowledge stanza
- **THEN** both verify their durables and pull their messages, and a client on the default `_INBOX` prefix gets no reply

### Requirement: An archive tombstone is never dead-lettered or half finished

A transient failure while applying an `ai_archive.subject.tombstoned.v1` delivery SHALL be redelivered without a delivery budget, and a redelivery of an already applied tombstone SHALL drain the blob erasure queue before it is acknowledged.

#### Scenario: More than five transient failures

- **WHEN** the database refuses a tombstone for eight consecutive deliveries and then recovers
- **THEN** no delivery is terminated, no failure row is written and the ninth delivery removes the derived rows

#### Scenario: The blob erasure failed after the rows were deleted

- **WHEN** the first delivery committed the row deletion and the erasure queue and the blob removal then failed
- **THEN** the redelivery drains the queue, the blob files are gone and the delivery is acknowledged

### Requirement: An archive fact that can never succeed is rejected at once

A tombstone for a subject that belongs only to another tenant SHALL be terminated at its first delivery with the class `foreign_subject`, an undecodable archive fact with `undecodable` and an unsupported archive type with `unsupported`, without a failure row and without touching the other tenant's rows.

#### Scenario: A foreign tombstone

- **WHEN** a tenant publishes a tombstone for another tenant's conversation
- **THEN** the delivery is terminated at once, the class is logged, and the other tenant's rows are intact
