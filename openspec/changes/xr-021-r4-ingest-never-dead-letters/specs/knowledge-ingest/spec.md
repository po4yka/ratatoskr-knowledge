## ADDED Requirements

### Requirement: A retryable ingest failure is redelivered without a budget

A failure of an ingest delivery that a later delivery can fix SHALL be negative-acknowledged with a delay of `min(2 s * 2^(delivered - 1), 60 s)` and SHALL NOT be terminated, however many deliveries it has had, in the documents, social and AI-archive families. The repository family SHALL keep its budget of five deliveries and settle a failed fact on the last.

#### Scenario: A document survives eight failed deliveries

- **WHEN** the database refuses to register an extracted document for eight consecutive deliveries and then recovers
- **THEN** no delivery is terminated, and the document is registered and analysed exactly once

#### Scenario: A social capture, a removal and a conversation survive eight failed deliveries

- **WHEN** the database refuses the inbox receipt of a social capture or an AI conversation, or the tombstone of an X removal, for eight consecutive deliveries and then recovers
- **THEN** no delivery is terminated, the capture or conversation is analysed once, and the removal deletes the source from search

#### Scenario: A provider outage followed by a database outage

- **WHEN** the provider fails transiently for three deliveries and the database then refuses the completion fact for four more
- **THEN** the capture is analysed once and its completion fact is queued once

#### Scenario: The delay grows and stops at one minute

- **WHEN** deliveries one to ten fail
- **THEN** the delays are 2, 4, 8, 16, 32, 60, 60, 60, 60 and 60 seconds

#### Scenario: The repository family still settles at the fifth delivery

- **WHEN** the README of a repository request stays unavailable
- **THEN** the fifth delivery settles one `knowledge.repository_analysis.failed.v1` fact and no sixth delivery happens

### Requirement: A redelivery is visible without a dead letter

Every negative acknowledgement of a retryable failure SHALL be counted by the label-free `knowledge_ingest_redeliveries_total` on the operator listener, and from the second delivery on it SHALL be logged with the family, the class and the delivery count and without content. Knowledge SHALL have no table that records an abandoned delivery.

#### Scenario: The counter and the log line

- **WHEN** a social capture fails for four deliveries
- **THEN** the operator listener shows a non-zero `knowledge_ingest_redeliveries_total`, the log has a line per delivery from the second on with `family`, `class` and `delivered`, and no line contains the post text or the tenant

#### Scenario: No dead-letter table

- **WHEN** the schema is applied to an empty database
- **THEN** `knowledge.ingest_failures` does not exist

## MODIFIED Requirements

### Requirement: Consumers verify their durables and classify failures

Each ingest consumer SHALL verify its Edge-provisioned durable, terminate permanently invalid input without redelivery, and negative-acknowledge transient failures with a delay that grows with the delivery count. A failure that a later delivery cannot fix, such as a response that stayed invalid, a permanent provider failure or a source the schema cannot hold, SHALL be terminated at its first delivery with a closed class.

#### Scenario: Wrong producer is terminated

- **WHEN** a message with a producer that is not allowed for the family is delivered
- **THEN** it is terminated, never redelivered, and no analysis state is written

#### Scenario: A permanent analysis failure is terminated

- **WHEN** the provider fails permanently for a social capture
- **THEN** the delivery is terminated with the class `unanalysable` instead of being redelivered

### Requirement: An archive tombstone is never dead-lettered or half finished

A transient failure while applying an `ai_archive.subject.tombstoned.v1` delivery SHALL be redelivered without a delivery budget, and a redelivery of an already applied tombstone SHALL drain the blob erasure queue before it is acknowledged.

#### Scenario: More than five transient failures

- **WHEN** the database refuses a tombstone for eight consecutive deliveries and then recovers
- **THEN** no delivery is terminated and the ninth delivery removes the derived rows

#### Scenario: The blob erasure failed after the rows were deleted

- **WHEN** the first delivery committed the row deletion and the erasure queue and the blob removal then failed
- **THEN** the redelivery drains the queue, the blob files are gone and the delivery is acknowledged

### Requirement: An archive fact that can never succeed is rejected at once

A tombstone for a subject that belongs only to another tenant SHALL be terminated at its first delivery with the class `foreign_subject`, an undecodable archive fact with `undecodable` and an unsupported archive type with `unsupported`, without touching the other tenant's rows.

#### Scenario: A foreign tombstone

- **WHEN** a tenant publishes a tombstone for another tenant's conversation
- **THEN** the delivery is terminated at once, the class is logged, and the other tenant's rows are intact
