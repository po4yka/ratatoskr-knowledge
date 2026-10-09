## ADDED Requirements

### Requirement: Extracted documents become analysed and searchable

Knowledge SHALL accept a `content.document.extracted.v1` event from the extractor producer only when the envelope carries a tenant, register the extractor-owned document blob as the source reference, run one article analysis per source revision and make the result searchable. A second delivery of the same event SHALL create no new analysis run.

#### Scenario: Extracted document is analysed once

- **WHEN** a valid extracted-document envelope is accepted twice
- **THEN** one source reference, one persisted analysis run and one search hit for the title exist, and an envelope without a tenant is rejected

### Requirement: Social removal deletes derived state and orders against captures

Knowledge SHALL treat `social.source.removed.v1` as a deletion of the source's heads, inbox rows, search documents and analyses and SHALL record a tombstone so an older capture delivered later is admitted as tombstoned while a later capture is admitted normally.

#### Scenario: Older capture after removal does not resurrect the source

- **WHEN** a social source is captured, removed, and the older capture event is redelivered
- **THEN** the redelivery is admitted as tombstoned and no search document exists for the source

### Requirement: Terminal repository and analysis facts are durable before they are published

Knowledge SHALL write each completion or failure fact to a transactional outbox in the same transaction as the state change that produced it, and SHALL publish it to the event stream with the outbox id as the message id, marking it published only after the broker acknowledged it.

#### Scenario: A terminal fact survives a crash before publication

- **WHEN** a repository analysis request settles and the process stops before publishing
- **THEN** exactly one unpublished outbox row with the stable dedupe key exists, and a second settle adds no row

### Requirement: Consumers verify their durables and classify failures

Each ingest consumer SHALL verify its Edge-provisioned durable, terminate permanently invalid input without redelivery, negative-acknowledge transient failures with a delay, and record a terminal failure without content after the fifth delivery.

#### Scenario: Wrong producer is terminated

- **WHEN** a message with a producer that is not allowed for the family is delivered
- **THEN** it is terminated, never redelivered, and no analysis state is written
