## ADDED Requirements

### Requirement: Every deletion fact is acknowledged

After an `ai_archive.subject.tombstoned.v1` or a `social.source.removed.v1` is applied (its rows deleted and its blobs collected), repeats an applied fact, or is refused as a foreign subject, Knowledge SHALL publish one `knowledge.deletion.acknowledged.v1` through its outbox with the family, the owner, the subject, the outcome and the acknowledged event id, and no content. A repeat delivery SHALL re-arm the row when it was published more than one minute earlier. A message that cannot be attributed SHALL NOT be acknowledged.

#### Scenario: A tombstone is acknowledged after the blobs are erased

- **WHEN** an AI-archive tombstone is applied
- **THEN** its blobs are erased and exactly one acknowledgement with family `ai_archive` and outcome `applied` naming the tombstone's event id is published afterwards

#### Scenario: No acknowledgement while the erasure fails

- **WHEN** blob erasure fails during a tombstone delivery
- **THEN** the delivery is redelivered and no acknowledgement is published

#### Scenario: A social removal is acknowledged

- **WHEN** a social removal is applied
- **THEN** exactly one acknowledgement with family `social` is published

#### Scenario: A lost acknowledgement is produced again

- **WHEN** a tombstone is redelivered after its acknowledgement was published and more than one minute has passed
- **THEN** a second acknowledgement is published

#### Scenario: A redelivery inside the window

- **WHEN** a tombstone is redelivered within one minute of the publication
- **THEN** no second acknowledgement is published

#### Scenario: A foreign subject is refused

- **WHEN** a tombstone names a subject that belongs only to another tenant
- **THEN** it is acknowledged with outcome `refused` and refusal `foreign_subject` and the delivery is terminated

#### Scenario: An unattributable message

- **WHEN** a message is undecodable, from a wrong producer or has no tenant
- **THEN** it is terminated and no acknowledgement is published

#### Scenario: A subject Knowledge never held

- **WHEN** a tombstone names a subject Knowledge never held
- **THEN** it is acknowledged `applied` and the tombstone is recorded
