## ADDED Requirements

### Requirement: An equal-time delivery with a later event id replaces the head

For one source, a delivery SHALL replace the analysis head when its `observed_at` is later than the head's, or equal with a greater event id. A strictly older delivery, or an equal one with a smaller event id, SHALL be historical, and a repeated event SHALL be a duplicate.

#### Scenario: The later event id wins

- **WHEN** two deliveries with equal `observed_at` and different digests are accepted in event id order
- **THEN** the second is `AcceptedCurrent`, becomes the head and is analysed

#### Scenario: The earlier event id delivered second

- **WHEN** the delivery with the greater event id arrives first and the one with the smaller event id second
- **THEN** the second is `AcceptedHistorical` and the head is unchanged

#### Scenario: Equal digests keep the head

- **WHEN** two deliveries share `observed_at` and digest
- **THEN** the analysis is not repeated

#### Scenario: A survivor re-announcement removes the deleted part

- **WHEN** a conversation with part P is deleted by an archive tombstone and the retained archive re-announces the conversation without P under its original `imported_at`
- **THEN** P is not searchable and the conversation is

### Requirement: A source is searchable as its head only

When the head revision of a social or AI-archive source is analysed, Knowledge SHALL remove the search documents and projection inputs of the source's superseded revisions in the same transaction, and a revision that is persisted after the head moved SHALL NOT become searchable. The analyses of superseded revisions SHALL be kept.

#### Scenario: The superseded text stops being served

- **WHEN** a second revision of a source with a different digest and a later event id becomes the head and is analysed
- **THEN** a term only the first revision contained finds nothing, a term of the second revision finds the source, and both source revisions remain

#### Scenario: A late analysis of a superseded revision

- **WHEN** the analysis of an older revision is persisted after the head moved to a newer revision that was already analysed
- **THEN** the older revision is not searchable
