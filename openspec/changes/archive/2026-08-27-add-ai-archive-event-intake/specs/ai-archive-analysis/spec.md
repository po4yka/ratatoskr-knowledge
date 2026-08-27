## MODIFIED Requirements

### Requirement: Archive facts create revision-bound analysis work

Knowledge SHALL consume the published AI-archive conversation lifecycle contract envelopes through
a durable, at-least-once-safe inbox before it creates analysis work. It SHALL preserve the supplied
archive provenance with the admitted revision and SHALL never query Archive-owned tables.

#### Scenario: Conversation fixture produces one archive-item analysis

- **WHEN** a valid ChatGPT or Claude conversation event fixture is delivered
- **THEN** Knowledge retains one inbox receipt and queues one analysis whose provenance names the exact conversation or message revision

#### Scenario: Published conversation event is admitted

- **WHEN** a valid `ai_archive.conversation.added.v1` or
  `ai_archive.conversation.updated.v1` envelope is delivered
- **THEN** Knowledge retains one receipt carrying the exact state payload and only a newer observed
  revision advances the current analysis head

#### Scenario: Replayed or out-of-order archive fact is safe

- **WHEN** an archive event is redelivered or an older graph revision arrives after a newer one
- **THEN** no duplicate analysis or budget charge occurs and the searchable current projection does not regress

## ADDED Requirements

### Requirement: Archive analysis publishes a revision-bound completion linkage

Knowledge SHALL produce `knowledge.ai_archive_analysis.completed.v1` only for an accepted archive
analysis and the payload SHALL name the exact provider, archive/conversation subject, immutable
content digest, and Knowledge run reference that produced it.

#### Scenario: Conversation analysis completion round-trips

- **WHEN** an admitted conversation revision completes archive analysis
- **THEN** serializing and decoding the completion payload preserves the exact conversation identity
  and content digest used for search projection

### Requirement: Explicit tombstones remove derived archive data

Knowledge SHALL consume explicit `ai_archive.subject.tombstoned.v1` contracts idempotently and
delete matching Knowledge-owned source revisions and derived search state. It SHALL NOT delete
anything merely because a later export omits an object.

#### Scenario: Conversation tombstone propagates to search

- **WHEN** a tombstone for an analyzed archive conversation is delivered
- **THEN** its source, analysis, and search projection are absent from Knowledge after the receipt commits

#### Scenario: Snapshot absence is not deletion

- **WHEN** a newer archive snapshot does not include a formerly admitted conversation and supplies
  no explicit tombstone
- **THEN** the conversation's existing derived state remains available
