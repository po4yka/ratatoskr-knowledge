# Design: AI archive event intake

## Boundary

The input is a typed `EventEnvelope` whose payload is one published AI archive lifecycle contract.
The consumer validates event type through the payload trait, then persists the envelope ID before it
schedules any analysis. The retained snapshot remains a state-carried contract value and does not
join Archive tables.

## Conversation flow

Conversation `added` and `updated` events share a single inbox family. Their payloads retain
`AiArchiveProvenance`; the head advances only on a newer observed time. `FamilyPipeline` loads the
claimed conversation, creates its immutable source/run identity, validates the archive-specific
analysis, and records the ordinary search projection. Its completion builder returns the published
analysis-completed payload using the exact conversation digest and an opaque run reference.

## Deletion flow

An explicit tombstone is separately deduplicated. A conversation tombstone resolves the corresponding
Knowledge source identity, then invokes the existing child-first deletion transaction so search,
outputs, attempts, runs, and source rows disappear together. An archive tombstone deletes every
AI-archive source scoped to that archive owner. Project and artifact tombstones are recorded and are
safe no-ops until those object kinds are admitted to a Knowledge analysis family. No path acts on a
missing snapshot.

## Privacy and failure handling

No event consumer logs content, titles, filenames, raw export paths, or payloads. Contract decoding,
unknown event type, durable writes, and deletion errors are typed failures. Inbox and tombstone
receipts make at-least-once delivery safe. The schema is edited in place because Ratatoskr remains in
development.
