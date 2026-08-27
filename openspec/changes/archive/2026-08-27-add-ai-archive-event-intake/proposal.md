# AI archive event intake

## Why

Knowledge can analyze a directly supplied `AiConversation`, but it has no contract-envelope
consumer, completion linkage, or explicit tombstone path. The published AI archive contract now
carries normalized conversation lifecycle facts and provenance; those facts must become the only
archive-to-Knowledge ingress before analysis and search projection.

## What Changes

- Consume the published archive conversation added/updated envelope fixtures through the durable
  inbox, retaining the full provenance-bearing payload.
- Emit the published `knowledge.ai_archive_analysis.completed.v1` linkage only after the matching
  conversation revision has been accepted, with its exact owner, conversation ID, digest, and run.
- Consume explicit archive/conversation tombstones and remove matching Knowledge-owned derived
  source, analysis, and search data. Snapshot absence remains a non-deletion state.
- Extend the editable Knowledge schema in place for event delivery/tombstone receipts; no migration
  tooling or compatibility path is introduced.

## Impact

- Modifies `ai-archive-analysis`.
- Consumes `ratatoskr-ai-archive-contracts` at published revision `51065776`.
- Implements the consumer side of workspace change `add-ai-archive-knowledge-intake`.
