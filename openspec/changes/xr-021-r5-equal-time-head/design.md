## Context

XR-021 CONTRACTS.md R5-08 rule 2 is the contract.

## Decisions

### Order by `(observed_at, inbox_event_id)`

The pair is a total order on deliveries of one source. A re-announced fact has the same `observed_at` and a later event id, so it replaces the head and is analysed again when its digest differs; an unchanged digest keeps the analysis revision. The Postgres `uuid` type compares bytewise, which is time order for UUIDv7.

### Nothing else moves

`tombstone_blocks` keeps comparing the tombstone's `observed_at` with the delivery's, so a re-announced conversation without the deleted part is admitted (its time is not earlier than the tombstone's removal in the sequence the survivor test replays) and the part stays unsearchable.

### A source is searchable as its head

Replaying the survivor sequence showed a second gap behind the head rule: once the re-announced fact became the head and was analysed, the revision it replaced kept its search document, so the text the deleted export wrote was still found. Source revisions are immutable and keep their analyses and raw responses as history (AGENTS.md), but the search projection must follow the head. `retire_superseded_projections` runs in the same transaction as the projection of a persisted revision and deletes the `search_documents` and `search_projection_inputs` rows of every revision of that source whose digest is not the head's. The rule is stated from the revision just persisted and covers both orders: a newer head retires the older revisions, and an older revision persisted late (its analysis was in flight when the head moved) retires itself. A source with no head row (a repository, an extracted document) is left alone. The projection input goes with the document so that `reindex-search-documents` cannot bring the old text back; embedding chunks of a retired revision are no longer reachable because retrieval joins `search_documents`.
