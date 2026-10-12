## Context

XR-021 CONTRACTS.md R5-08 rule 2 is the contract.

## Decisions

### Order by `(observed_at, inbox_event_id)`

The pair is a total order on deliveries of one source. A re-announced fact has the same `observed_at` and a later event id, so it replaces the head and is analysed again when its digest differs; an unchanged digest keeps the analysis revision. The Postgres `uuid` type compares bytewise, which is time order for UUIDv7.

### Nothing else moves

`tombstone_blocks` keeps comparing the tombstone's `observed_at` with the delivery's, so a re-announced conversation without the deleted part is admitted (its time is not earlier than the tombstone's removal in the sequence the survivor test replays) and the part stays unsearchable.
