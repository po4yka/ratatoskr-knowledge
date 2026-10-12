## 1. The head order (R5-08 rule 2)

- [x] 1.1 RED: add `crates/knowledge/tests/source_inbox.rs` cases: equal `observed_at` and different digests, the later event id is `AcceptedCurrent` and becomes the head; the earlier event id delivered second is `AcceptedHistorical`; equal digests and times keep the head; a strictly older `observed_at` stays historical whatever the event id; a repeated event is `Duplicate`. Add `services/knowledge/tests/ingest_bus/lifecycle.rs`: a conversation fact with a part P, the tombstone of the archive that carried P, and a re-announced conversation without P under the retained archive's original `imported_at` leave P unsearchable and the conversation searchable. Run them and confirm the re-announced fact is historical.
- [x] 1.2 GREEN: change the upsert predicate to `excluded.observed_at > head.observed_at or (excluded.observed_at = head.observed_at and excluded.inbox_event_id > head.inbox_event_id)` and update `DEVELOPMENT.md` and `docs/`.

## 2. The search follows the head (R5-08 rule 3, the survivor sequence)

- [x] 2.1 RED: add `crates/knowledge/tests/source_inbox_head.rs::the_head_revision_is_the_only_one_that_search_serves` and `a_revision_persisted_after_the_head_moved_is_not_searchable`, and `services/knowledge/tests/ingest_bus/lifecycle.rs::a_survivor_re_announcement_under_the_same_instant_removes_the_deleted_part`; run them without `retire_superseded_projections` and confirm the superseded text is still searchable (`left: 1, right: 0`). Against the old head predicate the survivor test fails earlier for the stated reason: the re-announced fact is historical.
- [x] 2.2 GREEN: add `retire_superseded_projections` and call it in `FamilyPipeline::persist`.
