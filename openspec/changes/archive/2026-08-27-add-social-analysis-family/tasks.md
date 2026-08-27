## 1. Contract intake and replay

- [x] 1.1 Pin the published `ratatoskr-social-contracts` revision; this dependency update has no RED because it exposes the published contract to the behavior tests.
- [x] 1.2 RED: add the social fixture delivery and replay assertions in `crates/knowledge/tests/source_inbox.rs`; they failed before durable social intake existed.
- [x] 1.3 GREEN: persist social inbox receipts and monotonic source heads keyed by event and content revision; focused replay tests pass.

## 2. Analysis and projection

- [x] 2.1 RED: add `family_pipeline::social_event_replay_produces_one_analysis_and_search_document`, asserting fixture event → provider → validated output → one search projection; it failed before the family worker existed.
- [x] 2.2 GREEN: implement the social prompt, strict schema, post-excerpt grounding validation, state-machine execution, and tenant search projection; focused test passes.
- [x] 2.3 RED: add the unsupported-excerpt case to schema validation coverage; it failed before post-only grounding validation existed.
- [x] 2.4 GREEN: reject social evidence excerpts absent from the normalized post and preserve the invalid response attempt without a projection.

## 3. Shared budget and gates

- [x] 3.1 RED: add `family_pipeline::one_shared_ledger_blocks_archive_after_social_usage`, asserting a shared provider ledger performs no second-family call after social spend; it failed before composition through the common ledger.
- [x] 3.2 GREEN: compose social execution with the existing controlled provider and durable ledger; cross-family test passes.
- [x] 3.3 Run the complete `DEVELOPMENT.md` gate and strict OpenSpec validation.
