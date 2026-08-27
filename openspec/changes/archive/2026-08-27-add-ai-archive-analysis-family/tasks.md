## 1. Contract intake and replay

- [x] 1.1 Pin the published `ratatoskr-ai-archive-contracts` revision; this dependency update has no RED because it exposes the published contract to behavior tests.
- [x] 1.2 RED: add archive delivery and redelivery assertions in `crates/knowledge/tests/source_inbox.rs`; they failed before durable archive intake existed.
- [x] 1.3 GREEN: persist archive inbox receipts and monotonic heads without replacing a newer revision; focused tests pass.

## 2. Analysis and projection

- [x] 2.1 RED: add `family_pipeline::archive_event_produces_grounded_analysis_and_search_document`, asserting fixture event → selected-message context → validated output → one search projection; it failed before the family worker existed.
- [x] 2.2 GREEN: implement the archive prompt, strict schema, conversation context, state-machine execution, and tenant search projection; focused test passes.
- [x] 2.3 RED: add `source_inbox::archive_analysis_rejects_a_decision_citing_an_absent_message`; it failed before message-scoped citation validation existed.
- [x] 2.4 GREEN: reject decisions and summary grounding IDs outside the selected conversation message set; focused test passes.

## 3. Shared budget and gates

- [x] 3.1 RED: add the shared-ledger exhaustion assertion after a social execution; it failed before cross-family controlled-provider composition existed.
- [x] 3.2 GREEN: charge archive execution through the existing shared controlled-provider ledger; cross-family test passes without a provider call.
- [x] 3.3 Run the complete `DEVELOPMENT.md` gate and strict OpenSpec validation.
