## 1. Published request intake

- [x] 1.1 Pin the published `ratatoskr-github-contracts` request contract; this dependency update has no RED because it consumes a separately published artifact.
- [x] 1.2 RED: add repository request replay/terminal-link tests in `crates/knowledge/tests/repository_analysis.rs`; they failed before durable request admission existed.
- [x] 1.3 GREEN: persist request identities and terminal facts idempotently; focused request tests pass.

## 2. README analysis and projection

- [x] 2.1 RED: add `family_pipeline::repository_request_acquires_readme_then_projects_search_document`, asserting contract request → authorized immutable README bytes → validated output → search document; it failed before the repository worker existed.
- [x] 2.2 GREEN: add the repository prompt, strict schema, metadata/README context, integrity-checked resolver boundary, state-machine execution, and search projection; focused test passes.
- [x] 2.3 RED: add the invalid evidence-excerpt assertion to repository validation; it failed before metadata/README grounding was checked.
- [x] 2.4 GREEN: reject evidence absent from both the acquired README and supplied metadata, preserving invalid attempts without a projection.

## 3. Shared budget and gates

- [x] 3.1 RED: extend the cross-family shared-ledger test to repository execution after social usage; it failed before repository execution used the common controlled-provider ledger.
- [x] 3.2 GREEN: use the existing durable usage ledger through `ControlledProvider`; repository execution is refused before provider invocation when another family exhausted it.
- [x] 3.3 Run the complete `DEVELOPMENT.md` gate and strict OpenSpec validation.
