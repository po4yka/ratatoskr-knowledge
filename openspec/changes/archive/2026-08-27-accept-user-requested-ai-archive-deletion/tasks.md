## 1. Consumer compatibility and scoped deletion

- [x] 1.1 RED — add `user_requested_archive_tombstone_is_deduplicated_and_scoped` in `crates/knowledge/tests/source_inbox.rs` with a safe fixture that seeds target and sibling analysis/search/embedding state; run the exact test through `build-gate` and confirm it fails because the current contracts pin rejects `reason = "user_requested"`
- [x] 1.2 GREEN — advance `ratatoskr-ai-archive-contracts` to the published contracts commit, rerun the test, and verify two deliveries remove every target-derived row once while the sibling tenant/subject remains searchable
- [x] 1.3 RED — add `user_requested_tombstone_refuses_a_cross_tenant_subject` in `crates/knowledge/tests/source_inbox.rs`; run it through `build-gate` and confirm the admission assertion fails because a mismatched owner currently records a successful zero-row deletion
- [x] 1.4 GREEN — validate archive/source ownership inside the tombstone delivery transaction before inserting the inbox receipt, return `InvalidArchiveFact` for a known subject owned only by another tenant, and verify source, inbox, and deletion audit state remain unchanged

## 2. Consumer gate and publication

- [x] 2.1 Run the full fenced gate from `DEVELOPMENT.md` through `build-gate` where compiler-backed, run `openspec validate --all --strict`, archive this change with its spec synced, rerun `openspec validate --archived`, then commit, integrate into `main`, push, and record the consumer commit that must precede ChatGPT production
