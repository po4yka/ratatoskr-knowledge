## 1. Tenant-scoped persistence

- [x] 1.1 Add `crates/knowledge/tests/user_content.rs::tag_names_are_unique_within_one_tenant`, apply the current schema, and confirm its tag insert fails because the table and constraint do not exist.
- [x] 1.2 Add the current-schema tag and tagging tables plus persistence commands so `tag_names_are_unique_within_one_tenant` passes; verify the focused test with `cargo nextest run --locked -p ratatoskr-knowledge --test user_content tag_names_are_unique_within_one_tenant`.
- [x] 1.3 Add `tag_merge_deduplicates_analysis_taggings` and confirm it fails because merge leaves the source tag or duplicate tagging.
- [x] 1.4 Implement an ordered-lock transactional tag merge and verify `tag_merge_deduplicates_analysis_taggings` passes.
- [x] 1.5 Add `collection_moves_preserve_unaffected_order` and confirm it fails because collection rows and explicit position handling do not exist.
- [x] 1.6 Implement collection and collection-item commands with dense locked positions and verify `collection_moves_preserve_unaffected_order` passes.

## 2. Analysis state, feedback, and tenant isolation

- [x] 2.1 Add `analysis_state_transitions_are_idempotent` and confirm it fails because per-analysis state persistence does not exist.
- [x] 2.2 Implement read/unread and favorite state commands with one tenant-scoped row per accepted analysis, then verify `analysis_state_transitions_are_idempotent` passes.
- [x] 2.3 Add `foreign_tenant_cannot_read_or_mutate_user_content` and confirm it fails because every user-content target does not yet use an authorization-scoped lookup.
- [x] 2.4 Apply same-tenant target predicates to tags, collections, state, and feedback commands and verify `foreign_tenant_cannot_read_or_mutate_user_content` passes.
- [x] 2.5 Add `typed_feedback_does_not_mutate_accepted_analysis` and confirm it fails because typed feedback persistence does not exist.
- [x] 2.6 Implement bounded typed feedback records and verify `typed_feedback_does_not_mutate_accepted_analysis` passes.

## 3. Immutable text annotations

- [x] 3.1 After the `add-document-ir-block-identifiers` contract is available, add `highlight_rejects_unknown_block_and_out_of_range_unicode_offsets` and confirm it fails because no anchor validator or highlight table exists.
- [x] 3.2 Implement source-revision/document-digest/block-ID validation and five-color-or-underline highlight persistence without storing source text; verify `highlight_rejects_unknown_block_and_out_of_range_unicode_offsets` passes.
- [x] 3.3 Add `source_and_tenant_deletion_remove_dependent_user_content_with_receipts` and confirm it fails because deletion does not include the new dependent rows and counts.
- [x] 3.4 Extend deletion transactions and receipts for user-content rows, then verify `source_and_tenant_deletion_remove_dependent_user_content_with_receipts` passes.

## 4. Internal surface and documentation

- [x] 4.1 Add `services/knowledge/tests/admin.rs::user_content_routes_require_tenant_scope_and_hide_foreign_targets` and confirm it fails because the internal user-content routes do not exist.
- [x] 4.2 Implement bounded no-store `/internal/user-content/...` CRUD routes and stable error mapping, then verify `user_content_routes_require_tenant_scope_and_hide_foreign_targets` passes.
- [x] 4.3 Document the internal surface, ownership, anchor evidence, and explicit non-goals in README; no RED test applies because this is documentation, and verify links and current behavior against the route tests.

## 5. Validation

- [x] 5.1 Run the focused user-content and admin suites, the documented full Knowledge gate through `build-gate`, strict OpenSpec validation, and final diff review; verify no migration file, legacy import, sharing, goals, or source acquisition was added.
