## 1. Search identity and effective state

- [x] 1.1 RED: add `crates/knowledge/tests/search.rs::newest_accepted_output_carries_effective_read_state` seeding an older read output and a newer projected output without a state row, run the focused test, and confirm it fails because results expose no analysis identity/read state.
- [x] 1.2 GREEN: extend the search result/page types and every lexical, hybrid, and browse query to return `analysis_id`, effective `read_state`, and `has_more`; rerun `newest_accepted_output_carries_effective_read_state` and verify the newer output is reported unread.
- [x] 1.3 RED: add `crates/knowledge/tests/search.rs::read_state_filter_precedes_ranking_offset_and_page_boundary` with interleaved owner/foreign and read/unread candidates, run it, and confirm the unread page is under-filled or has the wrong `has_more` because filtering is absent.
- [x] 1.4 GREEN: add the closed optional state filter to `SearchQuery` and apply the tenant/state predicate before candidate ranking, ordering, offset, and limit+one truncation in every query branch; rerun the focused search suite and verify full pages, stable relative order, and foreign isolation.

## 2. Read-state-only mutation

- [x] 2.1 RED: add `crates/knowledge/tests/user_content.rs::read_state_only_transition_is_idempotent_and_preserves_favorite` covering absent state, favorite=true, repeated calls, and a foreign output; run it and confirm the existing complete-state operation cannot satisfy preservation through a read-only input.
- [x] 2.2 GREEN: implement and export the tenant-scoped read-state-only upsert that updates only `read_state`/`updated_at` on conflict and returns scoped absence for foreign/missing outputs; rerun the focused user-content suite and verify favorite and unrelated rows are unchanged.

## 3. Loopback Platform adapter

- [x] 3.1 RED: add `services/knowledge/tests/admin.rs::library_search_and_read_state_adapter_is_bounded_and_tenant_scoped` asserting optional filter/result fields/`has_more`, invalid-filter preflight, read-only-state preservation, no-store headers, and identical foreign/missing absence; run it and confirm the current routes fail those assertions.
- [x] 3.2 GREEN: extend `/internal/search` and `/internal/user-content/command` additively with the validated filter/page fields and `set_read_state` operation, retaining bounded JSON and stable internal errors; rerun the focused admin tests and verify all assertions pass.

## 4. Documentation and gate

- [x] 4.1 Update README/architecture status to describe the expanded loopback search/read-state adapter and cite workspace contract `library-search-read-state`; cannot start from a failing behavior test because this is documentation, so verify links and terminology with the repository documentation checks.
- [x] 4.2 Run the complete command block in `DEVELOPMENT.md`, strict OpenSpec validation for this change, and the repository's archived/spec validation; verify every command passes without weakening lints, security policy, or database tests.
