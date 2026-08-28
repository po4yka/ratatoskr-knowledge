## Why

Knowledge already persists authorized search documents and per-analysis read state, but its search page does not expose the accepted analysis identity or effective state and cannot filter unread items. Platform therefore cannot implement the fleet contract without guessing joins or overwriting unrelated favorite state.

## What Changes

- Extend tenant-scoped search/browse queries with an optional `read`/`unread` filter applied before ordering and pagination.
- Return the current accepted analysis output identifier and effective read state with each result; absence of an `analysis_user_states` row is `unread`.
- Add an idempotent read-state-only command that preserves the existing favorite value and every other user-content record.
- Extend the loopback internal HTTP surface with bounded query and read-state operations for Platform's dedicated client, retaining no-store responses and scoped absence for foreign targets.
- Keep search ranking, embedding selection, tags, collections, favorites, highlights, feedback, saved searches, and public authentication outside this repository-local change.
- Conform to workspace change `add-library-search-read-state-contract` and merge before Platform consumes the expanded internal surface.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `search-documents`: Search results gain accepted-analysis identity and effective read state, and search/browse supports a tenant-scoped read-state filter before pagination.
- `user-content`: A read-state-only transition becomes independently idempotent and preserves favorite and other user-content state.

## Impact

- `crates/knowledge/src/search.rs` and its database tests gain the joined projection and filter semantics.
- `crates/knowledge/src/user_content.rs` gains read-state-only mutation behavior over the existing current-schema table.
- `services/knowledge/src/admin.rs` and tests expose the bounded internal adapter used by Platform.
- No new dependency, table, migration, provider call, or public listener is introduced.
- Rollback is code-only after Platform and Telegram have first been rolled back; existing read-state rows remain valid.
