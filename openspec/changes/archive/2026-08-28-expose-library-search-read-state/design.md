## Context

See `proposal.md` for motivation. `knowledge.search_documents.latest_output_id` already identifies the accepted output behind a result. `knowledge.analysis_user_states` stores `(tenant_ref, output_id, read_state, favorite)` and absence currently implies schema-default unread only when a row is created. Search is tenant-scoped but returns neither output identity nor state, and `set_analysis_state` replaces both read and favorite fields.

## Goals / Non-Goals

**Goals:**

- Make the existing search result sufficient for an authorized read-state action.
- Define effective unread without materializing default rows during reads.
- Add a narrow mutation that cannot reset favorite.
- Preserve existing lexical/hybrid ranking and loopback-only deployment.

**Non-Goals:**

- Changing the search index, ranking fusion, embedding provider, or analysis version model.
- Adding a public listener, session verification, or Platform error envelopes.
- Migrating read state from accepted outputs to logical documents.

## Decisions

### D1: Join state to the projected latest output

Every lexical, hybrid, and blank-query branch selects `search_documents.latest_output_id` as `analysis_id` and left-joins `analysis_user_states` on both tenant and output. `COALESCE(read_state, 'unread')` defines the effective state. The same predicate is included in every ranking leg before candidate truncation so hybrid fusion and pagination cannot admit read rows into an unread page.

Joining after ranking was rejected because it under-fills pages and makes `has_more` false when unread matches remain. Resolving state by document identity was rejected because the current owned state is per accepted output.

### D2: SearchQuery carries a closed optional filter and page lookahead

A `ReadStateFilter` closed enum is validated with the existing limit/offset checks. Search branches fetch `limit + 1`, truncate to `limit`, and expose `has_more`. The internal HTTP query accepts only `read` and `unread`; malformed values fail before a SQL query.

### D3: Add a read-state-only upsert

The narrow operation verifies tenant ownership of an accepted output, inserts `(read_state, favorite=false)` when absent, and on conflict updates only `read_state` and `updated_at`. It returns the authoritative `ReadState`. Existing `set_analysis_state` remains for internal callers that intentionally replace the complete pair.

### D4: Extend the loopback adapter additively

`GET /internal/search` gains the optional filter and additive result/page fields. `/internal/user-content/command` gains `set_read_state` with tenant, output identifier, and state. It keeps no-store responses and existing scoped-not-found/error classes. Platform is the only new consumer; this route is not mounted on a public listener.

## Risks / Trade-offs

- [Hybrid and lexical SQL branches drift] -> shared filter fragments/types and branch-comparison database tests pin identical tenant/state semantics.
- [Limit plus one exceeds the current maximum] -> validation keeps public limit at 100 while internal candidate fetch uses a checked 101-row bound.
- [Older JSON consumers see new fields] -> changes are additive and serde consumers already ignore unknown fields; rollout still deploys Knowledge first.
- [Concurrent state updates race] -> one primary key and atomic upsert produce one final row; read-only updates never write favorite.

## Migration Plan

No schema migration or data backfill exists. Deploy the additive Knowledge binary first. Roll it back only after Platform and Telegram are rolled back; state rows remain valid under both binaries.
