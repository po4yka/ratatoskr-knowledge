## ADDED Requirements

### Requirement: Search results identify accepted analyses and effective read state

Each tenant-scoped search or recency result SHALL expose the newest accepted analysis output identifier for its projected source and the effective `read` or `unread` state for that output. An accepted output without an `analysis_user_states` row SHALL have effective state `unread`; a state row for an older output SHALL NOT alter the newer output's effective state.

#### Scenario: Newest accepted output defaults unread

- **WHEN** a projected source points to a newest accepted output with no state row and an older output has a `read` row
- **THEN** the result identifies the newest output and reports it as `unread`

### Requirement: Read-state filtering applies inside the tenant query

Search and blank-query recency browse SHALL accept an optional closed read-state filter and apply it after tenant isolation but before ranking, recency ordering, offset, limit, and `has_more` calculation. Invalid filters SHALL fail before database work.

#### Scenario: Unread filter fills a page

- **WHEN** one tenant has read and unread matches interleaved by rank and requests an unread page
- **THEN** the result page contains the requested number of unread matches when that many exist and preserves their relative rank order

#### Scenario: Foreign state cannot affect pagination

- **WHEN** another tenant has a read-state row for an identifier that matches the caller's query text
- **THEN** the caller's filtered results and page boundary are unchanged
