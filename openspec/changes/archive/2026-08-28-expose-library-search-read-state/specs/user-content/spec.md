## ADDED Requirements

### Requirement: Read-state-only replacement preserves other state

Knowledge SHALL provide an idempotent tenant-scoped operation that replaces only `read_state` for one tenant-owned accepted analysis. The operation SHALL preserve the stored favorite value and SHALL NOT mutate tags, collections, highlights, feedback, analysis evidence, or source evidence. If no state row exists, the operation SHALL create one with the requested read state and the schema-default favorite value.

#### Scenario: Marking a favorite read preserves favorite

- **WHEN** a tenant replaces an accepted favorite analysis's read state with `read`
- **THEN** the returned state is `read`, favorite remains true, and every other user-content row is unchanged

#### Scenario: Repeating a read state is idempotent

- **WHEN** a tenant replaces the same analysis state with `read` twice
- **THEN** both operations return `read` and exactly one tenant/output state row remains

#### Scenario: Foreign target is hidden

- **WHEN** a tenant attempts a read-state-only replacement for another tenant's accepted output
- **THEN** the operation reports the same scoped absence as a missing output and changes no row
