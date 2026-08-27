## Purpose

Defines tenant-scoped user organization, reading state, annotations, and feedback over immutable
Knowledge analyses and source references without altering source evidence or analysis history.

## ADDED Requirements

### Requirement: Tags are tenant-scoped and mergeable

Knowledge SHALL create tags under one tenant only, SHALL reject a duplicate normalized tag name in
that tenant, and SHALL not expose or mutate another tenant's tags. A merge SHALL atomically retag all
target analyses from the source tag to the destination tag, remove duplicate taggings, and delete the
source tag; source and destination tags SHALL belong to the same tenant.

#### Scenario: same tag name in two tenants

- **WHEN** two different tenants create tags with the same normalized name
- **THEN** both tags exist and neither tenant can list or tag an analysis through the other tenant's tag

#### Scenario: merge preserves unique taggings

- **WHEN** a tenant merges a source tag into a destination tag and an analysis has both tags
- **THEN** the analysis has exactly one destination tagging and the source tag no longer exists

### Requirement: Collections preserve a stable explicit order

Knowledge SHALL let a tenant create collections and place each accepted analysis or immutable source
reference in an explicit position. Listing a collection SHALL return items by that position with a
deterministic tie-breaker, and an item move SHALL not reorder any unaffected item. A collection SHALL
not reference a target belonging to another tenant.

#### Scenario: moving one collection item

- **WHEN** a tenant moves the middle item of a three-item collection after the final item
- **THEN** the resulting order is first, final, middle and no item changes target

### Requirement: Analysis state is tenant-isolated and transition-safe

Knowledge SHALL store read/unread and favorite state per tenant and accepted analysis. A state change
SHALL target only an analysis owned by the request tenant, SHALL be idempotent when repeated, and
SHALL reject a request that addresses a foreign or non-accepted analysis without revealing its state.

#### Scenario: repeated favorite operation

- **WHEN** a tenant marks the same accepted analysis favorite twice
- **THEN** exactly one state record remains and it is favorite

#### Scenario: foreign analysis state request

- **WHEN** a tenant requests a state change for another tenant's accepted analysis
- **THEN** no state record is created or changed and the request reports an authorization-scoped absence

### Requirement: Highlights are validated against immutable Document IR text

Knowledge SHALL store a highlight only with its tenant, accepted analysis, immutable source revision,
stable Document IR block identifier, Unicode-scalar start offset, Unicode-scalar end offset, and one
of five configured colors or underline. It SHALL accept an anchor only when the supplied Document IR
revision matches the source reference, the block exists, and `0 <= start < end <= block length`; it
SHALL reject invalid anchors without persisting a partial highlight.

#### Scenario: valid highlighted range

- **WHEN** a tenant highlights a non-empty range entirely inside a block from the analysis source revision
- **THEN** the highlight is stored with the requested block identifier, offsets, and style

#### Scenario: invalid offset is rejected

- **WHEN** a tenant submits an end offset beyond the Unicode-scalar length of the referenced block
- **THEN** no highlight is stored and the request reports an invalid anchor

### Requirement: Feedback is typed and scoped to accepted analyses

Knowledge SHALL store a tenant-scoped feedback record only for an accepted analysis owned by that
tenant. Each record SHALL use a documented issue category and bounded optional detail; it SHALL not
alter the analysis result, source reference, or model evidence.

#### Scenario: feedback leaves evidence immutable

- **WHEN** a tenant records a grounding issue for an accepted analysis
- **THEN** the feedback is retrievable for that tenant and the accepted analysis result remains unchanged

### Requirement: Internal user-content operations are authorization-scoped

The internal CRUD surface SHALL require a tenant scope for every operation and SHALL use that scope
for target lookup, write predicates, and response filtering. It SHALL return stable bounded error
codes without exposing another tenant's tags, collections, state, highlights, or feedback.

#### Scenario: collection lookup is isolated

- **WHEN** a tenant requests a collection identifier owned by another tenant
- **THEN** the response is indistinguishable from a missing collection in that tenant scope

### Requirement: Deferred user-content capabilities remain absent

Knowledge SHALL not introduce multi-user collaborators or invites, public collection links, user
goals or streaks, saved-search persistence, or legacy user-content import in this change.

#### Scenario: no legacy import endpoint exists

- **WHEN** an internal caller attempts to invoke a user-content legacy import operation
- **THEN** Knowledge exposes no such operation
