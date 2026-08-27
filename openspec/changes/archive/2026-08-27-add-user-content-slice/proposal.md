## Why

Analyses are useful only when people can organize, revisit, annotate, and correct them without
changing immutable source evidence or analysis history. Knowledge must own this user-content
projection beside its searchable analyses so those actions remain tenant-isolated, inspectable, and
replaceable.

## What Changes

- Add tenant-scoped tags and analysis taggings, including an atomic tag-merge operation that
  preserves taggings while eliminating the source tag.
- Add ordered collections whose items reference either accepted analyses or immutable source
  references, plus per-analysis read/unread and favorite state.
- Add text highlights anchored to a stable Document IR block identifier and UTF-8 character offsets;
  commands validate that the supplied block text belongs to the referenced source revision before
  storing the anchor.
- Add typed analysis feedback records and an authorization-scoped internal CRUD surface alongside
  the existing internal search route.
- Add schema, domain, and route tests for tenant isolation, ordering stability, state transitions,
  tag-name uniqueness, and highlight anchor validation.
- Record collaboration, public links, goals/streaks, saved searches, and legacy-data import as
  explicit non-goals.

## Capabilities

### New Capabilities

- `user-content`: Tenant-scoped organization, state, annotations, and feedback over Knowledge
  analyses and source references.

### Modified Capabilities

- `analysis-runs`: Accepted analysis and immutable source revision identifiers become valid
  user-content targets without changing analysis execution or evidence retention.

## Impact

Knowledge changes `schema.sql`, its Rust persistence/domain surface, the loopback internal API, tests,
and README/operational documentation. The schema remains the single current definition: no migration
files or version negotiation. A companion workspace changeset, `add-document-ir-block-identifiers`,
defines the required stable Document IR block identity and cross-repository rollout. No external
source acquisition, source-table access, legacy import, collaboration, public sharing, or goal
tracking is added.
