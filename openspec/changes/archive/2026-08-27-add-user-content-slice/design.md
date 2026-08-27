## Context

See proposal.md and the `user-content` delta. Knowledge has a single PostgreSQL schema, immutable
source references, accepted analysis outputs, and a loopback JSON admin router. It does not retain
Document IR block text after analysis, so it cannot re-fetch or reconstruct text to validate an
annotation anchor.

## Goals / Non-Goals

**Goals:**

- Preserve tenant isolation in database constraints, read/write predicates, and responses.
- Make tag merge and collection moves transactional and deterministic.
- Validate a highlight against caller-supplied immutable Document IR without persisting source text.
- Keep analysis, source evidence, and deletion semantics coherent when user-content rows exist.

**Non-Goals:**

- General authorization infrastructure, a public API, sharing, collaboration, goals, saved searches,
  or legacy data migration.
- Highlight rebasing across source revisions; anchors remain pinned to their immutable source revision.

## Decisions

### User content is a normalized, tenant-carrying projection

Create current-schema tables for tags, taggings, collections, collection items, analysis state,
highlights, and feedback. Every table carries `tenant_ref`; every reference to an analysis output or
source revision is checked through a same-tenant join in the command transaction. This makes
cross-tenant access fail closed even if a caller has guessed an identifier. Database uniqueness
enforces normalized tag names and one state row per `(tenant, analysis output)`.

Store an analysis-output identifier rather than a mutable summary projection. A collection item uses
exactly one of accepted analysis output or source reference; a check constraint encodes that choice.
The alternative of a generic polymorphic string target would make referential integrity and deletion
unsafe.

### Collection positions are dense and transaction-owned

Collection insertion and move operations lock the owning collection row, shift only the affected
suffix/range, and maintain unique dense positions. Reads order by position then item identifier. This
is simpler and safer than fractional ranks for the small, user-managed collections in scope; it avoids
rank exhaustion and makes order tests exact.

### Highlight validation receives evidence but stores only an anchor

The highlight command carries the immutable Document IR revision used for validation. Knowledge
checks its document ID and digest against the accepted analysis source reference, resolves the stable
block ID, and validates Unicode-scalar offsets against that block's text. It persists only the source
reference, block ID, offsets, style, and user-content metadata. It neither stores the supplied text
nor fetches source data. This follows source ownership and avoids turning a text anchor into a second
source cache.

The companion workspace change supplies a block identifier stable within one immutable Document IR
revision. Cross-revision mapping is intentionally absent: the source revision is part of every anchor.

### Commands use the existing internal HTTP boundary

Extend the loopback Axum router with bounded JSON `/internal/user-content/...` routes. Each request
declares its tenant as the existing `/internal/search` route does; handlers parse bounded input,
delegate to library commands, and return stable JSON error codes with `no-store`. The library owns
SQL and invariants; routes never expose SQL errors or foreign-object existence. A command bus would
add an unimplemented infrastructure dependency without a current consumer.

### Deletion includes dependent user content

Source and tenant deletion paths delete dependent taggings, collection items, state, highlights, and
feedback in the same transaction before their referenced analysis/source rows. Deletion receipts gain
the corresponding counts. Cascading foreign keys alone are rejected because the audit receipt must
remain truthful and deterministic.

## Risks / Trade-offs

- [Concurrent tag merge or collection move] → lock rows in stable identifier order and cover the
  unique constraints with database tests.
- [Large caller-supplied Document IR] → reuse existing bounded request-body limits and validate only
  the requested block; do not log text.
- [Future source revision changes] → anchors retain immutable source identity and clients create a
  new highlight instead of silently rebasing text.
- [Internal tenant parameter is misrouted upstream] → all SQL predicates and returned rows include
  tenant scope; foreign targets are reported as scoped absence.

## Migration Plan

Edit `schema.sql` in place and create disposable databases from it. First land the shared Document
IR block-identifier contract, then the Knowledge consumer and its schema/API, then the extractor
producer. There is no deployed data, compatibility shim, or migration file. Before deployment,
rollback is a revert of the affected child commits and recreated development database.
