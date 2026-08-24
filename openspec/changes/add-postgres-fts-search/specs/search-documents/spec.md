# Search Documents Specification

## Purpose

Defines the tenant-scoped search projection derived from accepted analyses and its authorized ranked retrieval over the admin plane.

## ADDED Requirements

### Requirement: Accepted analyses become searchable exactly once per source

When an article analysis result is accepted and persisted, Knowledge SHALL maintain one search document per source-reference identity containing the document identifier, derived title, lead text, body text, and the newest accepted output identifier. A run that does not produce an accepted persisted result SHALL NOT create or modify any search document.

#### Scenario: an accepted run projects its source

- **WHEN** an article analysis completes with an accepted persisted result for a source that had no search document
- **THEN** exactly one search document exists for that source identity carrying its document identifier, title, lead, and body

#### Scenario: a failed run leaves no trace

- **WHEN** an analysis run ends without an accepted persisted result
- **THEN** no search document exists for that source and any previously projected document for it is unchanged

### Requirement: Reanalysis replaces the projection with the newest winner

When a newer accepted analysis exists for a source that already has a search document, Knowledge SHALL replace the projected title, lead, body, and output reference with values derived from the newest accepted output. An older accepted output SHALL never overwrite a newer projection.

#### Scenario: a newer accepted run wins

- **WHEN** a second accepted analysis completes for an already-projected source
- **THEN** the search document reflects the second output's derivation and references the second output

### Requirement: Retrieval is scoped to one tenant

Search retrieval SHALL restrict every result, including titles, snippets, ranks, and counts, to documents whose ownership matches the requested tenant. A tenant SHALL NOT observe another tenant's documents through any search path.

#### Scenario: another tenant's document is invisible

- **WHEN** tenant B searches for text that only tenant A's projected document contains
- **THEN** the response contains no results referencing tenant A's document

### Requirement: Matched searches rank and explain results

A non-empty query SHALL order matched documents by descending relevance with a stable deterministic tie-break, and each result SHALL expose a relevance score greater than zero and a bounded textual snippet taken from the projected document where the query matched.

#### Scenario: a title match outranks a body-only match

- **WHEN** two documents in one tenant match the same query, one in its title and one only in its body
- **THEN** the title-matching document is ordered first

#### Scenario: snippets stay bounded

- **WHEN** a query matches a very long body
- **THEN** the returned snippet remains within a small bounded length and marks the matched region

### Requirement: Empty queries browse by recency

An absent, empty, or whitespace-only query SHALL be an explicit browse mode returning the tenant's documents ordered by most recent projection update first, with no snippet and no relevance claim.

#### Scenario: browsing without a query

- **WHEN** a tenant requests a page with no query text
- **THEN** results are ordered by descending projection update time and carry no snippet or positive match score

### Requirement: Page size and offset bounds are explicit and finite

Retrieval SHALL accept only a page size of at least one and at most one hundred and a non-negative offset. Out-of-bounds values SHALL be rejected before any database work as a caller-visible error, not silently clamped.

#### Scenario: oversized page size is rejected

- **WHEN** a caller requests a page size of zero or above the maximum bound
- **THEN** retrieval fails with an explicit invalid-parameter error and no query runs

### Requirement: Search is reachable on the admin plane

The admin listener SHALL serve search retrieval under `/internal/search` requiring a tenant parameter, accepting optional query text, page size, and offset parameters, and returning JSON results with owner context, document identifier, title, snippet, and score. Missing or invalid parameters SHALL yield a client-error response without leaking other tenants' information.

#### Scenario: a well-formed search request succeeds

- **WHEN** the endpoint receives a valid tenant and query
- **THEN** it responds successfully with a JSON body whose results carry the documented fields

#### Scenario: a request without a tenant is refused

- **WHEN** the endpoint receives no tenant parameter
- **THEN** it responds with a client error and an explanatory message
