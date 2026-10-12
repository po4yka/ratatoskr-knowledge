## ADDED Requirements

### Requirement: A tenant has at most one extractor document per content digest

Knowledge SHALL keep at most one `source_refs` row per tenant and content digest for the owner context `ratatoskr-extractor`, whatever `document_id` an event carries, and SHALL keep Knowledge-owned snapshots under the full identity key.

#### Scenario: The same document twice

- **WHEN** two `content.document.extracted.v1` events with equal tenant, digest and document id are accepted
- **THEN** there is one source row, one analysis run and one search document, and the second event is `AlreadyAnalysed`

#### Scenario: The same digest under two document ids

- **WHEN** two events with equal tenant and digest and different document ids are accepted
- **THEN** the second reuses the first row, and the search document keeps the first document id

#### Scenario: The same digest under two tenants

- **WHEN** two tenants submit the same digest
- **THEN** each keeps its own row

#### Scenario: The database guards the rule

- **WHEN** a second extractor source row with an equal tenant and digest is inserted under another document id
- **THEN** the insert resolves to the first row

#### Scenario: Archive snapshots are untouched

- **WHEN** two archives carry snapshots with the same digest under different archive ids
- **THEN** both rows exist
