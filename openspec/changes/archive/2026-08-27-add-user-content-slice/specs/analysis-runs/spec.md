## MODIFIED Requirements

### Requirement: Source references preserve immutable identity

A source reference SHALL contain its tenant, owning bounded context, source document identifier,
Document IR content digest, and source provenance blob reference. A changed digest SHALL create a new
source revision rather than overwrite prior evidence. Accepted analyses and immutable source revisions
SHALL be addressable as tenant-scoped user-content targets without allowing user-content records to
mutate their source identity, accepted output, or execution history.

#### Scenario: one document arrives with a new digest

- **WHEN** a stored source identifier is registered with a different content digest
- **THEN** both revisions remain addressable and analysis uniqueness treats them as different inputs

#### Scenario: user content targets one immutable revision

- **WHEN** a tenant adds a collection item or highlight for an accepted analysis over one source revision
- **THEN** the user-content record retains that analysis and source-revision identity after a later revision is registered
