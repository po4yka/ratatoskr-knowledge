## ADDED Requirements

### Requirement: Tenant deletion removes the channel recap state

Deleting a tenant SHALL delete, in the deletion transaction and in foreign-key order, every row of the six channel recap tables owned by that tenant, queue the deleted attempts' raw-response digests for erasure, count the rows in `channel_recap_rows` and fail scope verification if any survives. Source and archive scope SHALL NOT touch these tables.

#### Scenario: Only the deleted tenant's recap state goes

- **WHEN** two tenants have recap rows and one raw-response digest in common and the first tenant is deleted
- **THEN** every row of the first tenant is gone, the second tenant's rows and blob remain, and only unreferenced digests are erased

### Requirement: Blob registration and erasure share one digest lock

Knowledge SHALL take a per-digest advisory lock before registering a reference to a Knowledge-owned blob and while erasing one, verify the blob is present at registration and re-store it from the bytes the registrar holds when an eraser removed it, so that no committed row references a missing blob.

#### Scenario: The eraser wins the race

- **WHEN** an eraser holds the digest lock after its re-check while a registrar tries to register the same digest
- **THEN** the registrar blocks, finds the blob missing, re-stores it and commits a row that references an existing blob
