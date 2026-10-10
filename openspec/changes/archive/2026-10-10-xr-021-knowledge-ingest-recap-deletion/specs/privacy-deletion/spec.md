## ADDED Requirements

### Requirement: Blob erasure is limited to digests the deletion enqueued

Deleting a scope SHALL enqueue only the Knowledge-owned blob digests of that scope in the same transaction as the row deletions and SHALL remove a queued blob only when no BlobRef-bearing column still references it. No deletion SHALL enumerate the blob root.

#### Scenario: Deleting an unrelated tenant keeps other tenants' blobs

- **WHEN** a tenant that owns nothing is deleted while another tenant holds a source snapshot and a recap raw response in the same blob root
- **THEN** both blobs remain readable and the receipt reports no removals

### Requirement: Deletion removes content-bearing inbox state

Deleting a tenant SHALL remove its inbox snapshots, heads, object inbox payloads, repository requests, tombstones and outbox rows, and source or archive scope SHALL remove the matching rows while keeping sibling rows and the tombstone's own deduplication receipt.

#### Scenario: Tenant deletion leaves no content-bearing state

- **WHEN** a tenant with rows in every one of those tables is deleted
- **THEN** none of its rows remain, another tenant's rows are untouched, and the scope verification fails if any inbox row survives
