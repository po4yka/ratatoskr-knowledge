## ADDED Requirements

### Requirement: User-requested archive tombstones remove only named derived state

Knowledge SHALL accept `ai_archive.subject.tombstoned.v1` carrying `reason = "user_requested"`, deduplicate it by event identity, and apply the same tenant-and-subject-scoped deletion used for other authoritative reasons. The reason SHALL NOT widen scope, recreate source authority, or permit cross-tenant deletion.

#### Scenario: User-requested conversation deletion is scoped and idempotent

- **WHEN** two conversations have derived analysis, search, and embedding rows and the same valid user-requested tombstone for one conversation is delivered twice
- **THEN** every derived row for the named conversation is unavailable, the sibling remains searchable, and the second delivery creates no additional deletion effect

#### Scenario: Cross-tenant subject mismatch is refused

- **WHEN** a user-requested tombstone's owner does not match the stored source owner for its subject
- **THEN** Knowledge records no successful deletion and leaves both tenants' derived state unchanged
