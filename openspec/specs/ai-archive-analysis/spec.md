# ai-archive-analysis Specification

## Purpose

Creates privacy-bounded, versioned interpretation of immutable ChatGPT and Claude archive conversation/message evidence while preserving the exact graph node and revision that grounds each result.

## Requirements

### Requirement: Archive facts create revision-bound analysis work

Knowledge SHALL validate supported AI-archive events and create analysis identities bound to the provider, archive/conversation/message identifiers, parser revision, and immutable content digest supplied by the contract.

#### Scenario: Conversation fixture produces one archive-item analysis

- **WHEN** a valid ChatGPT or Claude conversation event fixture is delivered
- **THEN** Knowledge retains one inbox receipt and queues one analysis whose provenance names the exact conversation or message revision

#### Scenario: Replayed or out-of-order archive fact is safe

- **WHEN** an archive event is redelivered or an older graph revision arrives after a newer one
- **THEN** no duplicate analysis or budget charge occurs and the searchable current projection does not regress

### Requirement: Archive output is isolated and independently validated

Knowledge SHALL use an archive-specific versioned prompt/context builder and schema, expose only the selected graph scope to the provider, and reject output whose citations escape that scope.

#### Scenario: Archive fixture yields a searchable scoped result

- **WHEN** a provider response cites only the supplied conversation/message evidence
- **THEN** the accepted result is projected for the owning tenant with the originating provider and graph revision

#### Scenario: Cross-conversation claim is rejected

- **WHEN** output claims evidence from a message outside the selected archive item
- **THEN** validation records the failure and creates no accepted result or search document

### Requirement: Archive analysis is budget-governed with other families

Knowledge SHALL account for archive analysis usage in the shared ledger before requesting a provider.

#### Scenario: Shared limit prevents archive request

- **WHEN** global or archive daily spend is exhausted
- **THEN** the archive item remains durably deferred and no provider call is made

### Requirement: User-requested archive tombstones remove only named derived state

Knowledge SHALL accept `ai_archive.subject.tombstoned.v1` carrying `reason = "user_requested"`, deduplicate it by event identity, and apply the same tenant-and-subject-scoped deletion used for other authoritative reasons. The reason SHALL NOT widen scope, recreate source authority, or permit cross-tenant deletion.

#### Scenario: User-requested conversation deletion is scoped and idempotent

- **WHEN** two conversations have derived analysis, search, and embedding rows and the same valid user-requested tombstone for one conversation is delivered twice
- **THEN** every derived row for the named conversation is unavailable, the sibling remains searchable, and the second delivery creates no additional deletion effect

#### Scenario: Cross-tenant subject mismatch is refused

- **WHEN** a user-requested tombstone's owner does not match the stored source owner for its subject
- **THEN** Knowledge records no successful deletion and leaves both tenants' derived state unchanged
