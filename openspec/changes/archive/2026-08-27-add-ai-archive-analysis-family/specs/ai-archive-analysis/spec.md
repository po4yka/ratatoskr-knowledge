## Purpose

Creates privacy-bounded, versioned interpretation of immutable ChatGPT and Claude archive conversation/message evidence while preserving the exact graph node and revision that grounds each result.

## ADDED Requirements

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

Knowledge SHALL reserve and settle archive analysis usage in the shared ledger before requesting a provider.

#### Scenario: Shared limit prevents archive request

- **WHEN** global or archive daily spend is exhausted
- **THEN** the archive item remains durably deferred and no provider call is made
