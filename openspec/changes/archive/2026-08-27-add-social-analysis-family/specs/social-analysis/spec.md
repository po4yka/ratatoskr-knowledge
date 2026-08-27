## Purpose

Creates reproducible, provenance-grounded analysis of normalized social-source snapshots while preserving platform, capture authority, content digest, and the distinction between a post and linked material.

## ADDED Requirements

### Requirement: Captured and updated social snapshots create idempotent analysis work

Knowledge SHALL validate published social captured and updated facts, bind work to the snapshot's immutable content digest, and accept at most one active analysis identity for the same family/version/source inputs.

#### Scenario: Captured X snapshot becomes analysis work

- **WHEN** a valid captured social fixture is delivered for a normalized X post
- **THEN** Knowledge persists one receipt and queues one social analysis associated with its snapshot digest and provenance

#### Scenario: Updated snapshot supersedes without duplicate replay work

- **WHEN** a newer social snapshot is delivered and then redelivered
- **THEN** one new analysis identity exists for the newer digest and the repeated event creates neither a duplicate run nor duplicate spend

### Requirement: Social output is grounded in the post evidence

Knowledge SHALL use a versioned social prompt and context builder, validate the social schema independently, and mark linked external material as external context rather than post evidence.

#### Scenario: Social fixture produces a searchable analysis

- **WHEN** a valid social response grounds claims in the normalized post snapshot
- **THEN** its accepted analysis and tenant-scoped search projection preserve the post's source identity and content digest

#### Scenario: Unsupported social claim is rejected

- **WHEN** the provider attributes a linked article statement to the post without supplied post evidence
- **THEN** validation rejects the response and no search projection is created

### Requirement: Social analysis uses the shared budget ledger

Knowledge SHALL reserve and settle social analysis usage through the same global/family budget ledger as the other analysis families.

#### Scenario: Earlier family usage blocks social analysis

- **WHEN** the shared daily budget is exhausted by prior analysis work
- **THEN** the social event is durably deferred without a provider call
