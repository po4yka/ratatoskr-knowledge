## ADDED Requirements

### Requirement: Manifest reads follow the shared contract

Knowledge SHALL read a digest manifest from `GET /v1/manifests/<uuid>` with the owner id, digest run id and manifest digest headers defined by the channel-digest contract and SHALL accept only bytes that the contract type reproduces canonically.

#### Scenario: Request shape matches the contract

- **WHEN** the recap worker fetches a manifest from a loopback server
- **THEN** the server observes the contract path, the three contract headers, no legacy owner header and a bearer-authenticated readiness probe

### Requirement: A killed recap run resumes from its durable state

Knowledge SHALL hold a per-run advisory lock while it executes a recap run and SHALL resume a redelivered run from the persisted state without repeating a provider call whose response was already stored, keeping provider calls bounded at two per run.

#### Scenario: Redelivery after a kill mid-call completes with one new provider call

- **WHEN** the provider future is dropped while the run is in the model-requested state and the message is redelivered
- **THEN** the run completes and exactly one additional provider call is made

### Requirement: Language, manifest digest and coverage come from the system

Knowledge SHALL ask the model only for headline, overview, topics, notable items and warnings and SHALL compose the language, manifest digest, coverage and the omitted-sources warning itself before validating the stored recap.

#### Scenario: Model output without system fields is accepted

- **WHEN** the scripted provider returns only the five model-authored fields
- **THEN** the run completes and the stored result carries the run's manifest digest, language and coverage
