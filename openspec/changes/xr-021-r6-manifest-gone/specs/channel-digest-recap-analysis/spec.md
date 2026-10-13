## MODIFIED Requirements

### Requirement: Manifest reads follow the shared contract

Knowledge SHALL read a digest manifest from `GET /v1/manifests/<uuid>` with the owner id, digest run id and manifest digest headers defined by the channel-digest contract and SHALL accept only bytes that the contract type reproduces canonically. A `410 Gone` answer SHALL be a terminal manifest failure: the recap request fails as `manifest_unavailable` at once and no retry is scheduled.

#### Scenario: Request shape matches the contract

- **WHEN** the recap worker fetches a manifest from a loopback server
- **THEN** the server observes the contract path, the three contract headers, no legacy owner header and a bearer-authenticated readiness probe

#### Scenario: A gone manifest is not retried

- **WHEN** the manifest route answers `410`
- **THEN** the first attempt settles the run as failed with `manifest_unavailable`, the recap failure fact is queued once and no manifest retry is scheduled

#### Scenario: An unavailable manifest still gets its one retry

- **WHEN** the manifest route answers `503` on the first attempt
- **THEN** one restart-safe retry is scheduled
