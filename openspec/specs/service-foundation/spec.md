# Service Foundation Specification

## Purpose

Defines the finite process, configuration, operator, persistence, and diagnostic controls required
before the first Knowledge analysis can run.

## Requirements

### Requirement: Configuration is typed and finite

Knowledge SHALL load one typed configuration tree from built-in defaults and `RATATOSKR__`
environment variables. It SHALL reject unknown keys and invalid values without reporting supplied
values. Database acquisition, provider execution, source context, raw response, shutdown, and owned
blob limits SHALL be finite.

#### Scenario: invalid configuration contains a secret value

- **WHEN** an environment value is invalid and contains sensitive text
- **THEN** startup fails before binding and the report names the key and rule without the value

### Requirement: The operator plane follows process state

Knowledge SHALL expose liveness, readiness, metrics, and build identity on a loopback admin listener.
Readiness SHALL fail before owned storage and PostgreSQL are ready and after drain starts. Admin
responses SHALL prohibit caching.

#### Scenario: the process starts and drains

- **WHEN** the admin routes are read before startup, after startup, and after shutdown begins
- **THEN** liveness remains successful and readiness changes from unavailable to available to
  unavailable

### Requirement: Knowledge owns one editable schema definition

Knowledge SHALL create only `knowledge.*` objects from one idempotent schema definition. It SHALL use
a finite PostgreSQL pool and SHALL NOT include migration files, migration tooling, cross-schema writes,
or foreign keys to another service.

#### Scenario: a disposable database is initialized twice

- **WHEN** the schema definition is applied twice to a fresh PostgreSQL 17 database
- **THEN** the required Knowledge tables and constraints exist once and no non-Knowledge schema changed

### Requirement: Telemetry excludes source and model content

Knowledge SHALL emit bounded operation names, states, outcomes, durations, attempt counts, and safe
identifiers. It SHALL NOT emit prompt text, source blocks, raw responses, credentials, database URLs,
or owned filesystem paths.

#### Scenario: validation fails on private text

- **WHEN** a raw provider response containing sensitive text fails validation
- **THEN** captured telemetry names the validation class and contains none of the response text

### Requirement: The API listener serves the library surface and the operator listener does not

Knowledge SHALL serve `GET /v1/capabilities`, `GET /internal/search`, `POST /internal/user-content/command`, `GET /internal/user-content/collection` and, only when the result reader secret is configured, the channel-digest result route on a dedicated loopback API listener (`RATATOSKR__API__LISTEN_ADDRESS`, default `127.0.0.1:8091`) that differs from the operator listener, and SHALL serve only `/live`, `/ready`, `/metrics` and `/version` on the operator listener. Readiness SHALL require both listeners bound.

#### Scenario: Routes are split between the two listeners

- **WHEN** the real service is started on two ephemeral loopback ports
- **THEN** the capability document, search and user-content routes answer on the API port, `/metrics` and `/ready` answer 404 there, the same library routes answer 404 on the operator port, and `/ready` is 503 until both listeners are bound

#### Scenario: Unsafe API addresses are refused

- **WHEN** the API address is non-loopback or equals the operator address
- **THEN** configuration loading fails naming `RATATOSKR__API__LISTEN_ADDRESS`

### Requirement: Digest source reachability does not gate readiness

With the channel recap enabled, `/ready` SHALL depend on the recap worker's bus connection and durable and SHALL NOT depend on the reachability of the digest source. The reachability SHALL be reported by the gauge `knowledge_channel_recap_source_up`, the recap worker SHALL NOT pull commands while the source is down, and each change of that state SHALL be logged once.

#### Scenario: The source is down

- **WHEN** the recap is enabled, the bus and durable are fine and nothing listens at the digest source URL
- **THEN** `/ready` is 200, the gauge is 0 and the recap command stays on the durable unconsumed

#### Scenario: The source appears

- **WHEN** the source starts listening
- **THEN** the gauge becomes 1 and the queued command is processed

### Requirement: Ack waits state the values Edge fixes

`RATATOSKR__INGEST__ACK_WAIT_SECONDS` SHALL load only as 120 and `RATATOSKR__CHANNEL_RECAP__ACK_WAIT_SECONDS` only as 30, and a refusal SHALL name the key, the required value and the Edge durable table.

#### Scenario: A plausible edit is refused

- **WHEN** the ingest ack wait is set to 60
- **THEN** configuration loading fails naming the key and 120

### Requirement: The database role is provisioned by a checked-in file

The repository SHALL ship `deploy/postgres/role.sql` that creates the `ratatoskr_knowledge` role without superuser rights, grants it connect and create on the `ratatoskr` database and installs the `vector` extension, and the shipped example SHALL name that database and role.

#### Scenario: The example and the role file agree

- **WHEN** the example is loaded
- **THEN** its database URL names `ratatoskr_knowledge` on database `ratatoskr` and the role file creates that role
