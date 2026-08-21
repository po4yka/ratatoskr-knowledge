## Purpose

Defines the finite process, configuration, operator, persistence, and diagnostic controls required
before the first Knowledge analysis can run.

## ADDED Requirements

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
