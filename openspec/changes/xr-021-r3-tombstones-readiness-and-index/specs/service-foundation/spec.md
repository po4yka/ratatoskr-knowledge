## ADDED Requirements

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
