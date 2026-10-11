## MODIFIED Requirements

### Requirement: The database role is provisioned by a checked-in file

The repository SHALL ship `deploy/postgres/role.sql` that creates the `ratatoskr_knowledge` role without superuser rights, grants it connect and create on the `ratatoskr` database and usage (never create) on schema `public`, and installs the `vector` extension, and the shipped example SHALL name that database and role.

#### Scenario: The example and the role file agree

- **WHEN** the example is loaded
- **THEN** its database URL names `ratatoskr_knowledge` on database `ratatoskr` and the role file creates that role

#### Scenario: The schema applies as the service role

- **WHEN** schema `public` has had every right revoked from `public`, the role file has run as an administrator and `schema.sql` is applied as `ratatoskr_knowledge`
- **THEN** the `vector` type resolves and the schema applies
