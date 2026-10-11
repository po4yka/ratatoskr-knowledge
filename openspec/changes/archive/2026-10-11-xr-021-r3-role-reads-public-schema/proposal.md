## Why

The Round 3 provisioning file `deploy/postgres/role.sql` installs pgvector into schema `public` and then revokes every right on `public` from the service role. Platform's `01-database-and-roles.sql` already runs `revoke all on schema public from public`, so the service role has no USAGE on `public` and cannot resolve the `vector` type: applying `schema.sql` as `ratatoskr_knowledge` fails with `type "vector" does not exist` at the first start. Reproduced in a throwaway PostgreSQL 17 with the real sequence (create database, revoke from public, `role.sql` as superuser, `schema.sql` as the role). The extractor-style `revoke all on schema public` line copied from XR-021 CONTRACTS.md R3-11 rule 2 is right for the extractor and insufficient for Knowledge, the one service that needs an object in `public`.

## What Changes

- `deploy/postgres/role.sql` grants `usage on schema public` (never `create`) to `ratatoskr_knowledge` instead of revoking, with a comment that says why.
- The deployment profile test requires that grant and refuses `create` on `public`.
- The README, `deploy/README.md` and the example comment say what the file does, including the schema grant.
- No behaviour of the running service changes. No new contract version, no migration file, no compatibility shim.

## Capabilities

### Modified Capabilities

- `service-foundation`: the role provisioning requirement includes USAGE on schema `public`, so the schema applies as the service role.

## Impact

Affected surfaces are `deploy/postgres/role.sql`, the deployment profile test, `deploy/README.md`, `README.md`, `DEVELOPMENT.md` and the example comment. XR-021 CONTRACTS.md R3-11 rule 2 needs the same correction for Knowledge; that is named in the handoff.
