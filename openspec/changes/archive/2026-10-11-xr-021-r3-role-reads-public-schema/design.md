## Context

XR-021 CONTRACTS.md R3-11 rule 2 is the contract for the provisioning file. This file records only the correction local to Knowledge.

## Decisions

### USAGE on public, never CREATE

The `vector` type, its operators and the `hnsw` or `ivfflat` operator classes are created by `create extension vector` in schema `public`, and `schema.sql` refers to them unqualified. The service role therefore needs USAGE on `public`. It does not need CREATE there: every object the service owns lives in the `knowledge` schema, which the role creates itself through `create` on the database. A grant of USAGE only keeps the role unable to add objects to `public`.

The file keeps no `revoke all on schema public from ratatoskr_knowledge` line: the role has no direct grant to revoke, and the `public` pseudo-role's rights are Platform's to set.

### Evidence

The test is a substring check and cannot prove the schema applies, so the real sequence was run once by hand against PostgreSQL 17 with pgvector (database `ratatoskr`, `revoke all on schema public from public`, `role.sql` as a superuser, `schema.sql` as `ratatoskr_knowledge`). Before the change it fails with `type "vector" does not exist`; after it, it applies and a second application is a no-op.
