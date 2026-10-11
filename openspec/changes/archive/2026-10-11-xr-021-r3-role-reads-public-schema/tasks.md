## 1. Schema usage for the service role (XR-021 CONTRACTS.md R3-11)

- [x] 1.1 RED: extend `services/knowledge/tests/deployment_profile.rs::the_example_names_the_shared_database_and_the_role_file_exists` to require `grant usage on schema public to ratatoskr_knowledge` and to refuse `create` on `public`; run it and confirm it fails because the grant is absent, and reproduce `type "vector" does not exist` with the real sequence against a private PostgreSQL 17.
- [x] 1.2 GREEN: replace the revoke in `deploy/postgres/role.sql` with the usage grant and a comment saying why; run the test, then the real sequence, and confirm `schema.sql` applies as the role twice.
- [x] 1.3 Documentation (no test, prose only): `README.md`, `deploy/README.md`, `DEVELOPMENT.md` and the example comment state the schema grant.
