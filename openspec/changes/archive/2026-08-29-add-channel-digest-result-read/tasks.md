## 1. Result-reader authority

- [x] 1.1 RED: add `crates/knowledge/tests/config.rs::channel_recap_result_reader_secret_is_redacted_and_bounded` covering absent/empty/oversized secret, role applicability, and `Debug`/effective-config redaction; run it and confirm the result-reader configuration is absent.
- [x] 1.2 GREEN: add the dedicated redacted result-reader secret and strict validation, then rerun the focused configuration test.

## 2. Integrity-checked result projection

- [x] 2.1 RED: add `crates/knowledge/tests/channel_digest_recap.rs::completed_recap_result_reads_are_scoped_and_integrity_checked` covering completed/absent/non-recap/incomplete/corrupt rows, exact digest, and forbidden field absence; run it and confirm no owned read method exists.
- [x] 2.2 GREEN: implement the bounded typed result-store read and digest/type revalidation, then rerun the focused PostgreSQL test.

## 3. Authenticated loopback HTTP surface

- [x] 3.1 RED: add `services/knowledge/tests/channel_digest_results.rs::result_route_requires_service_auth_and_returns_only_the_typed_recap` covering missing/wrong/correct bearer, constant response classes, no-store, malformed UUID, finite body, scoped 404, storage/integrity failure, and content-free diagnostics; run it and confirm the fixed route is absent.
- [x] 3.2 GREEN: add the fixed result route, constant-time service authentication, explicit DTO, safe failure mapping, and response bound; rerun the focused route matrix.
- [x] 3.3 RED: extend `services/knowledge/tests/boot.rs` with result-reader disabled/enabled startup, readiness, restart, and bounded drain cases; run it and confirm production composition does not expose or supervise the route correctly.
- [x] 3.4 GREEN: wire the result reader into lifecycle/config composition and rerun the boot matrix.

## 4. Documentation and gate

- [x] 4.1 Update interface, configuration, deployment, testing, security, and rotation documentation with the exact loopback route and separate-secret boundary; no independent failing test applies to prose, so verify examples against the production config parser and route constants.
- [x] 4.2 Run the exact `DEVELOPMENT.md` gate through `build-gate` where compiler-backed, real PostgreSQL tests, strict active OpenSpec validation, privacy/secret audit, and `git diff --check`; record the observed results before rollout.
