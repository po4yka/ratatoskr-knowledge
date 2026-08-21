## 1. Service foundation

- [x] 1.1 Add failing test `crates/knowledge/tests/config.rs::defaults_are_finite_and_security_cannot_be_disabled`; assert loopback admin, finite database/provider/context/raw-response/shutdown/blob limits, and no policy-disable field. Run it and confirm the scaffold defaults do not satisfy the assertions.
- [x] 1.2 Create the two-package Rust workspace, strict lint/toolchain files, typed defaults, and crate safety attributes; make 1.1 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 1.3 Add failing test `crates/knowledge/tests/config.rs::invalid_environment_is_reported_without_its_value`; use an unknown key and a wrong value containing `LEAKME`, and assert both fail while no diagnostic contains `LEAKME`. Run it and confirm one value is accepted or exposed.
- [x] 1.4 Implement the strict `RATATOSKR__` loader and semantic validation, make 1.3 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 1.5 Add failing test `services/knowledge/tests/admin.rs::readiness_follows_storage_startup_and_drain`; assert liveness stays 200, readiness is 503/200/503, metrics and version respond, and all responses use `Cache-Control: no-store`. Run it and confirm the scaffold routes fail.
- [x] 1.6 Implement the admin router and atomic lifecycle state, make 1.5 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 1.7 Add failing real-PostgreSQL test `crates/knowledge/tests/schema.rs::owned_schema_applies_twice_without_cross_schema_objects`; assert the four planned tables, constraints, and idempotent second apply on PostgreSQL 17. Run it and confirm the schema is absent.
- [x] 1.8 Add the finite SQLx pool, editable `schema.sql`, and disposable-database harness with no migration tooling; make 1.7 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 1.9 Add failing test `crates/knowledge/tests/telemetry.rs::validation_telemetry_excludes_source_and_response_text`; record a failure containing `LEAKME` and assert only the bounded class is captured. Run it and confirm the safe event is absent or content leaks.
- [x] 1.10 Implement one telemetry bootstrap and closed analysis fields, make 1.9 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 1.11 Add vetted exact dependencies, keep `schemars = 1.2.2`, generate and inspect `Cargo.lock`, add CI and the exact matching `DEVELOPMENT.md` command list. No test: build and policy files; verify with `cargo metadata --locked`, `cargo deny --locked check`, and the command-list drift check, then commit atomically on `main`.

## 2. Source references and analysis runs

- [x] 2.1 Add failing PostgreSQL test `crates/knowledge/tests/runs.rs::changed_source_digest_creates_an_immutable_revision`; store one document identity with two digests and assert both source revisions remain. Run it and confirm no source API exists.
- [x] 2.2 Implement bounded source references and revision persistence, make 2.1 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 2.3 Add failing PostgreSQL test `crates/knowledge/tests/runs.rs::complete_analysis_identity_is_idempotent`; create the same tenant/source/contract/prompt/context/policy identity twice and assert one run ID. Run it and confirm duplicate work is possible.
- [x] 2.4 Implement the natural run key and idempotent creation, make 2.3 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 2.5 Add failing PostgreSQL test `crates/knowledge/tests/runs.rs::terminal_state_cannot_regress`; table every legal transition and assert illegal skips and completed-to-active transitions affect no row. Run it and confirm state can regress.
- [x] 2.6 Implement typed states and expected-state SQL transitions, make 2.5 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 2.7 Add failing PostgreSQL test `crates/knowledge/tests/runs.rs::attempt_ordinals_and_reasons_are_durable`; record initial and repair attempts and assert increasing unique ordinals plus safe metadata. Run it and confirm attempts cannot be stored.
- [x] 2.8 Implement attempt persistence and bounded vocabularies, make 2.7 pass, run format and Clippy, and commit this TDD pair on `main`.

## 3. Article contract and context

- [x] 3.1 Add failing test `crates/knowledge/tests/article.rs::schema_rejects_unknown_and_out_of_bounds_fields`; table unknown fields, empty and oversized summaries, key-point counts, text lengths, and citation counts. Run it and confirm at least one invalid value is accepted.
- [x] 3.2 Implement the strict typed article result, exact `schemars 1.2.2` schema generation, schema-first validation, and value limits; make 3.1 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 3.3 Add failing test `crates/knowledge/tests/article.rs::citations_must_name_supplied_unique_blocks`; assert missing, duplicate, and omitted block indexes fail after structural validation. Run it and confirm an invalid citation passes.
- [x] 3.4 Implement semantic citation validation against the exact prepared context, make 3.3 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 3.5 Add failing test `crates/knowledge/tests/context.rs::builder_is_deterministic_and_omits_only_complete_tail_blocks`; build twice under a small budget and assert byte equality, ordered included indexes, complete omitted indexes, and no partial block text. Run it and confirm the builder is absent.
- [x] 3.6 Implement version-one context preparation for supported Document IR blocks, make 3.5 pass, run format and Clippy, and commit this TDD pair on `main`.
- [x] 3.7 Add failing test `crates/knowledge/tests/context.rs::source_instructions_cannot_replace_fixed_policy`; include command-like source text and assert fixed policy, task, schema, and source remain distinct request fields. Run it and confirm no bounded request exists.
- [x] 3.8 Implement the versioned prompt request and untrusted-source boundary, make 3.7 pass, run format and Clippy, and commit this TDD pair on `main`.

## 4. Fake-provider pipeline

- [ ] 4.1 Add failing test `crates/knowledge/tests/provider.rs::fake_provider_consumes_scripts_and_records_requests`; script two outcomes and assert ordered responses and captured bounded requests. Run it and confirm no fake boundary exists.
- [ ] 4.2 Implement the narrow provider trait and hand-written scripted fake without a mocking crate, make 4.1 pass, run format and Clippy, and commit this TDD pair on `main`.
- [ ] 4.3 Add failing PostgreSQL test `crates/knowledge/tests/pipeline.rs::malformed_response_is_stored_before_json_validation`; return malformed JSON and assert a Knowledge-owned digest-matching `BlobRef` and validation outcome exist while telemetry contains no raw text. Run it and confirm no blob is stored.
- [ ] 4.4 Implement bounded content-addressed raw-response storage and attempt linkage before parsing, make 4.3 pass, run format and Clippy, and commit this TDD pair on `main`.
- [ ] 4.5 Add failing PostgreSQL test `crates/knowledge/tests/pipeline.rs::one_transient_failure_retries_once`; script transient failure then valid output and assert exactly two attempts and completion. Run it and confirm retry is absent.
- [ ] 4.6 Implement the finite provider timeout and one-extra-attempt transient classifier, make 4.5 pass, run format and Clippy, and commit this TDD pair on `main`.
- [ ] 4.7 Add failing PostgreSQL test `crates/knowledge/tests/pipeline.rs::one_invalid_response_repairs_once`; script invalid then valid output and assert two raw blobs, repair reason, accepted result, and completion. Run it and confirm repair is absent.
- [ ] 4.8 Implement bounded repair using validation codes and no source-policy mutation, make 4.7 pass, run format and Clippy, and commit this TDD pair on `main`.
- [ ] 4.9 Add failing PostgreSQL test `crates/knowledge/tests/pipeline.rs::second_invalid_response_fails_without_a_third_call`; script two invalid responses and assert failed state, two attempts, no output, and no third call. Run it and confirm the call budget is not enforced.
- [ ] 4.10 Enforce the shared two-call retry-or-repair budget and permanent failure rules, make 4.9 pass, run format and Clippy, and commit this TDD pair on `main`.
- [ ] 4.11 Add failing PostgreSQL test `crates/knowledge/tests/pipeline.rs::completed_replay_returns_one_atomic_result_without_provider_call`; replay one complete identity and assert one output, completed state, and unchanged call count. Run it and confirm another call or output occurs.
- [ ] 4.12 Commit accepted output with the persisted transition and resume persisted runs without a provider call, make 4.11 pass, run format and Clippy, and commit this TDD pair on `main`.

## 5. Process and final gates

- [ ] 5.1 Add failing test `services/knowledge/tests/boot.rs::configured_process_serves_admin_without_inference_credentials`; start the real binary with disposable storage, assert `check-config` does not bind, readiness arrives, only admin routes exist, and termination is bounded. Run it and confirm readiness never arrives.
- [ ] 5.2 Wire configuration, storage, PostgreSQL, telemetry, admin listener, and joined shutdown without a public analysis endpoint; make 5.1 pass, run format and Clippy, and commit this TDD pair on `main`.
- [ ] 5.3 Update README, data model, interfaces, testing, deployment notes, and mark implementation-plan items 1 through 4 complete while replacing the obsolete migration wording with editable `schema.sql`. No test: documentation; verify every statement against built code and commit atomically on `main`.
- [ ] 5.4 Run the exact `DEVELOPMENT.md` gate order, real PostgreSQL tests, schema and prompt-schema drift checks, `openspec validate build-first-article-analysis --strict`, source-length, dependency, forbidden-panic, secret, cross-schema, and external-request audits. Push `main` only after all checks pass and verify the remote SHA and GitHub Actions.
