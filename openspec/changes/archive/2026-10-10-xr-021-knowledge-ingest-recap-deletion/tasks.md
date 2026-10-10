## 1. Pin bump and archive digest verification

- [x] 1.1 Configuration: move every `ratatoskr-*` contracts dependency to the XR-021 contracts commit and refresh `Cargo.lock`; no failing test applies to a dependency pin, so the existing suite staying green is the check.
- [x] 1.2 RED: add `crates/knowledge/tests/source_inbox.rs::tampered_conversation_digest_is_rejected`: a conversation with edited messages and a stale `content_digest` must return `InvalidArchiveFact` and write no inbox row; run it and confirm the tampered conversation is accepted today.
- [x] 1.3 GREEN: call `verify_content_digest` in `SourceInbox::accept_ai_conversation` before accepting.

## 2. Schema additions

- [x] 2.1 RED: add `crates/knowledge/tests/schema.rs` (now `outbox_and_social_tombstone_tables_exist_with_their_constraints`, `ingest_failure_and_erasure_queue_tables_exist_with_their_constraints` and `interrupted_recap_attempts_and_deletion_counts_exist`) covering `event_outbox`, `social_tombstones`, `ingest_failures`, `blob_erasure_queue` and the `interrupted` attempt outcome; run it and confirm the tables are absent.
- [x] 2.2 GREEN: append the four tables, the `interrupted` outcome and the new `deletion_records` count columns to `schema.sql`.

## 3. Recap manifest client conforms to the contract

- [x] 3.1 RED: add `source_client_requests_the_contract_path_and_headers` and `verify_accepts_bytes_built_by_the_contract_type_and_rejects_noncanonical_bytes` to `crates/knowledge/tests/channel_digest_recap.rs` and update the fixture server to the contract path; run them and confirm the client requests the legacy path and headers.
- [x] 3.2 GREEN: delete the private manifest types, use the contract types and constants, verify linkage and update the context module and documentation.

## 4. Resumable recap pipeline

- [x] 4.1 RED: add `crates/knowledge/tests/channel_recap_pipeline_resume.rs` with the six resume and lock cases; run it and confirm redelivery in `model_requested` returns `State` forever today.
- [x] 4.2 GREEN: add the per-run advisory lock and the recovery step driven by durable state, with the interrupted attempt outcome.

## 5. Model output versus deterministic fields

- [x] 5.1 RED: add the three cases to `crates/knowledge/tests/channel_digest_recap_result.rs`; run them and confirm model output without system fields settles `invalid_output` today.
- [x] 5.2 GREEN: add `ChannelRecapModelOutput`, its schema and `compose`, switch the prompt request, the scripted provider and the evaluation corpus to the model-output shape.

## 6. Event outbox and repository terminal facts

- [x] 6.1 RED: add `crates/knowledge/tests/repository_analysis.rs::completion_fact_is_durable_in_the_outbox_when_the_request_settles`; run it and confirm no outbox row exists today.
- [x] 6.2 GREEN: add `EventOutbox` and make `complete` and `fail` write the fact in the settling transaction.

## 7. Social removal and tombstone ordering

- [x] 7.1 RED: add `crates/knowledge/tests/source_inbox.rs::social_removal_deletes_derived_state_and_blocks_older_captures`; run it and confirm there is no removal handler.
- [x] 7.2 GREEN: add `accept_social_removed`, `is_current_head` and the social tombstone check.

## 8. Document intake

- [x] 8.1 RED: add `crates/knowledge/tests/document_intake.rs::extracted_document_is_analysed_and_searchable`; run it and confirm nothing connects the event to the article pipeline.
- [x] 8.2 GREEN: add `DocumentIntake`.

## 9. Ingest configuration, readiness and provider runtime

- [x] 9.1 RED: add `crates/knowledge/tests/config.rs` (now `ingest_section_defaults_and_accepts_a_complete_configuration`, `ingest_section_refuses_unsafe_bus_and_provider_settings` and `ingest_section_refuses_unsafe_readme_source_and_limits`) and `services/knowledge/tests/admin.rs::ready_requires_ingest_when_enabled`; run them and confirm the section and readiness bits are absent.
- [x] 9.2 GREEN: add `IngestConfig`, `validate_ingest`, the lifecycle bits, the shared provider runtime and the extractive fixture provider.

## 10. GitHub README client

- [x] 10.1 RED: add `crates/knowledge/tests/github_readme.rs` with the integrity, unavailability, construction and bearer cases; run it and confirm the client is absent.
- [x] 10.2 GREEN: add `GithubReadmeClient`.

## 11. Ingest worker

- [x] 11.1 RED: add `services/knowledge/tests/ingest_bus/lifecycle.rs` with the document, social, archive, disposition and crash-resume cases against a real JetStream; run it and confirm the service starts only the recap worker.
- [x] 11.2 GREEN: add `spawn_ingest_workers`, the four supervised consumers, the handlers and dispositions, failure recording and wire it into `main.rs`.

## 12. Completion publisher

- [x] 12.1 RED: add `completion_facts_reach_the_event_stream_once` to `ingest_bus/lifecycle.rs` covering publication, restart idempotency and readiness on a broker outage; run it and confirm nothing publishes.
- [x] 12.2 GREEN: add `publish_event_outbox` to the ingest supervisor.

## 13. Blob erasure queue

- [x] 13.1 RED: add `crates/knowledge/tests/deletion_isolation.rs` cases and rewrite the old orphan-sweep test into `committed_deletion_whose_collection_crashed_is_healed_by_the_queue`; run them and confirm the sweep deletes other tenants' blobs today.
- [x] 13.2 GREEN: add `deletion_blobs.rs`, remove the sweep, rename the receipt field and job output, drain after commit and at boot.

## 14. Knowledge-owned source snapshots

- [x] 14.1 RED: add `delete_source_collects_the_knowledge_owned_snapshot_blob` to `deletion_isolation.rs`; run it and confirm the snapshot is not collected today.
- [x] 14.2 GREEN: classify `source_blob` by owner service in the scope reference collection and queue owned digests.

## 15. Inbox state deletion

- [x] 15.1 RED: add `crates/knowledge/tests/deletion_state.rs` with the five cases; run them and confirm the rows survive today.
- [x] 15.2 GREEN: add `deletion_state.rs`, extend counts, audit row and `verify_scope_empty`.

## 16. Bus identity and authorized-broker tests

- [x] 16.1 RED: add `services/knowledge/tests/ingest_bus/authorized.rs` (first written as `authorized_bus.rs`); run it and confirm it fails on the missing `deploy/nats/identity.conf`.
- [x] 16.2 GREEN: add the identity fragment, document the credentials keys and add the `admin_default_is_the_allocated_operator_port` characterization test (green today, guards the port table).

## 17. Documentation and gate

- [x] 17.1 Update `AGENTS.md`, `README.md`, `docs/ARCHITECTURE.md` section 16.2 and `docs/INTERFACES.md`; documentation has no failing test, so verify examples against the configuration parser.
- [x] 17.2 Run the `DEVELOPMENT.md` gate, `cargo deny --locked check` and `openspec validate --strict`; record the observed results.

Observed on the final tree: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line cap, `cargo test --workspace --locked` (194 tests, 0 failed, against PostgreSQL 17 with pgvector and a JetStream broker), `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --strict` all pass.

## 18. Review fixes

RED evidence for sections 1 to 17. A RED line above that says "run it and confirm X today" was observed against the pre-change code for task 1.2 only. The others were written on tests from an earlier attempt and were not re-run against the pre-change tree, so their ticks record that the tests exist and pass, not that RED was re-observed. The independent review removed one behaviour at a time in a throwaway copy and each turned the matching test red: source-scope inbox deletion made a no-op (7 and 15), the social tombstone block dropped (7), the Knowledge-owned snapshot queue skipped (14), and the context-omitted warning dropped (5). The review also found two behaviours no test caught, which tasks 18.1 and 18.2 address.

- [x] 18.1 RED: extend `ingest_bus/authorized.rs` so every durable (recap command, document, social, AI-archive conversation, repository request) receives one valid message and must show one delivery with nothing pending or unacknowledged, the process must report ready, and the five completion facts must reach the stream once. The production code already satisfied it, so there was no failing run; RED was established by mutation instead: removing from `deploy/nats/identity.conf` all grants of each of the five durables, only the acknowledgement grant of the documents durable, only the pull grant of the AI-archive durable, or the repository completed-fact publish grant each turned the test red.
- [x] 18.2 RED: add `ingest_bus/repository.rs` (completed fact once; fifth-delivery retryable failed fact; digest mismatch as a non-retryable first-delivery failure) and `ingest_bus/tenant.rs` (payload owner differing from the envelope tenant is terminated and writes nothing, for social capture, social removal, archive conversation, archive tombstone and repository request). The production code already satisfied them; RED was established by mutation: removing each of the four owner checks in `ingest_handlers.rs` turned `tenant.rs` red, and making a transient failure never terminal, marking the source-unavailable failure non-retryable, or treating an integrity failure as transient each turned `repository.rs` red.
- [x] 18.3 RED: assert `RATATOSKR__INGEST__FETCH_BATCH` defaults to 1 in `config.rs`; run it with the old default of 16 and confirm it fails.
- [x] 18.4 GREEN: default the ingest fetch batch to 1 so a delivery's ack-wait clock starts when its handler starts, and record the batch-versus-ack-wait constraint in `DEVELOPMENT.md`. This departs from the default of 16 stated in CONTRACTS.md S09.
- [x] 18.5 Refactor and configuration, no failing test applies: split the three scenario tests that carried a `too_many_lines` allow, share one process-wide inference limiter between the recap and ingest workers, take the envelope schema version from `EnvelopeSchemaVersion::CURRENT`, let the outage and authorized-broker tests take a kernel-chosen port, make `Lifecycle::is_ready` public so a test can assert readiness, and correct the stale sentence in `docs/TESTING.md`.

## 19. Independent review: repository failure classification

The second review found that a transient database error in the repository request's intake or settlement was classified as a permanent source error, so the request could be acknowledged with no terminal fact or settled as failed after a successful analysis. Unlike the sections above, these tasks have genuine failing runs against the pre-fix code.

- [x] 19.1 RED: add `ingest_bus/repository.rs` tests that make the first request insert fail inside the database, that make the first completing update fail, and that make the first five inserts fail. Run against the pre-fix code, all three failed by timing out waiting for a completed fact (the first two) or the terminal failed fact (the third), because the delivery was acknowledged or settled instead of redelivered.
- [x] 19.2 GREEN: map a database or outbox failure of the repository intake and settlement to a persistence error, keep a source error only for a request that can never be stored (`PersistenceError::is_permanent`, `RepositoryAnalysisError::is_permanent`), and have the handler record the request before it settles one, so that a request the pipeline never stored still ends in a terminal failed fact and one that can never be recorded is terminated. The three tests pass.
- [x] 19.3 No failing test applies, documentation: record in `design.md` why the event outbox needs no per-row failure state.

## 20. Independent review: analysis runs resume from durable state

The third review found that a family analysis run that failed transiently, or that a kill left in `model_requested`, could never be resumed: every redelivery hit a state-machine miss that the repository handler settled as a permanent `contract_invalid` failure, and the other lanes spent their five deliveries on it. Unlike sections 1 to 17, these tasks have genuine failing runs against the pre-fix code.

- [x] 20.1 RED: add `ingest_bus/resume.rs` with eight bus tests, one per lane (repository, social, AI archive, document) for each of two causes: a provider that fails transiently on both calls of the first delivery and answers on the second, and a worker killed while the provider call is in flight (the run is asserted to rest in `model_requested`) whose redelivery goes to a new worker. Each must end with one run at `persisted`, one accepted output, one search document, no `ingest_failures` row, and for the repository lane one completed fact, no failed fact and a request at `completed`. Run against the pre-fix code: seven failed by timing out waiting for the analysis to become searchable; the document lane killed mid-call passed (its pipeline ignored the failed transitions), so it is a regression guard only.
- [x] 20.2 GREEN: replace the two copies of the attempt loop with one resumable `RunDriver` (`run_driver.rs`) used by `ArticlePipeline` and `FamilyPipeline`. It runs under a per-run advisory lock (the lock moved from the recap pipeline to `run_lock.rs`), returns the accepted output of a `persisted`, `indexed` or `completed` run, reopens a `failed` run whose last call failed transiently, records an unfinished call as `interrupted` and continues, fails a run whose calls were abandoned twice, and validates a stored response again without a provider call. A state-machine miss is now `Conflict` (transient), no longer `Source`. The per-run attempt ceiling is ten (two calls per delivery, five deliveries); `schema.sql` states it and gains the `interrupted` outcome. All eight bus tests pass.
- [x] 20.3 RED and GREEN together, observed passing only: add `crates/knowledge/tests/pipeline_resume.rs` (reopen after transient failure, the attempt ceiling, interrupted call, stored valid response from `response_received` and `schema_validated`, stored invalid response repaired once, a held run reports a conflict) and the `ingest_handlers` unit test that only `Source` is a permanent contract failure. They were written with the driver, so no failing run was observed for them; the bus tests of 20.1 carry the RED evidence.
- [x] 20.4 No failing test applies, documentation: record the resume rules in `DEVELOPMENT.md` and `docs/INTERFACES.md`, together with the `FETCH_BATCH` default of 1, and correct the `nats-server` extraction step of `ci.yml` (the pinned broker image is alpine and keeps the binary in `/usr/local/bin`, so the step asks the running container for the path).

## 21. Independent review: crash between response and state change, repository settlement, spec

The fourth review found one more state-machine miss, a repository settlement that could be terminated without a fact, and a missing requirement for the resume behaviour.

- [x] 21.1 RED: add two `pipeline_resume.rs` tests that seed a run at `model_requested` whose newest attempt holds a stored response (valid, and invalid) with an empty provider; both failed with `Conflict` against the pre-fix driver.
- [x] 21.2 GREEN: move such a run to `response_received` before the state dispatch in `RunDriver::drive_locked`, so the existing stored-response path validates the bytes and repairs or persists without a provider call. Both tests pass.
- [x] 21.3 RED: add a bus test in `ingest_bus/repository.rs` that fails the settlement write on the fifth delivery of an unavailable README; it failed by timing out waiting for the failed fact, because the generic fifth-delivery rule terminated the delivery into `ingest_failures`.
- [x] 21.4 GREEN: give a failed settlement its own `Outcome::Redeliver`, which is always negative-acknowledged and never reaches the `ingest_failures` rule, so the request ends in its terminal failed fact. The test passes with six deliveries.
- [x] 21.5 No failing test applies, specification and documentation: add the requirement for resuming a redelivered analysis run to `knowledge-ingest/spec.md` and the change delta, and record in `design.md` the decisions on a killed repair call and on spend-budget refusals.
