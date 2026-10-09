## 1. Pin bump and archive digest verification

- [ ] 1.1 Configuration: move every `ratatoskr-*` contracts dependency to the XR-021 contracts commit and refresh `Cargo.lock`; no failing test applies to a dependency pin, so the existing suite staying green is the check.
- [ ] 1.2 RED: add `crates/knowledge/tests/source_inbox.rs::tampered_conversation_digest_is_rejected`: a conversation with edited messages and a stale `content_digest` must return `InvalidArchiveFact` and write no inbox row; run it and confirm the tampered conversation is accepted today.
- [ ] 1.3 GREEN: call `verify_content_digest` in `SourceInbox::accept_ai_conversation` before accepting.

## 2. Schema additions

- [ ] 2.1 RED: add `crates/knowledge/tests/schema.rs::ingest_tables_exist_with_their_constraints` covering `event_outbox`, `social_tombstones`, `ingest_failures`, `blob_erasure_queue` and the `interrupted` attempt outcome; run it and confirm the tables are absent.
- [ ] 2.2 GREEN: append the four tables, the `interrupted` outcome and the new `deletion_records` count columns to `schema.sql`.

## 3. Recap manifest client conforms to the contract

- [ ] 3.1 RED: add `source_client_requests_the_contract_path_and_headers` and `verify_accepts_bytes_built_by_the_contract_type_and_rejects_noncanonical_bytes` to `crates/knowledge/tests/channel_digest_recap.rs` and update the fixture server to the contract path; run them and confirm the client requests the legacy path and headers.
- [ ] 3.2 GREEN: delete the private manifest types, use the contract types and constants, verify linkage and update the context module and documentation.

## 4. Resumable recap pipeline

- [ ] 4.1 RED: add `crates/knowledge/tests/channel_recap_pipeline_resume.rs` with the six resume and lock cases; run it and confirm redelivery in `model_requested` returns `State` forever today.
- [ ] 4.2 GREEN: add the per-run advisory lock and the recovery step driven by durable state, with the interrupted attempt outcome.

## 5. Model output versus deterministic fields

- [ ] 5.1 RED: add the three cases to `crates/knowledge/tests/channel_digest_recap_result.rs`; run them and confirm model output without system fields settles `invalid_output` today.
- [ ] 5.2 GREEN: add `ChannelRecapModelOutput`, its schema and `compose`, switch the prompt request, the scripted provider and the evaluation corpus to the model-output shape.

## 6. Event outbox and repository terminal facts

- [ ] 6.1 RED: add `crates/knowledge/tests/repository_analysis.rs::completion_fact_is_durable_in_the_outbox_when_the_request_settles`; run it and confirm no outbox row exists today.
- [ ] 6.2 GREEN: add `EventOutbox` and make `complete` and `fail` write the fact in the settling transaction.

## 7. Social removal and tombstone ordering

- [ ] 7.1 RED: add `crates/knowledge/tests/source_inbox.rs::social_removal_deletes_derived_state_and_blocks_older_captures`; run it and confirm there is no removal handler.
- [ ] 7.2 GREEN: add `accept_social_removed`, `is_current_head` and the social tombstone check.

## 8. Document intake

- [ ] 8.1 RED: add `crates/knowledge/tests/document_intake.rs::extracted_document_is_analysed_and_searchable`; run it and confirm nothing connects the event to the article pipeline.
- [ ] 8.2 GREEN: add `DocumentIntake`.

## 9. Ingest configuration, readiness and provider runtime

- [ ] 9.1 RED: add `crates/knowledge/tests/config.rs::ingest_section_is_validated` and `services/knowledge/tests/admin.rs::ready_requires_ingest_when_enabled`; run them and confirm the section and readiness bits are absent.
- [ ] 9.2 GREEN: add `IngestConfig`, `validate_ingest`, the lifecycle bits, the shared provider runtime and the extractive fixture provider.

## 10. GitHub README client

- [ ] 10.1 RED: add `crates/knowledge/tests/github_readme.rs` with the integrity, unavailability, construction and bearer cases; run it and confirm the client is absent.
- [ ] 10.2 GREEN: add `GithubReadmeClient`.

## 11. Ingest worker

- [ ] 11.1 RED: add `services/knowledge/tests/ingest_bus.rs` with the document, social, archive, disposition and crash-resume cases against a real JetStream; run it and confirm the service starts only the recap worker.
- [ ] 11.2 GREEN: add `spawn_ingest_workers`, the four supervised consumers, the handlers and dispositions, failure recording and wire it into `main.rs`.

## 12. Completion publisher

- [ ] 12.1 RED: add `completion_facts_reach_the_event_stream_once` to `ingest_bus.rs` covering publication, restart idempotency and readiness on a broker outage; run it and confirm nothing publishes.
- [ ] 12.2 GREEN: add `publish_event_outbox` to the ingest supervisor.

## 13. Blob erasure queue

- [ ] 13.1 RED: add `crates/knowledge/tests/deletion_isolation.rs` cases and rewrite the old orphan-sweep test into `committed_deletion_whose_collection_crashed_is_healed_by_the_queue`; run them and confirm the sweep deletes other tenants' blobs today.
- [ ] 13.2 GREEN: add `deletion_blobs.rs`, remove the sweep, rename the receipt field and job output, drain after commit and at boot.

## 14. Knowledge-owned source snapshots

- [ ] 14.1 RED: add `delete_source_collects_the_knowledge_owned_snapshot_blob` to `deletion_isolation.rs`; run it and confirm the snapshot is not collected today.
- [ ] 14.2 GREEN: classify `source_blob` by owner service in the scope reference collection and queue owned digests.

## 15. Inbox state deletion

- [ ] 15.1 RED: add `crates/knowledge/tests/deletion_state.rs` with the five cases; run them and confirm the rows survive today.
- [ ] 15.2 GREEN: add `deletion_state.rs`, extend counts, audit row and `verify_scope_empty`.

## 16. Bus identity and authorized-broker tests

- [ ] 16.1 RED: add `services/knowledge/tests/authorized_bus.rs`; run it and confirm it fails on the missing `deploy/nats/identity.conf`.
- [ ] 16.2 GREEN: add the identity fragment, document the credentials keys and add the `admin_default_is_the_allocated_operator_port` characterization test (green today, guards the port table).

## 17. Documentation and gate

- [ ] 17.1 Update `AGENTS.md`, `README.md`, `docs/ARCHITECTURE.md` section 16.2 and `docs/INTERFACES.md`; documentation has no failing test, so verify examples against the configuration parser.
- [ ] 17.2 Run the `DEVELOPMENT.md` gate, `cargo deny --locked check` and `openspec validate --strict`; record the observed results.
