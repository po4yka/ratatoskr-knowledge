## 1. Inbox prefix (XR-021 CONTRACTS.md R3-01)

- [x] 1.1 RED: add `services/knowledge/tests/ingest_bus/authorized.rs::knowledge_workers_complete_jetstream_requests_under_their_inbox_prefix` and change `deploy/nats/identity.conf` to allow `_INBOX_knowledge.>` only; run it and confirm both workers never become ready because their requests use the default prefix.
- [x] 1.2 GREEN: set `custom_inbox_prefix` on both branches of `bus::connect`.

## 2. Tombstones (R3-08)

- [x] 2.1 RED: add `services/knowledge/tests/ingest_bus/tombstone.rs` with the six scenarios; run them and confirm the dead letter after five deliveries, the duplicate that acknowledges without draining, the foreign subject that is redelivered six times and the unsupported type logged under the old class.
- [x] 2.2 GREEN: classify the archive handler outcomes as designed.

## 3. Recap reachability (R3-09)

- [x] 3.1 RED: add the four readiness scenarios to `services/knowledge/tests/channel_recap_source.rs`; run them and confirm `/ready` is 503 while the source is down and the warning repeats each second.
- [x] 3.2 GREEN: verify the durable before the probe, add the gauge, back off and log the transition only.

## 4. Ack-wait validation (R3-09)

- [ ] 4.1 RED: add the three scenarios to `crates/knowledge/tests/config.rs`; run them and confirm any value from 1 to 600 loads today.
- [ ] 4.2 GREEN: validate against the shared constants and update the example and the README.

## 5. Conversation text (R3-09)

- [ ] 5.1 RED: add `crates/knowledge/tests/family_pipeline.rs::a_conversation_is_indexed_without_prompt_scaffolding`; run it and confirm the scaffold tokens are searchable.
- [ ] 5.2 GREEN: build the search fields from the title and the message text.

## 6. Provisioning and documentation (R3-11, R3-19)

- [ ] 6.1 RED: add `services/knowledge/tests/deployment_profile.rs::the_example_names_the_shared_database_and_the_role_file_exists`; run it and confirm the example names another database and the role file is absent.
- [ ] 6.2 GREEN: add `deploy/postgres/role.sql`, the PostgreSQL paragraphs and the README rewrite. Documentation and provisioning files cannot start from a failing implementation test; the loader test in 6.1 is the check.
