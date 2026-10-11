## 1. Dedicated API listener (XR-021 CONTRACTS.md R2-01)

- [ ] 1.1 RED: add `crates/knowledge/tests/config.rs::api_listener_defaults_and_validation`, `services/knowledge/tests/admin.rs::operator_plane_serves_only_health_metrics_and_version` and `services/knowledge/tests/api_listener.rs`; run them and confirm the API key is unknown, the operator router still serves the library routes and the real service binds one listener.
- [ ] 1.2 GREEN: add `ApiConfig`, split `admin_router` and `api_router`, bind both in `main.rs`, make readiness require both and update every test and document that used the moved routes.

## 2. Recap provider default (R2-13)

- [ ] 2.1 RED: add `channel_recap_defaults_to_openrouter_and_requires_a_key_when_enabled` and `scripted_mode_requires_a_loopback_nats_endpoint` to `crates/knowledge/tests/config.rs`; run them and confirm the default is `scripted` and there is no loopback rule.
- [ ] 2.2 GREEN: change the default, extend `validate_channel_recap`, log the scripted mode at startup and set the explicit mode in every test that relied on the default.

## 3. Recap redelivery, batch size and progress (R2-05 c and d)

- [ ] 3.1 RED: add `services/knowledge/tests/channel_recap_redelivery.rs` (delay table, redelivery with growing gaps and the counter, progress acknowledgement against a 3 second ack wait) and `recap_fetch_batch_defaults_to_one` to `crates/knowledge/tests/config.rs`; run them and confirm the fixed 2 second Nak, batch 32 and redelivery after the ack wait.
- [ ] 3.2 GREEN: compute the Nak delay from the delivery count, add the counter, default the batch to one and run the handler beside a progress ticker without dropping the handler.

## 4. Tenant deletion covers the channel recap tables (R2-06)

- [ ] 4.1 RED: add `crates/knowledge/tests/deletion_isolation.rs::delete_tenant_removes_channel_recap_state_of_that_tenant_only`; run it and confirm nothing is deleted.
- [ ] 4.2 GREEN: add `deletion_recap.rs`, the `channel_recap_rows` count and column, and the verification.

## 5. Blob registration and erasure protocol (R2-07)

- [ ] 5.1 RED: add `crates/knowledge/tests/deletion_blob_race.rs` with the three interleavings; run it and confirm a dangling reference is committed today.
- [ ] 5.2 GREEN: add `blob_lock`, hold it through erasure and take it at every registration path with a presence check and re-store.

## 6. Operator example and documentation (R2-10)

- [ ] 6.1 RED: add `services/knowledge/tests/deployment_profile.rs::the_shipped_example_enables_ingest_recap_and_both_listeners`; run it and confirm the example file is absent.
- [ ] 6.2 GREEN: write `deploy/systemd/knowledge.conf.example`, the `deploy/README.md` section, and refresh `README.md` and `DEVELOPMENT.md`. Documentation and a shipped example cannot start from a failing implementation test; the loader test in 6.1 is the check.
