## 0. OpenSpec change

- [x] 0.1 Write this change (proposal, design, tasks, the `knowledge-ingest` delta) citing XR-021 CONTRACTS.md R4-05 and run `openspec validate --all --strict`. Documentation cannot start from a failing test.

## 1. The delay grows and nothing retryable is terminal (R4-05 rule 1)

- [x] 1.1 RED: add `services/knowledge/src/redelivery.rs::redelivery_delay_doubles_from_two_seconds_and_stops_at_sixty` and `services/knowledge/src/ingest.rs::a_retryable_outcome_is_never_terminal`; run them against a stub schedule that returns zero and the budgeted `ack_kind` and confirm the delay assertion fails with `[0, 0, ...]` and delivery 5 is `Term`.
- [x] 1.2 GREEN: compute the delay from the delivery count, merge `Outcome::Retry` and `Outcome::Redeliver` into `Outcome::Retry(class)`, delete the `ingest_failures` insert and move the recap worker to `RedeliverySchedule`.

## 2. Each family survives a fault longer than five deliveries (R4-05 rules 1 and 5)

- [x] 2.1 RED: add `services/knowledge/tests/ingest_bus/redelivery.rs` with a document, a social capture, a social removal, an AI conversation, a provider outage followed by a database outage, a permanent provider failure, an undecodable message and the repository family; run it before task 1.2 and confirm the first five time out because the delivery is terminated on its fifth attempt, and the log test finds no redelivery line.
- [x] 2.2 GREEN: nothing beyond 1.2 for the redelivery itself. Add `crates/knowledge/tests/failure_permanence.rs` (RED against `is_permanent` stubs that return `false`: the assertions on every permanent variant fail) and then the `is_permanent` methods, and route every retry arm of `ingest_handlers.rs` through them.

## 3. Visibility replaces the dead letter (R4-05 rule 3)

- [x] 3.1 RED: add `services/knowledge/tests/admin.rs::the_operator_plane_exposes_the_ingest_redelivery_counter` and `ingest_bus/redelivery.rs::a_redelivery_from_the_second_on_logs_family_class_and_count_without_content`; run them against a no-op `record_ingest_redelivery` and the old log line and confirm the counter stays 0 and no line says `will be redelivered`.
- [x] 3.2 GREEN: count each redelivery in `Metrics` and log it from the second delivery on.

## 4. Remove the dead-letter table (R4-05 rule 4)

- [x] 4.1 RED: remove `ingest_failures` from the expected table lists of `crates/knowledge/tests/schema.rs` and `ingest_bus/tenant.rs` and add `the_schema_has_no_ingest_failures_table`; run them and confirm the table list differs and `to_regclass` finds the table.
- [x] 4.2 GREEN: delete the table from `schema.sql`, the remaining assertions that read it, and the sentences in `README.md` and `docs/DATA_MODEL.md`. A development database is recreated.
