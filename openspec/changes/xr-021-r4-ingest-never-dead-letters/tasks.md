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

## 5. A provider outage is not a spend (R4-05 rule 1, review of the first attempt)

- [x] 5.1 RED: add `crates/knowledge/tests/pipeline_resume.rs::a_run_survives_a_transient_provider_outage_of_any_length` and `ingest_bus/redelivery.rs::a_provider_outage_longer_than_five_deliveries_does_not_lose_a_capture`; run them against the ten-attempt row check and confirm drive 6 reports `AttemptBudgetExhausted` instead of the transient failure and the capture is rejected as `unanalysable` after about six deliveries, so the redelivery wait times out.
- [x] 5.2 GREEN: count only the calls that did not fail transiently against `MAX_RUN_ATTEMPTS` in `record_attempt` and the run driver's reopen check, relax `analysis_attempts_ordinal_check` to `ordinal >= 1`, rewrite `a_run_at_its_attempt_budget_is_not_reopened` around ten spent calls, and update the README, DEVELOPMENT.md, INTERFACES.md, TESTING.md, the design and the delta.

## 6. A spend-budget refusal is a wait (R4-05 rule 1, review of the second attempt)

- [x] 6.1 RED: change `crates/knowledge/tests/failure_permanence.rs` so `ProviderError::BudgetExhausted` and the pipeline errors that wrap it recover, and add `crates/knowledge/tests/pipeline_resume.rs::a_run_survives_a_spend_budget_refusal_until_the_budget_clears` and `ingest_bus/redelivery.rs::a_spend_budget_refusal_longer_than_five_deliveries_does_not_lose_a_capture`; run them and confirm three permanence assertions fail on `BudgetExhausted`, drive 2 reports `Permanent` instead of the refusal, and the capture is rejected so the redelivery wait times out.
- [x] 6.2 GREEN: make `BudgetExhausted` recoverable, record it as a non-spent attempt that is not retried inside the same drive, and name the limits (the monthly window against the ordinal and the stream retention, and the unclassified query errors) in the design, the README and the delta.
