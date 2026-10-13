## 1. Change

- [x] 1.1 Write this change and run `openspec validate --all --strict`. Documentation cannot start from a failing test.

## 2. The disposition table (R6-01 rule 2)

- [x] 2.1 RED: add the unit test `repository_disposition_table` in `services/knowledge/src/ingest_handlers.rs` over `RepositoryDisposition` (operator and budget failures wait at deliveries 1, 5, 6 and 1000; transient provider failure, timeout, conflict, invalid response and unavailable README retry within the budget and settle at and after delivery 5; a source or integrity failure settles `contract_invalid` at delivery 1); add a signature-only `repository_disposition` that settles every input as `dependency_unavailable`, and confirm the test fails on an assertion.
- [x] 2.2 GREEN: implement the decision, use it in `repository`, keep `classify_repository_failure` as the mapping of a settled failure, and reword the `TERMINAL_DELIVERY` and `Outcome::Retry` doc comments.

## 3. Operator-fixable wait (R6-01 rule 5)

- [x] 3.1 RED: add `services/knowledge/tests/ingest_bus/repository_wait.rs::a_rejected_credential_does_not_lose_a_repository_request` (a real OpenRouter adapter over a loopback transport answering 401 eight times): one completed fact, no failed fact at any time, nine transport requests, two run rows, redeliveries counted, the `will be redelivered` WARN with `family="repository"` and `class="provider_operator_action"` for deliveries 2 to 8 and no content or credential. Run it against the budgeted handler and confirm it fails on the failed-fact assertion.
- [x] 3.2 GREEN: tasks 2.2 and 3.1 together make it pass.

## 4. Spend-budget wait

- [x] 4.1 RED: add `a_spend_budget_refusal_does_not_lose_a_repository_request` (a scripted provider answering `BudgetExhausted` eight times, then the analysis): one completed fact, no failed fact, redeliveries counted, log class `repository_analysis`. Run it before 2.2 and confirm the failed-fact assertion fails.
- [x] 4.2 GREEN: already green after 2.2.

## 5. The budget that stays

- [x] 5.1 Characterization pair, which passed at once because the behaviour is unchanged and so cannot be RED: a README source answering 503 for four deliveries and then recovering completes with no failed fact; a provider `Transient` for five deliveries settles the retryable `dependency_unavailable` fact exactly once. The existing `the_repository_family_still_settles_a_failed_fact_at_the_fifth_delivery` and the permanent cases of `repository.rs` pass unchanged.

## 6. Documentation

- [x] 6.1 `README.md` (the ingest paragraph), `DEVELOPMENT.md` (the `RATATOSKR__INGEST__FETCH_BATCH` note) and `docs/TESTING.md` say that the repository family waits on an operator-fixable failure and a spend-budget refusal and settles only a plain transient fault after five deliveries. Documentation cannot start from a failing test.
