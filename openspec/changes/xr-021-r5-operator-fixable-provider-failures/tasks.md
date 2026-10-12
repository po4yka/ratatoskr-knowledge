## 1. Classification (R5-08 rule 1)

- [x] 1.1 RED: add `crates/knowledge/tests/openrouter.rs` status table (401 and 403 `AuthError` with `OperatorAction`, 402 `PaymentRequired`, 404 `ModelUnavailable`, 400 and 422 `RequestInvalid` with `Permanent`, 408, 429, 5xx as before), the permanence of `OperatorAction` and the spellings, and `crates/knowledge/tests/failure_permanence.rs` cases (a run survives twenty consecutive real-adapter 401s over a loopback transport with no permanent failure and no spent attempt, in `pipeline_resume.rs`). Add the new variant and classes as stubs that classify like today and confirm the failures are on the assertions.
- [x] 1.2 GREEN: implement `ProviderError::OperatorAction`, the two classes and spellings, `classify_error`, the three `failure_class` CHECK lists, the non-spent attempt handling in `run_driver.rs`, and the matching arms in `controlled.rs`, `embeddings.rs` and the ingest outcome mapping.

## 2. Redelivery of the three families (R5-08 rule 1)

- [x] 2.1 RED: add `services/knowledge/tests/ingest_bus/redelivery.rs` cases on a real broker and PostgreSQL: a document, a social capture and an AI-archive conversation (one test each, through the real adapter over a loopback transport) whose provider answers 401 for the first eight deliveries end applied once the fault clears with no Term and `knowledge_ingest_redeliveries_total` above zero; a 400 is still Termed at its first delivery. Run them against the classification stubs and confirm the deliveries are Termed.
- [x] 2.2 GREEN: the WARN with family, class and delivered count from the second delivery, and the README table of provider failure classes.
