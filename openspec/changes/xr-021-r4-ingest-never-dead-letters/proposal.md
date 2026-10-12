## Why

Round 4 of changeset XR-021 re-verified the Round 3 smoke results against `origin/main`. A database or provider outage that outlasts five deliveries silently loses the ingestion of extracted documents, social captures, social removals and AI conversations: the delivery is terminated on its fifth attempt and a row goes to `knowledge.ingest_failures`, a table that nothing reads. The extractor, the social producers and ChatGPT and Claude already reported that content as published, so nothing ever sends it again, and an X deletion fact is lost the same way. Round 3 fixed the same hazard for AI-archive tombstones only. The cross-repository behaviour is defined once in XR-021 CONTRACTS.md R4-05; this change cites it and does not restate it.

## What Changes

- Every retryable ingest failure is redelivered without a delivery budget and with a delay of `min(2 s * 2^(delivered - 1), 60 s)`; `Outcome::Retry` and `Outcome::Redeliver` become one outcome (R4-05 rules 1 and 5).
- A delivery that can never succeed is rejected at its first delivery with a closed class: the writer decided every retry arm per error variant (`is_permanent` on the pipeline, intake, inbox, deletion, blob, provider and outbox errors) instead of letting a permanent failure loop at one delivery a minute for ever (R4-05 rule 1).
- A provider outage is not a spend: a call that failed transiently is recorded and is not counted against the run's ten calls, so the run is reopened for as long as the outage lasts and the bound only stops a run that keeps getting unusable answers (the review of the first attempt found the capture lost after about five deliveries).
- A spend-budget refusal is a wait, not a verdict: it is recoverable, recorded as a non-spent attempt and redelivered until the daily or monthly window clears (the review of the second attempt found a bulk import past the daily cap lost for good).
- The repository family keeps its bounded budget, because its terminal delivery settles a `knowledge.repository_analysis.failed.v1` fact the requester waits for (R4-05 rule 2).
- The dead letter is replaced by visibility: a WARN from the second delivery on with the family, the class and the delivery count, and the label-free counter `knowledge_ingest_redeliveries_total` on the operator listener (R4-05 rule 3).
- `knowledge.ingest_failures` is removed from `schema.sql`, the tests and the documentation (R4-05 rule 4).
- Deliberate breaks, named in the commits that make them: the `knowledge.ingest_failures` table is gone (a development database is recreated); a delivery that failed five times is no longer terminated, it keeps coming back; the repository family's redelivery delay now grows (2, 4, 8, 16 seconds) instead of staying at 2 seconds; `redelivery_delay` is replaced by `RedeliverySchedule::STANDARD.delay`.

## Capabilities

### Modified Capabilities

- `knowledge-ingest`: redelivery without a budget, the reject rule, visibility instead of a dead letter.

## Impact

Affected surfaces are the ingest worker and its handlers, the operator metrics, the failure classification of the pipeline crate, `schema.sql`, the tests and the documentation. No new contract version, no migration file and no compatibility shim is introduced. Cross-repository behaviour is defined in XR-021 CONTRACTS.md.
