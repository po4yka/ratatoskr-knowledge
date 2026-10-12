## Context

XR-021 CONTRACTS.md R4-05 is the contract. This file records only the choices local to Knowledge.

## Decisions

### One retryable outcome and one pure decision

`Outcome` is `Done`, `Reject(class)` and `Retry(class)`. `ack_kind(outcome, delivered, schedule)` is a pure function that maps a retryable outcome to a negative acknowledgement with `schedule.delay(delivered)` and never to a termination, so the rule "nothing retryable is terminal" is a unit test and not a property of a database round trip. `IngestContext::disposition` only logs and counts around it.

### The schedule is a value

`RedeliverySchedule { base, cap }` replaces the recap worker's private `redelivery_delay`; `STANDARD` is 2 s doubling to 60 s and both workers use it. The tests need eight redeliveries of one message inside a 40 second polling deadline, so the spawn seam `spawn_ingest_workers_with_schedule` takes a `Redelivery { schedule, metrics }` argument. It is an argument and never a configuration key: a deployment cannot lower the production delay.

### Which failures are permanent

A redelivery without a budget is only safe for a delivery that a later attempt can fix. The run driver records every provider failure except `Transient` as a permanent attempt, and a run that failed permanently, or whose response stayed invalid after the repair, reports the same failure on every later drive without calling the provider. A run holds at most ten attempts over all its deliveries (`analysis_attempts_ordinal_check`). `is_permanent` is therefore:

| Error | Permanent | Recovers |
|---|---|---|
| `PersistenceError` | invalid source, invalid identity, attempt budget exhausted, value overflow, encode, a permanent blob error | connect, schema, query, a recoverable blob error |
| `BlobError` | too large, invalid reference | unavailable, io |
| `ProviderError` | everything but `Transient` | `Transient` |
| `PipelineError` and `FamilyPipelineError` | invalid response, permanent provider or persistence or blob, source identity, contract encoding, corrupt README, a permanent inbox error | timeout, a run held by another executor, transient provider, a recoverable storage error, an unavailable README |
| `DocumentIntakeError` | unsupported event, wrong producer, missing tenant, invalid payload, context, request, plus the permanent persistence and analysis cases | the rest |
| `SourceInboxError`, `ArchiveEventError`, `EventOutboxError`, `DeletionError` | undecodable envelope, unencodable snapshot, contradictory archive fact, unrepresentable value | storage and deletion failures |

The repository family keeps its own classification (`classify_repository_failure`, whose unit test pins `Invalid` as retryable): its handler stops retrying at `TERMINAL_DELIVERY` and settles a fact, so a response that stays invalid still ends in a failed fact, not in a termination.

A permanent failure is `Reject` with a closed class: `document`, `unanalysable`, `inbox_invalid`, `unqueueable`, `unprocessable`, next to the existing `undecodable`, `producer`, `tenant`, `owner`, `payload`, `type`, `unsupported`, `foreign_subject`, `invalid_fact` and `repository_request`.

### Known limit: a provider outage is bounded by the run

A provider outage is retried without a delivery budget, but each delivery makes up to two provider calls and a run records at most ten over all its deliveries. A provider that stays down for about five deliveries (about a minute with the standard schedule) exhausts the run, the next delivery reports `AttemptBudgetExhausted`, which is permanent, and the delivery is rejected as `unanalysable`. The source and the failed run stay in the database and the log line carries the class. Lifting that needs a decision about the per-run attempt bound (it is a spend guard and a schema check), which is outside this change; the tests therefore combine a provider outage of three deliveries with a database outage of four more.

### Visibility

`knowledge_ingest_redeliveries_total` counts every negative acknowledgement of a retryable outcome, from the first. The WARN starts at the second delivery so a single transient hiccup is not a warning; the first failure is logged at debug. The family and class live in the WARN line because `metrics.rs` is label-free by design.

### Removing the dead-letter table

`knowledge.ingest_failures` is deleted from `schema.sql` (no migration: development status), from the table lists of the schema and tenant tests, and from the README and `docs/DATA_MODEL.md`. `TERMINAL_DELIVERY` stays, documented as the repository family's budget, and `lib.rs` keeps exporting it.
