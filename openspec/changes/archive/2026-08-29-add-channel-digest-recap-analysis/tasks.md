## 1. Contract pin and durable intake

- [x] 1.1 RED: add a compile/fixture test proving Knowledge accepts only the typed `knowledge.channel_digest_recap.requested.v1` contract and rejects malformed owner/run/manifest/window/count fields without logging values; run it and confirm the contract pin/handler is absent.
- [x] 1.2 GREEN: pin the published channel-digest Contracts revision, add the typed subject mapping and content-free admission errors, and rerun the focused contract test.
- [x] 1.3 RED: add a real-PostgreSQL test `channel_recap_inbox_is_idempotent_and_owner_scoped` that applies current `schema.sql`, admits duplicate event and semantic request identities, and expects one durable work item; run it and confirm the schema/path is absent.
- [x] 1.4 GREEN: edit current `schema.sql` in place for recap inbox/run/result/outbox linkage and implement transactional admission; rerun the focused PostgreSQL test and confirm replay produces one item.
- [x] 1.5 RED: add `channel_recap_terminal_state_cannot_regress` covering every legal retry/persist/complete/fail transition and replay after restart; run it and confirm an illegal transition or duplicate terminal fact is possible.
- [x] 1.6 GREEN: implement expected-state SQL transitions, natural analysis identity, and atomic terminal outbox insertion; rerun the state test until it passes.

## 2. Authenticated manifest retrieval

- [x] 2.1 RED: add a scripted HTTP test `manifest_client_sends_only_service_and_owner_authority` proving fixed loopback origin, no redirect, finite connect/request/body limits, caller-header stripping, owner/run/reference claims, and redacted diagnostics; run it and confirm the client is absent.
- [x] 2.2 GREEN: implement strict digest-source client configuration and transport using existing networking patterns; rerun the focused client test.
- [x] 2.3 RED: add `manifest_bytes_are_verified_before_analysis` with valid canonical bytes and wrong digest, owner, run, window, duplicate revision, count, timestamp, and per-post digest cases; run it and confirm at least one malformed manifest reaches provider preparation.
- [x] 2.4 GREEN: implement deny-unknown manifest decoding, canonical digest/integrity and cross-field validation, and durable accepted-source identity; rerun the manifest matrix and prove invalid input makes zero provider calls.
- [x] 2.5 RED: add a retry/restart test for digest API timeout, unavailable, oversized body, and exhausted deadline; run it and confirm work is lost, loops, or reports success.
- [x] 2.6 GREEN: implement bounded retry/backoff state and safe terminal failure publication; rerun the failure/restart test and verify no source text or endpoint value enters telemetry.

## 3. Context, prompt, and typed recap

- [x] 3.1 RED: add `channel_recap_context_is_deterministic_and_keeps_complete_revisions` over reordered inputs, 100/20 boundaries, token pressure, edits, and omissions; run it and confirm no deterministic builder exists.
- [x] 3.2 GREEN: implement the versioned recap context builder with stable ordering, complete-revision selection, exact included/omitted identities, and budget accounting; rerun the context tests.
- [x] 3.3 RED: add `channel_source_instructions_cannot_replace_fixed_policy` with instruction-like content and links; assert policy, task, schema, labels, and source fields remain separated and external fetch is impossible, then run it and confirm the recap prompt is absent.
- [x] 3.4 GREEN: add versioned channel-recap system/task prompt resources and the bounded provider request; rerun the injection-boundary test.
- [x] 3.5 RED: add strict result tests for all headline/overview/topic/notable/citation/coverage/warning limits, unknown fields, duplicate/foreign/omitted citations, and model-provided URLs; run them and confirm at least one invalid output passes.
- [x] 3.6 GREEN: implement generated-schema-first and semantic grounding validation for `ChannelDigestRecap`; rerun the result matrix until every invalid case fails safely.

## 4. Provider pipeline and completion facts

- [x] 4.1 RED: add a real-PostgreSQL pipeline test proving bounded raw response is stored before parsing, accepted typed output and state commit atomically, and telemetry excludes source/model text; run it and confirm recap execution is absent.
- [x] 4.2 GREEN: connect the recap family to the existing raw-response-first provider pipeline and atomic result persistence; rerun the focused pipeline test.
- [x] 4.3 RED: add scripted transient-then-valid, invalid-then-repaired, second-invalid, permanent, and replay-after-completion cases; assert at most two calls and no spend on completed replay, then run them and confirm budget/idempotency is not enforced.
- [x] 4.4 GREEN: reuse the shared one-extra-attempt retry-or-repair budget with recap-specific identities and safe failure mapping; rerun the attempt table.
- [x] 4.5 RED: add outbox tests proving exactly one typed completion or failure with matching owner/run/manifest/result/counts and no content, including publish-redelivery restart; run them and confirm duplicate or inconsistent facts are possible.
- [x] 4.6 GREEN: implement typed completion/failure construction and transactional outbox settlement; rerun outbox/replay tests.

## 5. Evaluation, process wiring, and gate

- [x] 5.1 RED: add synthetic evaluation fixtures for empty/partial/full multi-channel windows, edited/repeated/conflicting/long posts, malformed manifests, and prompt injection; run the evaluator and confirm the recap family/metrics are absent.
- [x] 5.2 GREEN: register recap evaluation metrics for schema, citations, unsupported claims, coverage, deterministic context digest, and budgets; make the synthetic evaluation gate pass without real channel content or external inference.
- [x] 5.3 RED: add a service boot test proving the recap consumer, digest-source dependency readiness, graceful drain/resume, and no inference credentials required for scripted mode; run it and confirm the process does not satisfy the lifecycle.
- [x] 5.4 GREEN: wire configuration, consumer supervision, source client, worker, outbox, readiness, telemetry, and bounded shutdown; rerun the boot test.
- [x] 5.5 Update README, interfaces, data model, privacy, prompts, deployment, and evaluation documentation; no failing test applies to prose, so verify statements against code and run doc/config drift checks.
- [x] 5.6 Run the exact `DEVELOPMENT.md` gate through `build-gate` where compiler-backed, all real-PostgreSQL and scripted-provider tests, the evaluation gate, `openspec validate add-channel-digest-recap-analysis --strict`, secret/content audits, and `git diff --check`; record observed results before publication.
