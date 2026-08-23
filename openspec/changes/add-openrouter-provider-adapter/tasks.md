## 1. Recorded wire contract

- [x] 1.1 Add failing test `crates/knowledge/tests/openrouter.rs::request_body_maps_separated_fields_and_carries_no_credential`; serialize a fixture generation request under key `sk-LEAKME` and assert the body carries model, system/user message split with source inside the user message, JSON response format, `max_tokens`, and that neither the body nor any produced header string contains `sk-LEAKME`. Run it and confirm no adapter serialization exists.
- [x] 1.2 Add recorded OpenRouter fixtures (success, rate-limit, server-fault, auth, invalid-request) under `crates/knowledge/tests/fixtures/openrouter/` and implement request serialization in `crates/knowledge/src/openrouter.rs`; make 1.1 pass, run format and Clippy, and commit this pair on the task branch. Fixtures are recorded data: no test starts from them failing.
- [x] 1.3 Add failing test `crates/knowledge/tests/openrouter.rs::success_envelope_parses_content_usage_and_request_identity`; assert the recorded success fixture parses to the exact assistant content bytes, prompt/completion token counts, and the envelope request id. Run it and confirm parsing is absent.
- [x] 1.4 Implement success-envelope parsing into `ProviderResponse`, make 1.3 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 1.5 Add failing test `crates/knowledge/tests/openrouter.rs::error_envelopes_classify_transient_and_permanent`; assert the recorded 429 and 500 fixtures classify transient with preserved status while 401 and 400 classify permanent. Run it and confirm no classifier exists.
- [x] 1.6 Introduce `ProviderFailure` with the closed failure-class vocabulary and the status-preserving classifier, convert `ScriptedProvider` through `From`, make 1.5 pass, run format and Clippy, and commit this pair on the task branch.

## 2. Bounded transport

- [x] 2.1 Add failing test `crates/knowledge/tests/openrouter.rs::oversized_body_fails_without_buffering_past_cap`; stream more bytes than the cap from the fake transport and assert a permanent size failure while the process never buffers beyond the cap. Run it and confirm no HTTP adapter exists.
- [x] 2.2 Add the pinned `reqwest` client, chunked capped body reads, per-call deadline, and loopback-only plain-text base URLs; make 2.1 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 2.3 Add failing test `crates/knowledge/tests/openrouter.rs::stalled_response_hits_deadline_as_transient_timeout`; hold the response open past the deadline and assert a transient timeout classification within the deadline. Run it and confirm the call hangs or misclassifies.
- [x] 2.4 Wire the deadline into every transport try and classify expiry as `timeout`, make 2.3 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 2.5 Add failing test `crates/knowledge/tests/openrouter.rs::transient_faults_retry_with_jitter_inside_bounds`; script one 500 then a success and assert exactly two transport tries with a nonnegative bounded delay, plus no second try after a 401. Run it and confirm no retry exists.
- [x] 2.6 Implement the bounded jittered retry policy for network, rate-limit, and server classes only, make 2.5 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 2.7 Add failing test `crates/knowledge/tests/rate_limit.rs::second_call_waits_at_least_one_spacing_interval`; acquire twice back to back under a 100 ms interval and assert the second admission waits. Run it and confirm no limiter exists.
- [x] 2.8 Implement the fixed-spacing limiter, make 2.7 pass, run format and Clippy, and commit this pair on the task branch.

## 3. Durable budget ledger

- [x] 3.1 Add failing schema test `crates/knowledge/tests/schema.rs::provider_usage_records_window_totals`; insert two ledger rows across a day boundary and assert per-day and per-month token and cost totals through the ledger API. Run it and confirm the table is absent.
- [x] 3.2 Edit `schema.sql` in place with `knowledge.provider_usage` and its window index, implement ledger recording and window sums, make 3.1 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 3.3 Add failing test `crates/knowledge/tests/budget.rs::projected_daily_overrun_blocks_before_transport`; seed same-day usage past the ceiling, call the controlled provider against a counting fake transport, and assert budget exhaustion with zero transport requests. Run it and confirm no pre-call check exists.
- [x] 3.4 Implement the conservative characters/4 plus output-bound projection and pre-call enforcement, make 3.3 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 3.5 Add failing test `crates/knowledge/tests/budget.rs::monthly_ceiling_counts_earlier_days`; seed earlier-days usage below the daily but above the monthly ceiling and assert refusal. Run it and confirm only daily windows gate.
- [x] 3.6 Implement the UTC monthly window, make 3.5 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 3.7 Add failing test `crates/knowledge/tests/budget.rs::cost_ceiling_blocks_with_token_headroom`; configure nonzero prices so projected cost exceeds the cost ceiling while tokens remain, and assert refusal. Run it and confirm cost is not enforced.
- [x] 3.8 Implement micro-US-dollar cost projection and recording with u128 ceiling rounding, make 3.7 pass, run format and Clippy, and commit this pair on the task branch.

## 4. Pipeline integration

- [x] 4.1 Add failing test `crates/knowledge/tests/pipeline.rs::real_attempts_record_identity_latency_and_failure_class`; run the pipeline over the controlled provider against a 500-only fake transport and assert attempt rows carry adapter identity, model, positive latency, the server-error class, and status 500. Run it and confirm attempts still hold scripted placeholders only.
- [x] 4.2 Extend attempt persistence and the pipeline to use provider identity and structured failure facts, make 4.1 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 4.3 Add failing test `crates/knowledge/tests/pipeline.rs::cancelled_mid_request_keeps_durable_state_and_replays_once`; abort an in-flight run against a stalled transport, assert the run stays `model_requested` with the attempt open and no output, then replay against a healthy transport and assert exactly one accepted result. Run it and confirm the replay path mishandles the open attempt.
- [x] 4.4 Make replay idempotent for an open attempt after cancellation, make 4.3 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 4.5 Add failing test `crates/knowledge/tests/pipeline.rs::flaky_transport_keeps_retry_and_repair_bounded`; assert the always-500 transport ends the run after exactly two recorded attempts with a bounded transport-try count, and that an invalid-then-repair run with transient faults completes within the same bounds. Run it and confirm bounds are exceeded or unmeasured.
- [x] 4.6 Compose the controlled wrapper (limiter, budget, ledger) around the adapter as the real-provider constructor and enforce the bounds, make 4.5 pass, run format and Clippy, and commit this pair on the task branch.

## 5. Configuration and privacy

- [x] 5.1 Add failing test `crates/knowledge/tests/config.rs::provider_keys_are_finite_strict_and_secret`; assert unknown provider keys fail naming only key and rule, the credential redacts itself in Debug and Serialize output, model is required with a credential present, plain-text non-loopback base URLs fail, and all new limits default finite and positive. Run it and confirm the keys are unrecognized.
- [x] 5.2 Implement the provider and limit configuration keys with the redacting secret type, make 5.1 pass, run format and Clippy, and commit this pair on the task branch.
- [x] 5.3 Add failing test `crates/knowledge/tests/openrouter.rs::ordinary_logs_carry_no_credential_or_content`; capture logs for a full fake-transport run seeded with `LEAKME` credential and source text and assert neither appears while bounded facts do. Run it and confirm content or the key leaks.
- [x] 5.4 Add bounded-fact logging and redaction across adapter and wrapper, make 5.3 pass, run format and Clippy, and commit this pair on the task branch.

## 6. Process, docs, and gates

- [ ] 6.1 Add `crates/knowledge/examples/live_openrouter_smoke.rs`, a manually-run live check that loads environment configuration, sends one tiny bounded request, and prints bounded facts only. No test: live network example excluded from the gate by design; verify it compiles under Clippy and never runs in CI.
- [ ] 6.2 Update README, `docs/INTERFACES.md`, `docs/DATA_MODEL.md`, `docs/TESTING.md`, `DEVELOPMENT.md`, and tick implementation-plan item 5. No test: documentation; verify every statement against the built code and commit atomically on the task branch.
- [ ] 6.3 Run the exact `DEVELOPMENT.md` gate order with real PostgreSQL tests, `openspec validate add-openrouter-provider-adapter --strict`, the source-length ratchet, and secret and cross-schema audits; then merge to `main` and push only after all checks pass.
