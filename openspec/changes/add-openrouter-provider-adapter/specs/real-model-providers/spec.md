## Purpose

Defines how Knowledge reaches one real inference provider: a recorded wire contract for the
OpenRouter chat-completions API, bounded transport behavior, durable spend control, cancellation-safe
execution, structured attempt evidence, and privacy rules that keep credentials and user content out
of logs and persisted configuration.

## ADDED Requirements

### Requirement: One real adapter speaks the OpenRouter wire format

Knowledge SHALL expose exactly one real provider adapter, speaking the OpenRouter-compatible
chat-completions HTTP protocol, behind the existing provider-neutral generation seam. The adapter
SHALL map the separated request fields (fixed system policy, task instruction, untrusted source
content, output schema identity) onto distinct message roles and SHALL carry the concrete model id,
a strict-JSON response request, and an output token bound. The adapter SHALL parse a successful
envelope into raw content bytes plus bounded token usage plus a provider request identity, and SHALL
classify error envelopes into transient (rate limiting, server faults, network faults, deadlines) and
permanent (authentication, malformed request) failures. Contract tests SHALL verify this mapping from
recorded fixtures without any live request.

#### Scenario: a generation request maps to the recorded body shape

- **WHEN** the versioned prompt request is serialized for the adapter
- **THEN** the body carries the configured model, separate system and user messages where untrusted
  source content stays inside the user message, a JSON-only response format, the output token bound,
  and no credential material anywhere in the body

#### Scenario: a success envelope is parsed into protected raw facts

- **WHEN** the transport returns a recorded success envelope with usage and a provider request id
- **THEN** the adapter yields the assistant content bytes as still-untrusted raw output, the counted
  input and output tokens, and the request identity

#### Scenario: error envelopes classify without retry ambiguity

- **WHEN** the transport returns recorded rate-limit, server-fault, authentication, or invalid-request
  envelopes
- **THEN** rate-limit and server-fault responses classify as retryable while authentication and
  invalid-request responses classify as permanent, each preserving its HTTP status

### Requirement: Transport is bounded by deadline, size, retry, and spacing

Every adapter call SHALL enforce a per-call deadline, cap buffered response bytes during streaming
reads, retry only transient classifications with a bounded attempt count and jittered backoff, and
pass through a rate limiter that spaces request starts by the configured interval. Oversized or
deadline-exceeded responses SHALL fail without buffering beyond the cap, and permanent failures SHALL
NOT be retried.

#### Scenario: a stalled server hits the call deadline

- **WHEN** the fake transport accepts the request but never completes the response
- **THEN** the call fails within the configured deadline as a transient timeout classification

#### Scenario: an oversized body is cut off

- **WHEN** the fake transport streams more bytes than the response cap
- **THEN** the call fails with a permanent size failure and the process never holds more than the cap
  of body bytes

#### Scenario: a server fault recovers inside the retry budget

- **WHEN** the first transport response is a server fault and the next succeeds
- **THEN** the adapter returns the successful response after exactly one extra transport attempt

#### Scenario: an authentication failure does not retry

- **WHEN** the transport answers with an authentication failure
- **THEN** the adapter returns a permanent failure and makes no further transport attempts

#### Scenario: concurrent calls are spaced

- **WHEN** two calls start back to back under a nonzero spacing interval
- **THEN** the second request starts no earlier than one interval after the first

### Requirement: Spend is budgeted before calls and recorded durably

Knowledge SHALL persist one usage-ledger row per real provider response with provider, model, input
tokens, output tokens, estimated cost in micro-US dollars, and recording time, inside the owned
schema. Before each call, the ledger SHALL project the request conservatively from supplied context
size, the output token bound, and configured per-token prices, and SHALL refuse the call when the
projection would exceed either the daily or monthly token or cost ceiling in UTC windows.

#### Scenario: successful usage is recorded once per response

- **WHEN** a real adapter response completes
- **THEN** exactly one ledger row records that response's tokens and estimated cost

#### Scenario: a projected daily overrun blocks the call

- **WHEN** recorded same-day usage plus the request projection exceeds the daily token ceiling
- **THEN** the call is refused as a budget exhaustion and no transport request starts

#### Scenario: monthly totals span days

- **WHEN** usage was recorded on earlier days of the current month below the daily ceiling but above
  the monthly one
- **THEN** the next call is refused by the monthly ceiling

#### Scenario: cost ceilings use configured prices

- **WHEN** configured per-token prices make the projected cost exceed a cost ceiling
- **THEN** the call is refused even though the token ceilings still have headroom

### Requirement: Cancellation leaves durable analysis state consistent

A real-provider run cancelled mid-request SHALL leave the run in its requested model state with the
started attempt still recorded, no accepted output, and no terminal regression. A later replay of the
same analysis identity SHALL complete using at most the remaining bounded attempts and SHALL produce
at most one accepted result.

#### Scenario: aborting a mid-flight request keeps replay safe

- **WHEN** an executing run's task is aborted while its transport request is outstanding
- **THEN** durable state shows the run awaiting its response with the attempt open and no output, and
  a replay afterwards completes the run exactly once

### Requirement: Retry and repair stay bounded on a flaky transport

With a transport that fails transiently, the pipeline SHALL still make at most its two recorded
attempts, each attempt making at most the adapter's bounded transport tries, and a repairable invalid
response followed by flaky transport SHALL either complete within those bounds or fail the run
without exceeding them.

#### Scenario: a permanently failing transport ends the run with bounded work

- **WHEN** every transport try returns a server fault
- **THEN** the run fails after exactly two recorded attempts with a bounded total number of transport
  tries and no accepted output

#### Scenario: a repair survives flaky transport inside the bounds

- **WHEN** the first response is invalid and the repair attempt succeeds after transient transport
  faults
- **THEN** the run completes after exactly two recorded attempts with one accepted result

### Requirement: Attempt records carry structured adapter facts

Each recorded provider attempt SHALL persist the adapter identity, the concrete model, measured
latency, and, when a transport response or failure exists, its HTTP status and one value from a
closed failure-class vocabulary. Logs at ordinary levels SHALL carry only these bounded facts and
SHALL NOT carry credential values, prompts, source content, or response bodies. The credential SHALL
redact itself in diagnostics, SHALL never be persisted to the database, and SHALL reach only the
adapter's authorization header.

#### Scenario: a failed attempt stores its class, status, and latency

- **WHEN** an attempt fails from a recorded server-fault envelope
- **THEN** its row shows the closed-class server fault, the HTTP status, a positive latency, and the
  adapter and model identities

#### Scenario: secrets and content never reach ordinary logs

- **WHEN** an analysis runs against the fake transport with a marked credential and marked source
  text
- **THEN** captured log output contains neither the credential nor the marked content, only bounded
  facts

### Requirement: Provider configuration is environment-only and finite

Adapter and control configuration SHALL load only from the strict `RATATOSKR__` environment keys:
credential, base URL, model id, per-token prices, output token bound, request spacing, and daily and
monthly token and cost ceilings, each with a finite default except the credential. An unknown key,
non-Unicode value, or invalid value SHALL stop startup without printing any supplied value. When a
credential is present a model id SHALL be required; absent credential SHALL keep default tests and
the process offline. Plain-text base URLs SHALL be accepted only for loopback hosts.

#### Scenario: invalid provider configuration stops without leaking

- **WHEN** the environment contains an unknown provider key or a non-loopback plain-text base URL
- **THEN** configuration loading fails, names only the key and rule, and prints no supplied value

#### Scenario: defaults stay finite

- **WHEN** configuration loads with no provider keys set
- **THEN** every new limit holds a positive finite default and no credential exists
