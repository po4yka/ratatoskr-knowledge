# Channel Digest Recap Analysis Specification

## Purpose

Defines Knowledge's durable, provenance-grounded, schema-validated recap analysis over one bounded immutable manifest of public Telegram channel-post revisions.

## Requirements

### Requirement: Recap requests are validated, authorized, and deduplicated before work

Knowledge SHALL consume only the published `knowledge.channel_digest_recap.requested.v1` command from its exact configured subject and authenticated digest-service producer. It SHALL validate envelope type, owner, operation/run identity, closed-open window, language, counts, manifest reference, SHA-256 digest, and empty producer-authored extensions before persisting one inbox receipt and one recap analysis identity.

The analysis identity SHALL be unique by owner, digest run, manifest digest, analysis family/contract version, prompt version, context-preparation version, and output language. Exact redelivery SHALL reuse that identity and SHALL not create another active run or model spend; reuse of a run with contradictory immutable fields SHALL be a terminal conflict.

#### Scenario: Exact request is redelivered

- **WHEN** the same logical recap request arrives again under another transport command identifier
- **THEN** Knowledge acknowledges it through the existing analysis identity and records no second provider attempt

#### Scenario: Foreign producer uses the subject

- **WHEN** an otherwise valid payload arrives from a producer identity not authorized for channel-digest recap requests
- **THEN** Knowledge creates no analysis run and records only a safe authorization failure class

### Requirement: Source manifest retrieval verifies immutable provenance

Knowledge SHALL retrieve the manifest and referenced post revisions only through the authenticated `ratatoskr-channel-digests` source interface, binding the request owner and manifest reference to its service identity. It SHALL enforce finite response-byte, source-count, channel-count, per-post-byte, nesting, and timeout limits before materializing context. It SHALL verify the manifest SHA-256, every post revision content digest, exact source and channel counts, source uniqueness, publication inside the requested window, and owner/run linkage.

Any absent source, foreign owner, duplicate identity with contradictory bytes, digest mismatch, count contradiction, out-of-window source, or changed manifest SHALL fail before inference. Knowledge SHALL not repair or fetch missing channel content from Telegram, the Internet, another service database, or a provider URL.

#### Scenario: One post digest no longer matches

- **WHEN** the manifest digest is valid but one retrieved revision hashes differently from its listed content digest
- **THEN** the run settles with `manifest_integrity`, makes no provider request, and publishes no successful result

#### Scenario: Source interface returns another owner

- **WHEN** a manifest retrieval resolves to a different owner than the command
- **THEN** Knowledge treats it as scoped absence/authorization failure and exposes no manifest fact to the requester

### Requirement: Context preparation is deterministic and budgeted

Knowledge SHALL prepare recap context from the verified manifest in a versioned deterministic order. It SHALL preserve stable post revision references, channel references, provider-authored publication times, content digests, and explicit truncation/omission evidence separately from untrusted post text. At most 100 posts, 20 channels, and the configured finite input-token budget SHALL enter one recap run.

If all verified posts fit, included coverage SHALL equal selected coverage. If the token budget requires omission, selection SHALL be deterministic and coverage SHALL identify exactly how many verified sources were included or omitted using safe warning codes. A context that cannot include at least one complete source SHALL fail with `context_budget` rather than analyze a fragment presented as full coverage.

#### Scenario: Same manifest is prepared twice

- **WHEN** identical verified manifest bytes, context-preparation version, and budget are processed twice
- **THEN** the selected source references, order, truncation evidence, token estimate, and prepared-context digest are identical

#### Scenario: Oversized context requires omission

- **WHEN** 100 valid posts exceed the configured token budget but 30 complete posts fit
- **THEN** the recap may continue with those deterministically selected 30 and coverage reports 70 omitted without claiming full coverage

### Requirement: Channel recap output is structured and grounded

The accepted recap contract SHALL contain: a headline of 1 through 160 Unicode scalar values; an overview of 1 through 1600; 1 through 5 topic groups, each with a 1 through 80 label, a 1 through 400 summary, and 1 through 10 distinct citations; 0 through 5 notable items, each with a 1 through 160 title, a 1 through 320 summary, and at least one citation; coverage counts and up to 10 closed safe warning codes; output language; contract, prompt, and context-preparation versions; and the verified manifest digest.

Every citation SHALL reference an included post revision. Every topic and notable item SHALL have at least one citation, and every cited revision SHALL belong to the same verified manifest. Knowledge SHALL not invent public URLs, channel identity, publication time, author identity, or unsupported claims. Free-form unknown fields SHALL be rejected.

#### Scenario: Model cites an omitted post

- **WHEN** otherwise valid structured output cites a manifest post omitted from prepared context
- **THEN** schema/provenance validation rejects the attempt and bounded repair may run only under the documented repair budget

#### Scenario: Valid recap preserves exact grounding

- **WHEN** every topic and notable item cites included source revisions and all bounds hold
- **THEN** Knowledge persists the typed recap with those exact citations, manifest digest, versions, and coverage

### Requirement: Channel content is untrusted evidence, never instruction

The versioned recap prompt SHALL separate system policy, task instruction, source metadata, and channel-post text. It SHALL explicitly treat all post content, channel titles, links, and quoted messages as untrusted evidence and SHALL forbid following embedded instructions, selecting tools/models, reading files/URLs, revealing prompts, or performing external writes. The provider request SHALL receive no MTProto credential, Bot API token, private dialog material, owner identifier unnecessary for analysis, or internal service URL.

Structured output SHALL be independently validated regardless of provider claims. A prompt change that can alter stored recap behavior SHALL use a new prompt version and pass the recap evaluation set.

#### Scenario: Post contains prompt injection

- **WHEN** a fixture post instructs the model to ignore policy, reveal secrets, open a URL, and omit citations
- **THEN** the evaluated result follows the recap schema and grounding policy or fails safely without tool/network/provider side effects

### Requirement: Execution is durable, bounded, and terminally truthful

Each recap SHALL progress through explicit persisted receipt, context verification/preparation, provider request, raw-response evidence, schema validation/repair, result persistence, and completion publication states. Provider attempts, repairs, timeouts, cancellation, concurrency, input/output tokens, and cost SHALL have finite configured bounds. A terminal completion or failure SHALL not regress, and an indexing failure SHALL not erase a valid recap result.

Knowledge SHALL publish `knowledge.channel_digest_recap.completed.v1` only after the typed result and result digest are durable. Permanent or exhausted failures SHALL publish the typed safe failure fact. It SHALL never publish both terminal outcomes for one analysis identity.

#### Scenario: Provider response is lost after request

- **WHEN** the provider request may have executed but no valid response is observed before bounded attempts expire
- **THEN** the run settles with a safe timeout/unavailable failure and does not fabricate or publish a successful recap

#### Scenario: Completion is redelivered from the outbox

- **WHEN** a persisted completion fact is published more than once after uncertain broker acknowledgement
- **THEN** every event carries the same analysis/result linkage and downstream deduplication needs no content comparison

### Requirement: Privacy-safe telemetry and evaluations cover quality boundaries

Ordinary logs and metric labels SHALL contain only bounded analysis stage/outcome, attempt class, counts/budget buckets, safe identifiers permitted by existing Knowledge policy, and correlation data. Post text, channel username/title, public link, recap narrative, manifest bytes, prompt/raw response, internal owner, provider diagnostic, and credentials SHALL NOT appear in ordinary logs or labels.

Committed synthetic evaluations SHALL cover contract validity, citation membership, coverage arithmetic, prompt injection, unsupported claims, repeated/edited posts, multilingual `ru` and `en` outputs, partial context, empty/invalid manifests, and bounded failure paths. Evaluation SHALL judge grounding and contract usefulness rather than exact wording equality.

#### Scenario: Failed recap remains content-free in telemetry

- **WHEN** a source containing a unique private-looking marker causes output validation failure
- **THEN** captured logs and metrics report only finite recap failure classes and contain none of the marker, source text, or generated narrative
