## Purpose

Accepts immutable source facts from other bounded contexts into Knowledge exactly once, retaining enough delivery evidence to safely replay, defer, and diagnose them without reading producer tables.

## ADDED Requirements

### Requirement: Contract-validated source facts are durably claimed

Knowledge SHALL validate a supported source event before creating analysis work, persist its delivery identity and causation metadata, and create work only after the source fact is durably claimed.

#### Scenario: Valid repository fact is accepted once

- **WHEN** a valid `knowledge.repository_analysis.requested.v1` command is delivered for a new immutable revision
- **THEN** Knowledge records one inbox receipt and queues one repository analysis identity tied to that revision

#### Scenario: Unsupported or invalid event is not analyzed

- **WHEN** an event has an unsupported subject or fails its published contract validation
- **THEN** Knowledge records a safe rejection outcome and creates no analysis run or provider request

### Requirement: Redelivery and gaps are safe

Knowledge SHALL make at-least-once delivery idempotent and SHALL preserve enough sequence or source revision information to avoid terminal-state regression when delayed facts arrive.

#### Scenario: Redelivery does not duplicate work or spend

- **WHEN** the same event is delivered again after its analysis has completed
- **THEN** the inbox reports the existing receipt and no second run, provider attempt, or budget charge is created

#### Scenario: Older event arrives after newer revision

- **WHEN** a delayed fact refers to an older immutable revision after a newer revision has been accepted
- **THEN** its receipt is retained for audit while no current projection regresses
