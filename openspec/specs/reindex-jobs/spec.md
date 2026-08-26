# Reindex Jobs Specification

## Purpose

Defines explicit bounded operator jobs that regenerate lexical projections and embeddings from durable Knowledge state under the configured active identity.

## Requirements

### Requirement: Search documents rebuild from durable projection inputs

The search rebuild SHALL restore in-scope `search_documents` from their persisted calculated projection inputs without re-fetching source content. Missing inputs SHALL be counted as skipped.

#### Scenario: damaged projections are restored

- **WHEN** one search row is missing and another is corrupted after persistence
- **THEN** a scoped rebuild restores only in-scope data and an unrestricted rebuild restores every reproducible row to the persisted title, lead, body, document id, and output id

### Requirement: Jobs are idempotent and resumable

Both jobs SHALL commit each source separately in deterministic source-id order. A converged rerun SHALL write nothing and issue no embedding provider calls.

#### Scenario: an interrupted run resumes without repeated mutation

- **WHEN** processing stops after a source commits and the same job is run again
- **THEN** completed work stays durable and only remaining work is selected

### Requirement: Jobs honor explicit scope restrictions

Both jobs SHALL accept unrestricted, tenant, or tenant-plus-source scopes. Planning SHALL select only scoped sources and preserve all out-of-scope stale projections and vectors.

#### Scenario: another tenant remains untouched

- **WHEN** stale rows exist for two tenants and a job runs for one tenant
- **THEN** only that tenant appears in the plan or progress and the other tenant's rows remain byte-identical

### Requirement: Jobs report progress and exit honestly

Jobs SHALL print a deterministic source progress line for each committed change and a final processed/failed total. A full success SHALL exit zero; a source failure SHALL leave earlier commits durable and exit nonzero. Embeddings reindex SHALL fail before database access when no usable embeddings configuration exists.

#### Scenario: process output matches durable work

- **WHEN** an operator runs either job against several changed sources
- **THEN** stdout presents ascending source progress and totals matching the committed database outcome

### Requirement: Embeddings use only the configured active identity

Embeddings reindex SHALL derive provider, model, chunking, and prompt identity solely from validated configuration and SHALL prune obsolete vectors only in the successful replacement transaction.

#### Scenario: scope does not override embedding identity

- **WHEN** any valid scope is run under one configured embedding identity
- **THEN** every replacement vector carries that identity and no other identity is created

### Requirement: Concurrency is bounded and ordered

Jobs SHALL process exactly one source at a time in ascending source identifier order and SHALL NOT issue unbounded parallel provider calls.

#### Scenario: progress proves sequential processing

- **WHEN** more than one source needs work
- **THEN** each source commit completes before the next progress line and lines appear in ascending identifier order
