# Reindex Jobs Specification

## Purpose

Defines the explicit operator commands that rebuild the lexical search projection and the embedding vectors from durable analysis data under the active configured identity, scoped by tenant or source, idempotent, resumable, bounded, and reporting what they did.

## ADDED Requirements

### Requirement: Search documents rebuild from accepted outputs

A reindex-search-documents operation SHALL regenerate each tenant's `search_documents` rows from the immutable `search_projection_inputs` snapshot written beside the accepted output of every in-scope source. A missing projection SHALL be recreated and a corrupted one SHALL be restored to the same content the persist path wrote, without re-fetching source content.

#### Scenario: a damaged projection is restored byte-identically

- **WHEN** a source's search document row is deleted and another source's row has its title, lead, or body altered after their analyses persisted, and the rebuild job runs
- **THEN** both rows again hold exactly the title, lead, and body that the original persist transaction wrote, including unchanged document identifiers and latest output references

### Requirement: Jobs are idempotent

Running either reindex job against fully converged data SHALL perform no writes and no provider calls, and running it again after success SHALL change nothing.

#### Scenario: converged projections make the rebuild a quiet pass

- **WHEN** every in-scope search document already matches its latest accepted output and the rebuild job runs twice
- **THEN** the first run reports zero rebuilt sources and the second run also reports zero, with all stored rows unchanged between runs

#### Scenario: converged embeddings make zero provider calls

- **WHEN** every in-scope source carries complete vector coverage under the active embedding identity and the embeddings reindex runs
- **THEN** no provider request is made and the summary reports zero processed sources

### Requirement: Jobs honor explicit scope restrictions

Both jobs SHALL accept an optional tenant restriction and an optional single-source restriction naming owner context and source document identifier; planning SHALL select only sources inside the scope, and sources outside it SHALL remain untouched, including their superseded-identity vectors and stale projections.

#### Scenario: another tenant's data is invisible to a scoped run

- **WHEN** two tenants hold sources needing rebuilds and the job runs restricted to one tenant
- **THEN** only that tenant's sources appear in the plan and results, and the other tenant's rows keep their pre-run values

### Requirement: Jobs are resumable with completed work persisted

Each job SHALL commit its work per source in a deterministic order, so an interrupted run leaves finished sources converted and unfinished sources selectable by the next run of the same command without redoing committed conversions.

#### Scenario: an interrupted run converges on rerun

- **WHEN** a run is stopped after some planned sources have committed and the identical command runs again
- **THEN** the second run processes only the remaining sources, and afterwards every planned source is converted while already-committed sources show no repeated mutation

### Requirement: Jobs report progress and exit honestly

Each job SHALL print one progress line per processed source and a final total summary to standard output, SHALL exit nonzero when any source fails, and SHALL leave successfully completed sources persisted when exiting nonzero. A fully successful run SHALL exit zero.

#### Scenario: a failing source still keeps earlier successes and reports failure

- **WHEN** the embeddings reindex processes several sources and the provider fails permanently for one of them
- **THEN** sources completed before the failure retain their new vectors, the failing source's failure record notes the class, the summary names the failure count, and the process exit code is nonzero

#### Scenario: operator output shows per-source progress and totals

- **WHEN** either job completes over multiple sources in a real process invocation
- **THEN** standard output contains one line per processed source and one final line with the processed and failed totals, matching the recorded database outcome

### Requirement: Jobs bind only the configured active identity

Reindex jobs SHALL derive their target embedding identity solely from the configured provider configuration, SHALL accept no identity override on the command line, and SHALL prune superseded-identity rows for a source only inside the transaction that stores that source's replacement vectors.

#### Scenario: scoping never changes which identity is written

- **WHEN** an embeddings reindex runs with any scope restriction against a configuration naming one provider, model, chunking version, and prompt version
- **THEN** every written vector row carries exactly that configured identity tuple and no row under any other identity is created

### Requirement: Concurrency stays bounded and ordered

Jobs SHALL process one source at a time in deterministic ascending source order, and SHALL NOT issue unbounded parallel provider calls regardless of plan size.

#### Scenario: processing order is deterministic and sequential

- **WHEN** a job plans more than one source and runs
- **THEN** the emitted progress lines name sources in ascending identifier order and each source's work commits before the next line appears
