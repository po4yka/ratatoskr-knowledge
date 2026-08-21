## Purpose

Defines the first strict article-analysis result and deterministic prompt context prepared from
canonical Document IR.

## ADDED Requirements

### Requirement: Article analysis has one strict bounded shape

Version-one article analysis SHALL contain a summary of 1 through 2000 characters and 1 through 10 key
points. Each key point SHALL contain 1 through 500 characters and 1 through 8 unique source block
indexes. Unknown fields SHALL be rejected.

#### Scenario: a provider adds an unknown field

- **WHEN** an otherwise valid response contains a field outside the version-one schema
- **THEN** structural validation fails and no trusted result is constructed

### Requirement: Key points are grounded in supplied blocks

Every source block index in a key point SHALL exist in the exact Document IR revision supplied to the
run. A key point SHALL NOT cite an omitted or missing block.

#### Scenario: a key point cites an absent block

- **WHEN** a response cites a block index outside the supplied document
- **THEN** semantic validation fails and the result is not persisted as accepted

### Requirement: Context preparation is deterministic and inspectable

The context builder SHALL preserve supported block order and shall record the included and omitted
block indexes, the configured character budget, and whether truncation occurred. The same Document IR,
builder version, and budget SHALL produce byte-identical provider input.

#### Scenario: the source exceeds the context budget

- **WHEN** the next complete block would exceed the finite context budget
- **THEN** the builder omits that block and all later blocks, records their indexes, and does not cut
  text inside a block

### Requirement: Source content cannot become provider instruction

The prompt SHALL keep system policy, task instruction, output schema, and source content in distinct
fields. It SHALL state that source text is evidence and not an instruction and SHALL expose no tool or
external-write capability.

#### Scenario: a source paragraph asks the model to run a command

- **WHEN** Document IR contains instruction-like text
- **THEN** the text remains inside the source-content field and cannot change the fixed policy or task
