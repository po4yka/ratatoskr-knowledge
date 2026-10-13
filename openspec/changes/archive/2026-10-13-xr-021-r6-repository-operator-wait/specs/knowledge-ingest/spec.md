## MODIFIED Requirements

### Requirement: Consumers verify their durables and classify failures

Each ingest consumer SHALL verify its Edge-provisioned durable, terminate permanently invalid input without redelivery, and negative-acknowledge transient failures with a delay. The repository family SHALL record a terminal failure without content after the fifth delivery of a plain transient fault, and SHALL NOT settle a failure after any number of deliveries when the provider refused the call for a reason an operator can fix (a rejected credential, an empty balance, a withdrawn model) or because a spend budget is exhausted; it SHALL redeliver those until the provider answers.

#### Scenario: Wrong producer is terminated

- **WHEN** a message with a producer that is not allowed for the family is delivered
- **THEN** it is terminated, never redelivered, and no analysis state is written

#### Scenario: A repository request waits out a rejected credential

- **WHEN** the provider rejects the credential on eight deliveries of a repository request and then answers
- **THEN** no failed fact is published at any time, one completed fact is published, the run's attempts are not spent and the redeliveries are counted

#### Scenario: A repository request waits out a spend-budget refusal

- **WHEN** the spend budget refuses the call on eight deliveries of a repository request and then clears
- **THEN** no failed fact is published and one completed fact is published

#### Scenario: A plain transient fault still settles at the fifth delivery

- **WHEN** a repository request fails with a transient fault on five deliveries
- **THEN** one retryable failed fact is published and the request is acknowledged
