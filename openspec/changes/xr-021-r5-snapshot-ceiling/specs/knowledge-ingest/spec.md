## ADDED Requirements

### Requirement: A snapshot as large as the largest producer fact is stored and analysed

The blob store SHALL be bounded by `RATATOSKR__LIMITS__BLOB_BYTES` alone, and `RATATOSKR__LIMITS__RAW_RESPONSE_BYTES` SHALL bound only one provider HTTP response. Configuration SHALL refuse a `blob_bytes` below 8388608 and a `blob_bytes` below `raw_response_bytes`.

#### Scenario: A large conversation is analysed

- **WHEN** an AI-archive conversation whose snapshot serialises to 2 MiB, or to 7.9 million bytes, is accepted with the default `blob_bytes`
- **THEN** the snapshot is stored, the conversation is analysed and it is returned by library search

#### Scenario: A snapshot above the ceiling is rejected

- **WHEN** a snapshot is larger than `blob_bytes`
- **THEN** it is rejected with a size class and leaves no partial row

#### Scenario: A real broker delivers a 2 MiB conversation

- **WHEN** a 2 MiB conversation fact is published with the ChatGPT identity under the deployed ACL
- **THEN** it reaches `search_documents`

#### Scenario: The snapshot floor

- **WHEN** `RATATOSKR__LIMITS__BLOB_BYTES` is 8388607
- **THEN** configuration is refused with a message that names both limits, and 8388608 is accepted

#### Scenario: A ceiling below the response cap

- **WHEN** `RATATOSKR__LIMITS__BLOB_BYTES` is below `RATATOSKR__LIMITS__RAW_RESPONSE_BYTES`
- **THEN** configuration is refused

#### Scenario: A provider response stays bounded

- **WHEN** a provider answers with more than `raw_response_bytes`
- **THEN** the adapter cuts the response before it is stored
