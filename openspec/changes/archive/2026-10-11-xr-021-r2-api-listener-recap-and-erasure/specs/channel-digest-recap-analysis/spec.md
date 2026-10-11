## ADDED Requirements

### Requirement: The recap does not default to the scripted provider

Knowledge SHALL default the recap provider mode to `openrouter` and require the provider API key when the recap is enabled. It SHALL accept `scripted` mode only with a loopback `nats://` bus endpoint and no credentials file, and SHALL log a warning naming the mode at startup.

#### Scenario: Enabling the recap alone is refused

- **WHEN** the recap is enabled without the provider API key and without an explicit mode
- **THEN** configuration loading fails naming `RATATOSKR__PROVIDER__OPENROUTER__API_KEY`

### Requirement: Recap redelivery is bounded and progress is acknowledged

Knowledge SHALL redeliver a transiently failed recap message with a delay of `min(2 s * 2^(delivered-1), 60 s)`, count each redelivery in `knowledge_channel_recap_redeliveries_total`, default the fetch batch to one and acknowledge progress every third of the ack wait while a message is being processed, so a handler longer than the ack wait does not cause redelivery.

#### Scenario: A slow handler is delivered once

- **WHEN** a handler takes longer than the durable's ack wait
- **THEN** the message is delivered exactly once
