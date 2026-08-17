# Security Policy for Ratatoskr Knowledge

> Status: Proposed  
> Last reviewed: 2026-08-17

Report vulnerabilities privately. Do not publish prompts containing private source data, model credentials, personal archives, retrieval results, or production traces.

Security review is required for provider adapters, prompt/context construction, tool calls, retrieval authorization, model routing, data export, embeddings, logging, redaction, and deletion.

Baseline:

- Treat every source document and retrieved passage as untrusted data.
- Never let source text grant tools, reveal secrets, or override system policy.
- Enforce owner/tenant filters before ranking or returning results.
- Minimize provider data, redact telemetry, encrypt credentials, and audit outbound inference.
- Validate structured output and bound repair/retry/cost.
