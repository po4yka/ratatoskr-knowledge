# Knowledge testing and evaluation

## Deterministic tests

State transitions, idempotent redelivery, context construction, schema validation/repair bounds, citation resolution, usage accounting, migrations, FTS/vector filters, deletion propagation, and provider error mapping.

## Evaluation sets

Maintain synthetic/licensed examples by analysis family. Measure schema validity, groundedness, citation correctness, unsupported claims, omissions, language quality, retrieval precision/recall, latency, tokens, and cost.

## Security tests

Prompt-injection corpora, cross-owner isolation, malicious metadata, oversized context, provider leakage, logging redaction, and budget bypass.

## Gates

Provider/prompt/model/chunking changes require before/after reports and explicit thresholds. Default CI uses deterministic fakes; optional provider evaluations are isolated and budgeted. Workspace tests cover Extractor/source -> Knowledge -> Platform retrieval.
