# Knowledge testing and evaluation

## Deterministic tests

State transitions, idempotent redelivery, context construction, schema validation/repair bounds, citation resolution, usage accounting, migrations, FTS/vector filters, deletion propagation, and provider error mapping.

## Evaluation sets

Maintain synthetic/licensed examples by analysis family. Measure schema validity, groundedness, citation correctness, unsupported claims, omissions, language quality, retrieval precision/recall, latency, tokens, and cost.

## Security tests

Prompt-injection corpora, cross-owner isolation, malicious metadata, oversized context, provider leakage, logging redaction, and budget bypass.

## Gates

Provider/prompt/model/chunking changes require before/after reports and explicit thresholds. Default CI uses deterministic fakes; optional provider evaluations are isolated and budgeted. Workspace tests cover Extractor/source -> Knowledge -> Platform retrieval.

## Test-first

A change is planned before it is built, and the plan is a task list in which behaviour arrives in
pairs: one task adds a failing test, the next makes it pass. `openspec/config.yaml` carries that
rule, which is what puts it into every planning and implementation request rather than only into this
document.

The loop:

1. Write the test the scenario names. Run it. Confirm it fails, and read the failure — a test that
   fails because it does not compile has proved nothing about the behaviour.
2. Write the smallest change that makes it pass. Run it again.
3. Refactor only once it is green, adding no test and changing no behaviour.

Two checks stand behind this, and neither of them can see the order:

- `openspec validate --archived`, in `.github/workflows/openspec.yml`, fails when a change was
  archived with a task left unticked.
- A step in `.github/workflows/fleet.yml` fails when this repository holds a manifest and a `ci.yml`
  that never runs a test.

`ratatoskr-workspace/docs/QUALITY_GATES.md` records why the order itself is not checkable, rather
than leaving the gap to be discovered.
