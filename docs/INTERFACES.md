# Knowledge interfaces

## Current process interface

The loopback admin listener exposes only:

- `GET /live`;
- `GET /ready`;
- `GET /metrics`;
- `GET /version`.

Every response uses `Cache-Control: no-store`. There is no public or internal analysis HTTP route.
The process accepts strict `RATATOSKR__` configuration and a `check-config` command that does not
bind a socket.

## Current library interfaces

- `prepare_context` consumes canonical Document IR and keeps complete blocks in source order.
- `build_generation_request` separates fixed policy, task text, untrusted source content, and the
  generated output schema.
- `LlmProvider` is the narrow structured-generation seam. Only `ScriptedProvider` exists.
- `ArticlePipeline` records attempts, stores raw bytes before parsing, validates the result, and
  persists one accepted output.
- `BlobStore` owns content-addressed raw responses and returns contract `BlobRef` values.

There are no commands, events, search queries, model SDK adapters, or external network requests in
this slice. Those interfaces need their own changesets.
