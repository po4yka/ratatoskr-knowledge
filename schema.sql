create schema if not exists knowledge;

create table if not exists knowledge.source_refs (
    source_ref_id uuid primary key,
    tenant_ref text not null,
    owner_context text not null,
    source_document_id text not null,
    content_digest_algorithm text not null,
    content_digest_hex text not null,
    source_blob jsonb not null,
    created_at timestamptz not null default now(),
    constraint source_refs_owner_context_check
        check (owner_context ~ '^[a-z][a-z0-9-]{1,63}$'),
    constraint source_refs_digest_algorithm_check
        check (content_digest_algorithm = 'sha256'),
    constraint source_refs_digest_hex_check
        check (content_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint source_refs_identity_key unique (
        tenant_ref,
        owner_context,
        source_document_id,
        content_digest_algorithm,
        content_digest_hex
    )
);

create table if not exists knowledge.analysis_runs (
    run_id uuid primary key,
    source_ref_id uuid not null references knowledge.source_refs(source_ref_id),
    contract_version text not null,
    prompt_version text not null,
    context_builder_version text not null,
    model_policy text not null,
    state text not null default 'queued',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint analysis_runs_state_check check (state in (
        'queued',
        'context_prepared',
        'model_requested',
        'response_received',
        'schema_validated',
        'repaired',
        'persisted',
        'completed',
        'failed'
    )),
    constraint analysis_runs_identity_key unique (
        source_ref_id,
        contract_version,
        prompt_version,
        context_builder_version,
        model_policy
    )
);

create table if not exists knowledge.analysis_attempts (
    run_id uuid not null references knowledge.analysis_runs(run_id),
    ordinal smallint not null,
    reason text not null,
    provider text not null,
    model_policy text not null,
    provider_request_id text,
    raw_response jsonb,
    input_tokens bigint,
    output_tokens bigint,
    outcome text not null,
    validation_code text,
    created_at timestamptz not null default now(),
    primary key (run_id, ordinal),
    constraint analysis_attempts_ordinal_check check (ordinal between 1 and 2),
    constraint analysis_attempts_reason_check check (reason in ('initial', 'retry', 'repair')),
    constraint analysis_attempts_outcome_check check (outcome in (
        'requested',
        'transient_failure',
        'permanent_failure',
        'response_received',
        'invalid',
        'accepted'
    )),
    constraint analysis_attempts_usage_check check (
        (input_tokens is null or input_tokens >= 0)
        and (output_tokens is null or output_tokens >= 0)
    )
);

create table if not exists knowledge.provider_usage (
    usage_id uuid primary key,
    provider text not null,
    model text not null,
    input_tokens bigint not null,
    output_tokens bigint not null,
    estimated_cost_micro_usd bigint not null,
    recorded_at timestamptz not null default now(),
    constraint provider_usage_tokens_check check (
        input_tokens >= 0 and output_tokens >= 0
    ),
    constraint provider_usage_cost_check check (estimated_cost_micro_usd >= 0),
    constraint provider_usage_model_check check (char_length(model) between 1 and 128),
    constraint provider_usage_provider_check check (provider ~ '^[a-z][a-z0-9_-]{0,63}$')
);

create index if not exists provider_usage_window_idx
    on knowledge.provider_usage (provider, recorded_at);

create table if not exists knowledge.analysis_outputs (
    output_id uuid primary key,
    run_id uuid not null references knowledge.analysis_runs(run_id),
    result jsonb not null,
    raw_response jsonb not null,
    accepted boolean not null default true,
    created_at timestamptz not null default now(),
    constraint analysis_outputs_result_object_check check (jsonb_typeof(result) = 'object')
);

create unique index if not exists one_accepted_output_per_run
    on knowledge.analysis_outputs(run_id)
    where accepted;
