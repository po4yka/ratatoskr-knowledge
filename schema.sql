create schema if not exists knowledge;

create extension if not exists vector;

create table if not exists knowledge.source_refs (
    source_ref_id uuid primary key,
    tenant_ref text not null,
    owner_context text not null,
    ai_archive_id text not null default '',
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
        ai_archive_id,
        source_document_id,
        content_digest_algorithm,
        content_digest_hex
    )
);

create index if not exists source_refs_ai_archive_idx
    on knowledge.source_refs (tenant_ref, ai_archive_id)
    where ai_archive_id <> '';

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
        'indexed',
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
    model text,
    provider_request_id text,
    raw_response jsonb,
    input_tokens bigint,
    output_tokens bigint,
    outcome text not null,
    validation_code text,
    duration_ms integer,
    http_status smallint,
    error_class text,
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
    ),
    constraint analysis_attempts_model_check
        check (model is null or char_length(model) between 1 and 128),
    constraint analysis_attempts_duration_check
        check (duration_ms is null or duration_ms >= 0),
    constraint analysis_attempts_status_check
        check (http_status is null or http_status between 100 and 599),
    constraint analysis_attempts_error_class_check check (error_class is null or error_class in (
        'timeout',
        'network',
        'rate_limited',
        'server_error',
        'auth_error',
        'request_invalid',
        'size_limit',
        'budget_exhausted',
        'unclassified'
    ))
);

create table if not exists knowledge.deletion_records (
    deletion_id uuid primary key,
    tenant_ref text not null,
    scope text not null,
    owner_context text,
    ai_archive_id text,
    source_document_id text,
    source_refs_deleted integer not null,
    analysis_runs_deleted integer not null,
    analysis_attempts_deleted integer not null,
    analysis_outputs_deleted integer not null,
    search_projection_inputs_deleted integer not null,
    search_documents_deleted integer not null,
    embedding_chunks_deleted integer not null,
    embedding_failures_deleted integer not null,
    tags_deleted integer not null default 0,
    taggings_deleted integer not null default 0,
    collections_deleted integer not null default 0,
    collection_items_deleted integer not null default 0,
    analysis_user_states_deleted integer not null default 0,
    highlights_deleted integer not null default 0,
    analysis_feedback_deleted integer not null default 0,
    blob_digests_removed integer not null,
    completed_at timestamptz not null default now(),
    constraint deletion_records_scope_check check (scope in ('tenant', 'source', 'archive'))
);

create index if not exists deletion_records_tenant_idx
    on knowledge.deletion_records (tenant_ref, completed_at desc);

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

create table if not exists knowledge.repository_analysis_requests (
    request_id uuid primary key,
    tenant_ref text not null,
    repository_id uuid not null,
    github_repository_numeric_id bigint not null,
    source_revision jsonb not null,
    repository_attributes jsonb not null,
    requested_contract text not null,
    idempotency_digest_hex text not null unique,
    state text not null default 'pending',
    analysis_result_ref text,
    failure_code text,
    retryable boolean,
    terminal_at timestamptz,
    created_at timestamptz not null default now(),
    constraint repository_analysis_numeric_id_check check (github_repository_numeric_id > 0),
    constraint repository_analysis_idempotency_digest_check
        check (idempotency_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint repository_analysis_state_check
        check (state in ('pending', 'completed', 'failed')),
    constraint repository_analysis_terminal_check check (
        (state = 'pending' and analysis_result_ref is null and failure_code is null and retryable is null and terminal_at is null)
        or (state = 'completed' and analysis_result_ref is not null and failure_code is null and retryable is null and terminal_at is not null)
        or (state = 'failed' and analysis_result_ref is null and failure_code is not null and retryable is not null and terminal_at is not null)
    )
);

-- At-least-once source deliveries are claimed before family-specific analysis starts. Snapshot
-- payloads are state-carried contract values, never a producer-table dependency.
create table if not exists knowledge.source_analysis_inbox (
    event_id uuid primary key,
    subject text not null,
    family text not null,
    tenant_ref text not null,
    source_id text not null,
    content_digest_hex text not null,
    observed_at timestamptz not null,
    snapshot jsonb not null,
    accepted_at timestamptz not null default now(),
    constraint source_analysis_inbox_subject_check check (subject in (
        'social.source.captured.v1', 'social.source.updated.v1',
        'ai_archive.conversation.added.v1', 'ai_archive.conversation.updated.v1',
        'ai_archive.project.added.v1', 'ai_archive.project.updated.v1',
        'ai_archive.subject.tombstoned.v1'
    )),
    constraint source_analysis_inbox_family_check check (family in ('social', 'ai_archive')),
    constraint source_analysis_inbox_digest_check check (content_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint source_analysis_inbox_snapshot_object_check check (jsonb_typeof(snapshot) = 'object')
);

create table if not exists knowledge.source_analysis_heads (
    family text not null,
    tenant_ref text not null,
    source_id text not null,
    content_digest_hex text not null,
    observed_at timestamptz not null,
    inbox_event_id uuid not null references knowledge.source_analysis_inbox (event_id),
    primary key (family, tenant_ref, source_id),
    constraint source_analysis_heads_family_check check (family in ('social', 'ai_archive')),
    constraint source_analysis_heads_digest_check check (content_digest_hex ~ '^[0-9a-f]{64}$')
);

create index if not exists repository_analysis_pending_idx
    on knowledge.repository_analysis_requests (tenant_ref, created_at)
    where state = 'pending';

create unique index if not exists one_accepted_output_per_run
    on knowledge.analysis_outputs(run_id)
    where accepted;

-- Knowledge-owned user content. All rows carry the tenant explicitly: callers must still prove
-- that the referenced analysis/source belongs to it before inserting or reading the row.
create table if not exists knowledge.tags (
    tag_id uuid primary key,
    tenant_ref text not null,
    normalized_name text not null,
    display_name text not null,
    created_at timestamptz not null default now(),
    constraint tags_normalized_name_check check (char_length(normalized_name) between 1 and 128),
    constraint tags_display_name_check check (char_length(display_name) between 1 and 128),
    constraint tags_tenant_normalized_name_key unique (tenant_ref, normalized_name)
);

create table if not exists knowledge.analysis_taggings (
    tag_id uuid not null references knowledge.tags(tag_id) on delete cascade,
    output_id uuid not null references knowledge.analysis_outputs(output_id) on delete cascade,
    tenant_ref text not null,
    created_at timestamptz not null default now(),
    primary key (tag_id, output_id)
);

create index if not exists analysis_taggings_tenant_output_idx
    on knowledge.analysis_taggings (tenant_ref, output_id);

create table if not exists knowledge.collections (
    collection_id uuid primary key,
    tenant_ref text not null,
    name text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint collections_name_check check (char_length(name) between 1 and 256)
);

create index if not exists collections_tenant_updated_idx
    on knowledge.collections (tenant_ref, updated_at desc);

create table if not exists knowledge.collection_items (
    collection_id uuid not null references knowledge.collections(collection_id) on delete cascade,
    position integer not null,
    tenant_ref text not null,
    output_id uuid references knowledge.analysis_outputs(output_id) on delete cascade,
    source_ref_id uuid references knowledge.source_refs(source_ref_id) on delete cascade,
    created_at timestamptz not null default now(),
    constraint collection_items_collection_position_key unique (collection_id, position)
        deferrable initially immediate,
    constraint collection_items_position_check check (position >= 0),
    constraint collection_items_exactly_one_target_check check (
        (output_id is not null)::integer + (source_ref_id is not null)::integer = 1
    ),
    constraint collection_items_unique_output unique (collection_id, output_id),
    constraint collection_items_unique_source unique (collection_id, source_ref_id)
);

create table if not exists knowledge.analysis_user_states (
    tenant_ref text not null,
    output_id uuid not null references knowledge.analysis_outputs(output_id) on delete cascade,
    read_state text not null default 'unread',
    favorite boolean not null default false,
    updated_at timestamptz not null default now(),
    primary key (tenant_ref, output_id),
    constraint analysis_user_states_read_state_check check (read_state in ('unread', 'read'))
);

create table if not exists knowledge.highlights (
    highlight_id uuid primary key,
    tenant_ref text not null,
    output_id uuid not null references knowledge.analysis_outputs(output_id) on delete cascade,
    source_ref_id uuid not null references knowledge.source_refs(source_ref_id) on delete cascade,
    block_id uuid not null,
    start_offset integer not null,
    end_offset integer not null,
    style text not null,
    created_at timestamptz not null default now(),
    constraint highlights_offsets_check check (start_offset >= 0 and end_offset > start_offset),
    constraint highlights_style_check check (style in ('yellow', 'green', 'blue', 'pink', 'purple', 'underline')),
    constraint highlights_anchor_key unique (tenant_ref, output_id, block_id, start_offset, end_offset, style)
);

create index if not exists highlights_tenant_output_idx
    on knowledge.highlights (tenant_ref, output_id, created_at);

create table if not exists knowledge.analysis_feedback (
    feedback_id uuid primary key,
    tenant_ref text not null,
    output_id uuid not null references knowledge.analysis_outputs(output_id) on delete cascade,
    issue_category text not null,
    detail text,
    created_at timestamptz not null default now(),
    constraint analysis_feedback_category_check check (issue_category in (
        'incorrect', 'missing_context', 'unsupported_claim', 'poor_quality', 'other'
    )),
    constraint analysis_feedback_detail_check check (detail is null or char_length(detail) between 1 and 2_000)
);

create index if not exists analysis_feedback_tenant_output_idx
    on knowledge.analysis_feedback (tenant_ref, output_id, created_at desc);

create table if not exists knowledge.search_projection_inputs (
    source_ref_id uuid primary key references knowledge.source_refs(source_ref_id),
    latest_output_id uuid not null references knowledge.analysis_outputs(output_id),
    tenant_ref text not null,
    owner_context text not null,
    document_id uuid not null,
    title text not null,
    lead text not null,
    body text not null,
    updated_at timestamptz not null,
    constraint search_projection_inputs_output_identity unique (latest_output_id)
);

create index if not exists search_projection_inputs_tenant_idx
    on knowledge.search_projection_inputs (tenant_ref, source_ref_id);

create table if not exists knowledge.search_documents (
    search_document_id uuid primary key,
    source_ref_id uuid not null references knowledge.source_refs(source_ref_id),
    latest_output_id uuid not null,
    tenant_ref text not null,
    owner_context text not null,
    document_id uuid not null,
    title text not null,
    lead text not null,
    body text not null,
    search_vector tsvector generated always as (
        setweight(to_tsvector('english', title), 'A')
        || setweight(to_tsvector('english', lead), 'B')
        || setweight(to_tsvector('english', body), 'C')
    ) stored,
    updated_at timestamptz not null,
    constraint search_documents_source_identity unique (source_ref_id)
);

create index if not exists search_documents_search_vector_idx
    on knowledge.search_documents using gin (search_vector);

create index if not exists search_documents_tenant_recency_idx
    on knowledge.search_documents (tenant_ref, updated_at desc);

create table if not exists knowledge.embedding_chunks (
    embedding_chunk_id uuid primary key,
    source_ref_id uuid not null references knowledge.source_refs(source_ref_id),
    output_id uuid not null references knowledge.analysis_outputs(output_id),
    tenant_ref text not null,
    owner_context text not null,
    document_id uuid not null,
    ordinal integer not null,
    chunk_text text not null,
    chunk_digest_hex text not null,
    chunking_version text not null,
    provider text not null,
    model text not null,
    dimensions integer not null,
    prompt_version text not null,
    embedding vector(1536) not null,
    created_at timestamptz not null default now(),
    constraint embedding_chunks_ordinal_check check (ordinal >= 0),
    constraint embedding_chunks_digest_check
        check (chunk_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint embedding_chunks_chunking_version_check
        check (char_length(chunking_version) between 1 and 64),
    constraint embedding_chunks_prompt_version_check
        check (char_length(prompt_version) between 1 and 64),
    constraint embedding_chunks_provider_check
        check (provider ~ '^[a-z][a-z0-9_-]{0,63}$'),
    constraint embedding_chunks_model_check
        check (char_length(model) between 1 and 128),
    constraint embedding_chunks_dimensions_check check (dimensions = 1536),
    constraint embedding_chunks_identity_key unique (
        source_ref_id,
        chunking_version,
        provider,
        model,
        prompt_version,
        ordinal
    )
);

create index if not exists embedding_chunks_embedding_hnsw_idx
    on knowledge.embedding_chunks using hnsw (embedding vector_cosine_ops);

create index if not exists embedding_chunks_identity_idx
    on knowledge.embedding_chunks (provider, model, prompt_version, chunking_version);

create table if not exists knowledge.embedding_failures (
    failure_id uuid primary key,
    source_ref_id uuid not null references knowledge.source_refs(source_ref_id),
    output_id uuid not null,
    tenant_ref text not null,
    chunking_version text not null,
    provider text not null,
    model text not null,
    prompt_version text not null,
    error_class text not null,
    attempt integer not null,
    detail_code text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint embedding_failures_chunking_version_check
        check (char_length(chunking_version) between 1 and 64),
    constraint embedding_failures_prompt_version_check
        check (char_length(prompt_version) between 1 and 64),
    constraint embedding_failures_provider_check
        check (provider ~ '^[a-z][a-z0-9_-]{0,63}$'),
    constraint embedding_failures_model_check
        check (char_length(model) between 1 and 128),
    constraint embedding_failures_class_check check (error_class in (
        'timeout',
        'network',
        'rate_limited',
        'server_error',
        'auth_error',
        'request_invalid',
        'size_limit',
        'budget_exhausted',
        'unclassified'
    )),
    constraint embedding_failures_attempt_check check (attempt >= 1),
    constraint embedding_failures_detail_code_check
        check (detail_code is null or char_length(detail_code) <= 128),
    constraint embedding_failures_identity_key unique (
        source_ref_id,
        chunking_version,
        provider,
        model,
        prompt_version
    )
);
