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
    provider_replay_key text,
    provider_replay_authorized boolean not null default false,
    state text not null default 'queued',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint analysis_runs_state_check check (state in (
        'queued',
        'context_prepared',
        'model_requested',
        'provider_outcome_unknown',
        'response_received',
        'schema_validated',
        'repaired',
        'persisted',
        'indexed',
        'completed',
        'failed'
    )),
    constraint analysis_runs_replay_check check (
        (provider_replay_authorized and provider_replay_key is not null)
        or (not provider_replay_authorized)
    ),
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
    constraint analysis_attempts_ordinal_check check (
        ordinal between 1 and 2 or (ordinal = 3 and reason = 'operator_replay')
    ),
    constraint analysis_attempts_reason_check
        check (reason in ('initial', 'retry', 'repair', 'operator_replay')),
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

-- The primary event adapter acknowledges a JetStream delivery only after this receipt and its
-- work row commit together. The canonical envelope digest turns event-id reuse with another
-- immutable fact into an observable collision instead of a harmless duplicate.
create table if not exists knowledge.primary_event_receipts (
    event_id uuid primary key,
    subject text not null,
    envelope_digest_hex text not null,
    producer text not null,
    tenant_ref text not null,
    aggregate_id text not null,
    family text not null,
    accepted_at timestamptz not null default now(),
    constraint primary_event_receipts_subject_check check (subject in (
        'evt.content.document.extracted.v1',
        'evt.social.source.captured.v1', 'evt.social.source.updated.v1',
        'evt.social.source.removed.v1',
        'evt.ai_archive.archive.imported.v1',
        'evt.ai_archive.conversation.added.v1',
        'evt.ai_archive.conversation.updated.v1',
        'evt.ai_archive.project.added.v1', 'evt.ai_archive.project.updated.v1',
        'evt.ai_archive.artifact.added.v1', 'evt.ai_archive.artifact.updated.v1',
        'evt.ai_archive.subject.tombstoned.v1',
        'evt.knowledge.repository_analysis.requested.v1'
    )),
    constraint primary_event_receipts_digest_check
        check (envelope_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint primary_event_receipts_family_check
        check (family in ('document', 'social', 'ai_archive', 'repository'))
);

-- Invalid transport input is retained without its user-controlled payload. A rejection digest is
-- sufficient to count repeated poison deliveries without storing source content or diagnostics.
create table if not exists knowledge.primary_event_rejections (
    rejection_id uuid primary key,
    delivery_digest_hex text not null,
    transport_subject text not null,
    rejection_code text not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    occurrence_count integer not null default 1,
    constraint primary_event_rejections_digest_check
        check (delivery_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint primary_event_rejections_code_check check (rejection_code in (
        'transport_subject', 'envelope', 'event_type', 'producer', 'tenant',
        'aggregate', 'payload', 'event_id_collision'
    )),
    constraint primary_event_rejections_occurrence_check check (occurrence_count > 0),
    constraint primary_event_rejections_identity_key unique (
        delivery_digest_hex, transport_subject, rejection_code
    )
);

-- Work is independent of the JetStream acknowledgement window. A worker can reclaim any
-- non-terminal row after lease expiry and resumes from the explicit state. Provider uncertainty
-- is deliberately non-claimable until an operator-authorized requeue changes the state.
create table if not exists knowledge.analysis_work (
    work_id uuid primary key,
    event_id uuid not null unique references knowledge.primary_event_receipts(event_id),
    family text not null,
    tenant_ref text not null,
    source_key text not null,
    parent_source_key text not null,
    source_revision text not null,
    input_envelope jsonb not null,
    state text not null default 'admitted',
    attempt_count integer not null default 0,
    max_attempts integer not null default 2,
    next_eligible_at timestamptz not null default now(),
    lease_owner text,
    lease_expires_at timestamptz,
    provider_request_key text,
    analysis_run_id uuid references knowledge.analysis_runs(run_id),
    terminal_code text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint analysis_work_family_check
        check (family in ('document', 'social', 'ai_archive', 'repository')),
    constraint analysis_work_input_object_check check (jsonb_typeof(input_envelope) = 'object'),
    constraint analysis_work_state_check check (state in (
        'admitted', 'preparing', 'provider_pending', 'provider_outcome_unknown',
        'response_received', 'persisting', 'retry_wait', 'completed', 'failed', 'suppressed'
    )),
    constraint analysis_work_attempt_check
        check (attempt_count between 0 and max_attempts and max_attempts between 1 and 8),
    constraint analysis_work_lease_check check (
        (lease_owner is null and lease_expires_at is null)
        or (lease_owner is not null and lease_expires_at is not null)
    ),
    constraint analysis_work_terminal_check check (
        (state in ('failed', 'suppressed') and terminal_code is not null)
        or (state not in ('failed', 'suppressed') and terminal_code is null)
    ),
    constraint analysis_work_logical_revision_key unique (
        tenant_ref, family, source_key, source_revision
    )
);

create index if not exists analysis_work_claim_idx
    on knowledge.analysis_work (next_eligible_at, created_at)
    where state in (
        'admitted', 'preparing', 'provider_pending', 'response_received',
        'persisting', 'retry_wait'
    );

-- Authoritative source ordering is retained independently of analysis artifacts. A removed or
-- tombstoned head remains after derived deletion, preventing stale replay from resurrecting data.
create table if not exists knowledge.primary_source_heads (
    family text not null,
    tenant_ref text not null,
    source_key text not null,
    revision text not null,
    observed_at timestamptz not null,
    lifecycle text not null,
    event_id uuid not null references knowledge.primary_event_receipts(event_id),
    primary key (family, tenant_ref, source_key),
    constraint primary_source_heads_family_check
        check (family in ('document', 'social', 'ai_archive', 'repository')),
    constraint primary_source_heads_lifecycle_check check (lifecycle in ('active', 'removed'))
);

-- Import, Artifact, and tombstone facts change Knowledge-owned source state without invoking a
-- provider. Their typed envelope is retained here instead of being ACKed into a digest-only row.
create table if not exists knowledge.primary_source_state (
    family text not null,
    tenant_ref text not null,
    source_key text not null,
    event_id uuid not null references knowledge.primary_event_receipts(event_id),
    lifecycle text not null,
    input_envelope jsonb not null,
    updated_at timestamptz not null default now(),
    primary key (family, tenant_ref, source_key),
    constraint primary_source_state_lifecycle_check check (lifecycle in ('active', 'removed')),
    constraint primary_source_state_envelope_check check (jsonb_typeof(input_envelope) = 'object')
);

-- Terminal state and its publication intent are inserted in the same transaction. The publisher
-- uses message_id as Nats-Msg-Id and marks sent only after the JetStream publish acknowledgement.
create table if not exists knowledge.knowledge_outbox (
    outbox_id uuid primary key,
    work_id uuid not null references knowledge.analysis_work(work_id),
    event_type text not null,
    subject text not null,
    envelope jsonb not null,
    message_id uuid not null unique,
    created_at timestamptz not null default now(),
    publish_attempts integer not null default 0,
    next_attempt_at timestamptz not null default now(),
    published_at timestamptz,
    constraint knowledge_outbox_subject_check check (subject in (
        'evt.knowledge.analysis.completed.v1',
        'evt.knowledge.ai_archive_analysis.completed.v1',
        'evt.knowledge.repository_analysis.completed.v1',
        'evt.knowledge.repository_analysis.failed.v1',
        'evt.knowledge.channel_digest_recap.completed.v1',
        'evt.knowledge.channel_digest_recap.failed.v1'
    )),
    constraint knowledge_outbox_payload_check check (jsonb_typeof(envelope) = 'object'),
    constraint knowledge_outbox_attempt_check check (publish_attempts >= 0),
    constraint knowledge_outbox_logical_key unique (work_id, event_type)
);

create index if not exists knowledge_outbox_pending_idx
    on knowledge.knowledge_outbox (next_attempt_at, created_at)
    where published_at is null;

-- At-least-once source deliveries are claimed before family-specific analysis starts. Snapshot
-- payloads are state-carried contract values, never a producer-table dependency.
create table if not exists knowledge.source_analysis_inbox (
    event_id uuid primary key,
    subject text not null,
    family text not null,
    tenant_ref text not null,
    archive_id text,
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

create index if not exists source_analysis_inbox_archive_idx
    on knowledge.source_analysis_inbox (tenant_ref, archive_id, source_id)
    where archive_id is not null;

-- Channel recap commands are retained as content-free typed receipts. The natural request identity
-- converges redeliveries carrying a different transport command id.
create table if not exists knowledge.channel_recap_inbox (
    command_id uuid primary key,
    owner_ref text not null,
    operation_id uuid not null,
    digest_run_id uuid not null,
    manifest_ref text not null,
    manifest_digest_hex text not null,
    window_start_at timestamptz not null,
    window_end_at timestamptz not null,
    source_count integer not null,
    channel_count integer not null,
    analysis_family text not null,
    analysis_contract text not null,
    output_language text not null,
    request_payload jsonb not null,
    accepted_at timestamptz not null default now(),
    constraint channel_recap_inbox_owner_check
        check (owner_ref ~ '^user:[0-9a-f-]{36}$'),
    constraint channel_recap_inbox_manifest_ref_check
        check (manifest_ref ~ '^channel-digest-manifest:[0-9a-f-]{36}$'),
    constraint channel_recap_inbox_manifest_digest_check
        check (manifest_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint channel_recap_inbox_window_check
        check (window_start_at < window_end_at and window_end_at <= window_start_at + interval '7 days'),
    constraint channel_recap_inbox_count_check
        check (source_count between 1 and 100 and channel_count between 1 and 20
            and channel_count <= source_count),
    constraint channel_recap_inbox_family_check
        check (analysis_family = 'channel_digest_recap'),
    constraint channel_recap_inbox_contract_check
        check (analysis_contract = 'channel_digest_recap.v1'),
    constraint channel_recap_inbox_language_check
        check (output_language in ('ru', 'en')),
    constraint channel_recap_inbox_payload_object_check
        check (jsonb_typeof(request_payload) = 'object'),
    constraint channel_recap_inbox_semantic_key unique (
        owner_ref,
        digest_run_id,
        manifest_digest_hex,
        analysis_contract,
        output_language
    )
);

create table if not exists knowledge.channel_recap_runs (
    recap_run_id uuid primary key,
    inbox_command_id uuid not null unique
        references knowledge.channel_recap_inbox(command_id),
    owner_ref text not null,
    digest_run_id uuid not null,
    manifest_digest_hex text not null,
    analysis_family text not null,
    analysis_contract text not null,
    prompt_version text not null,
    context_version text not null,
    output_language text not null,
    state text not null default 'received',
    manifest_attempt_count smallint not null default 0,
    manifest_retry_not_before timestamptz,
    attempt_count smallint not null default 0,
    failure_code text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint channel_recap_runs_state_check check (state in (
        'received', 'manifest_retry', 'manifest_verified', 'context_prepared',
        'model_requested', 'response_received', 'schema_validated', 'repaired',
        'persisted', 'completed', 'failed'
    )),
    constraint channel_recap_runs_attempt_check check (attempt_count between 0 and 2),
    constraint channel_recap_runs_manifest_attempt_check check (
        manifest_attempt_count between 0 and 2
        and ((state = 'manifest_retry') = (manifest_retry_not_before is not null))
    ),
    constraint channel_recap_runs_terminal_check check (
        (state = 'failed' and failure_code is not null)
        or (state <> 'failed' and failure_code is null)
    ),
    constraint channel_recap_runs_identity_key unique (
        owner_ref,
        digest_run_id,
        manifest_digest_hex,
        analysis_contract,
        prompt_version,
        context_version,
        output_language
    )
);

-- Exact accepted source evidence is committed before provider work. The JSON value is bounded by
-- the source client and verifier; the scalar columns preserve the natural immutable identity.
create table if not exists knowledge.channel_recap_manifests (
    recap_run_id uuid primary key references knowledge.channel_recap_runs(recap_run_id),
    owner_ref text not null,
    digest_run_id uuid not null,
    manifest_ref text not null,
    manifest_digest_hex text not null,
    window_start_at timestamptz not null,
    window_end_at timestamptz not null,
    source_count integer not null,
    channel_count integer not null,
    manifest jsonb not null,
    accepted_at timestamptz not null default now(),
    constraint channel_recap_manifests_digest_check
        check (manifest_digest_hex ~ '^[0-9a-f]{64}$'),
    constraint channel_recap_manifests_window_check check (window_start_at < window_end_at),
    constraint channel_recap_manifests_count_check check (
        source_count between 1 and 100 and channel_count between 1 and 20
        and channel_count <= source_count
    ),
    constraint channel_recap_manifests_object_check check (jsonb_typeof(manifest) = 'object'),
    constraint channel_recap_manifests_identity_key unique (
        owner_ref, digest_run_id, manifest_ref, manifest_digest_hex
    )
);

create table if not exists knowledge.channel_recap_attempts (
    recap_run_id uuid not null references knowledge.channel_recap_runs(recap_run_id),
    ordinal smallint not null,
    reason text not null,
    provider text not null,
    model text not null,
    provider_request_id text,
    raw_response jsonb,
    raw_response_digest_hex text,
    input_tokens bigint,
    output_tokens bigint,
    outcome text not null,
    validation_code text,
    failure_class text,
    duration_ms integer not null,
    created_at timestamptz not null default now(),
    primary key (recap_run_id, ordinal),
    constraint channel_recap_attempts_ordinal_check check (ordinal between 1 and 2),
    constraint channel_recap_attempts_reason_check check (reason in ('initial', 'retry', 'repair')),
    constraint channel_recap_attempts_outcome_check check (outcome in (
        'requested', 'response_received', 'accepted', 'invalid',
        'transient_failure', 'permanent_failure'
    )),
    constraint channel_recap_attempts_digest_check check (
        raw_response_digest_hex is null or raw_response_digest_hex ~ '^[0-9a-f]{64}$'
    ),
    constraint channel_recap_attempts_raw_link_check check (
        (raw_response is null) = (raw_response_digest_hex is null)
    ),
    constraint channel_recap_attempts_usage_check check (
        (input_tokens is null or input_tokens >= 0) and (output_tokens is null or output_tokens >= 0)
    ),
    constraint channel_recap_attempts_duration_check check (duration_ms >= 0),
    constraint channel_recap_attempts_validation_check check (
        validation_code is null or validation_code in ('json_syntax', 'schema', 'grounding')
    ),
    constraint channel_recap_attempts_failure_check check (
        failure_class is null or failure_class in (
            'timeout', 'network', 'rate_limited', 'server_error', 'auth_error',
            'request_invalid', 'size_limit', 'budget_exhausted', 'unclassified'
        )
    )
);

create table if not exists knowledge.channel_recap_results (
    result_id uuid primary key,
    recap_run_id uuid not null unique references knowledge.channel_recap_runs(recap_run_id),
    result jsonb not null,
    result_digest_hex text not null,
    coverage jsonb not null,
    created_at timestamptz not null default now(),
    constraint channel_recap_results_result_object_check check (jsonb_typeof(result) = 'object'),
    constraint channel_recap_results_coverage_object_check check (jsonb_typeof(coverage) = 'object'),
    constraint channel_recap_results_digest_check check (result_digest_hex ~ '^[0-9a-f]{64}$')
);

create table if not exists knowledge.channel_recap_outbox (
    outbox_id uuid primary key,
    recap_run_id uuid not null unique references knowledge.channel_recap_runs(recap_run_id),
    subject text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    published_at timestamptz,
    constraint channel_recap_outbox_subject_check check (subject in (
        'knowledge.channel_digest_recap.completed.v1',
        'knowledge.channel_digest_recap.failed.v1'
    )),
    constraint channel_recap_outbox_payload_object_check check (jsonb_typeof(payload) = 'object')
);

-- Authoritative archive deletion facts are retained after their derived source snapshots are
-- removed so an old at-least-once conversation delivery cannot recreate a tombstoned projection.
create table if not exists knowledge.ai_archive_tombstones (
    event_id uuid primary key,
    tenant_ref text not null,
    archive_id text not null,
    subject_kind text not null,
    subject_id text,
    observed_at timestamptz not null,
    constraint ai_archive_tombstones_subject_kind_check
        check (subject_kind in ('archive', 'conversation', 'project', 'artifact')),
    constraint ai_archive_tombstones_subject_id_check
        check ((subject_kind = 'archive' and subject_id is null)
            or (subject_kind <> 'archive' and subject_id is not null))
);

create index if not exists ai_archive_tombstones_lookup_idx
    on knowledge.ai_archive_tombstones (tenant_ref, subject_kind, subject_id, observed_at desc);

-- Project and Artifact state is retained as a contract receipt. These objects do not enter the
-- conversation analysis family until Knowledge owns an authorized byte resolver and a typed
-- analysis contract for them.
create table if not exists knowledge.ai_archive_object_inbox (
    event_id uuid primary key,
    subject text not null,
    tenant_ref text not null,
    archive_id text not null,
    object_kind text not null,
    object_id text not null,
    observed_at timestamptz not null,
    payload jsonb not null,
    accepted_at timestamptz not null default now(),
    constraint ai_archive_object_inbox_subject_check check (subject in (
        'ai_archive.archive.imported.v1',
        'ai_archive.project.added.v1', 'ai_archive.project.updated.v1',
        'ai_archive.artifact.added.v1', 'ai_archive.artifact.updated.v1'
    )),
    constraint ai_archive_object_inbox_kind_check check (object_kind in ('archive', 'project', 'artifact')),
    constraint ai_archive_object_inbox_payload_object_check check (jsonb_typeof(payload) = 'object')
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
