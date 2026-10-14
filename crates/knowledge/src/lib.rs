#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod archive_events;
mod article;
mod blob_registry;
mod blob_store;
mod budget;
mod channel_digest_recap;
mod channel_digest_recap_context;
mod channel_digest_recap_evaluation;
mod channel_digest_recap_pipeline;
mod channel_digest_recap_prompt;
mod channel_digest_recap_result;
mod channel_digest_recap_store;
mod chunking;
mod config;
mod config_api;
mod config_ingest;
mod config_recap;
mod context;
mod controlled;
mod database;
mod deletion;
mod deletion_ack;
mod deletion_audit;
mod deletion_blobs;
mod deletion_recap;
mod deletion_state;
mod deterministic_provider;
mod document_intake;
mod embeddings;
mod evaluation;
mod event_outbox;
mod family_analysis;
mod family_pipeline;
mod github_readme;
mod indexer;
mod note_analysis;
mod notes_inbox;
mod openrouter;
mod pipeline;
mod provider;
mod provider_secret;
mod rate_limit;
mod reindex;
mod repository_analysis;
mod result_reader_secret;
mod run_driver;
mod run_lock;
mod runs;
pub mod search;
mod source_inbox;
mod telemetry;
mod user_content;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use archive_events::{ArchiveEventAdmission, ArchiveEventConsumer, ArchiveEventError};
pub use article::{
    ArticleAnalysis, ArticleValidationError, KeyPoint, article_analysis_schema,
    validate_article_citations, validate_article_json,
};
pub use blob_store::{BlobError, BlobStore};
pub use budget::{BudgetError, BudgetLedger, BudgetLimits, BudgetWindow, TokenPrices};
pub use channel_digest_recap::{
    DigestManifestAttemptError, DigestManifestAttemptOutcome, DigestManifestError,
    DigestManifestRequest, DigestSourceClient, DigestSourceClientError, DigestSourceClientSettings,
    DigestSourceSecret, VerifiedDigestManifest, attempt_digest_manifest, verify_digest_manifest,
};
pub use channel_digest_recap_context::{
    CHANNEL_RECAP_CONTEXT_VERSION, ChannelRecapContextError, ChannelRecapContextPolicy,
    ChannelRecapOmission, ChannelRecapOmissionReason, PreparedChannelRecapContext,
    PreparedChannelRecapSource, prepare_channel_recap_context,
};
pub use channel_digest_recap_evaluation::{
    ChannelRecapEvalCaseReport, ChannelRecapEvalCheck, ChannelRecapEvaluationError,
    ChannelRecapEvaluationReport, run_committed_channel_recap_evaluation,
};
pub use channel_digest_recap_pipeline::{ChannelRecapPipeline, ChannelRecapPipelineError};
pub use channel_digest_recap_prompt::{
    CHANNEL_RECAP_PROMPT_VERSION, ChannelRecapGenerationError, ChannelRecapProviderRequest,
    ChannelRecapSourceContent, ChannelRecapSourceLabel, build_channel_recap_provider_request,
};
pub use channel_digest_recap_result::{
    ChannelDigestRecap, ChannelRecapCitation, ChannelRecapContextVersion,
    ChannelRecapContractVersion, ChannelRecapCoverage, ChannelRecapModelOutput,
    ChannelRecapNotableItem, ChannelRecapOutputLanguage, ChannelRecapPromptVersion,
    ChannelRecapResultError, ChannelRecapTopic, ChannelRecapWarning, channel_digest_recap_schema,
    channel_recap_model_output_schema, compose_channel_digest_recap, validate_channel_digest_recap,
    validate_channel_recap_model_output,
};
pub use channel_digest_recap_store::{
    ChannelRecapAdmissionError, ChannelRecapInbox, ChannelRecapInboxAdmission,
    ChannelRecapInboxError, ChannelRecapResultProjection, ChannelRecapResultReadError,
    ChannelRecapRunError, ChannelRecapRunState, ChannelRecapRunStore, admit_channel_digest_recap,
};
pub use chunking::{CHUNKING_VERSION, Chunk, ChunkPolicy, ChunkPolicyError, chunk_article};
pub use config::{
    AdminConfig, BlobsConfig, CHANNEL_RECAP_ACK_WAIT_SECONDS, ChannelRecapConfig,
    ChannelRecapProviderMode, Config, ConfigError, Limits, MIN_SNAPSHOT_BLOB_BYTES, StorageConfig,
};
pub use config_api::ApiConfig;
pub use config_ingest::{INGEST_ACK_WAIT_SECONDS, IngestConfig, IngestProviderMode};
pub use context::{
    ContextError, GenerationRequest, PreparedContext, build_generation_request, prepare_context,
};
pub use controlled::{ControlledProvider, SpendControls};
pub use database::{Database, PersistenceError};
pub use deletion::{
    DeletionCounts, DeletionError, DeletionReceipt, DeletionScope, delete_source, delete_tenant,
    execute_deletion, verify_scope_empty,
};
pub use deletion_ack::{DELETION_ACK_REARM_AFTER, DeletionAcknowledgement};
pub use deletion_blobs::collect_pending_blobs;
pub use deterministic_provider::ExtractiveArticleProvider;
pub use document_intake::{DocumentIntake, DocumentIntakeError, DocumentIntakeOutcome};
pub use embeddings::{
    ControlledEmbeddings, EmbeddingIdentity, EmbeddingProvider, EmbeddingResponse,
    EmbeddingsSettings, EmbeddingsWireError, OpenAiCompatibleEmbeddings, ScriptedEmbeddingProvider,
    ScriptedEmbeddingSuccess, embeddings_request_body, parse_embeddings_envelope,
};
pub use evaluation::{
    CaseScore, CheckOutcome, EvalCase, EvalExpectations, EvalSource, EvaluationError,
    EvaluationReport, ResponseSet, SetScore, load_case_bytes, load_cases, render_report,
    run_committed_evaluation, run_offline, score_case, score_response_sets,
};
pub use event_outbox::{
    EnqueueOutcome, EventCause, EventOutbox, EventOutboxError, OutboxFact, OutboxRow, OutboxSubject,
};
pub use family_analysis::{
    ArchiveAnalysis, ArchiveDecision, ArchiveProjectAnalysis, FamilyValidationError,
    RepositoryAnalysis, RepositoryReadmeEvidence, SocialAnalysis, SocialConfidence,
    archive_analysis_schema, archive_context, archive_generation_request,
    archive_project_analysis_schema, archive_project_context, archive_project_generation_request,
    repository_analysis_schema, repository_context, repository_generation_request,
    social_analysis_schema, social_context, social_generation_request, validate_archive_analysis,
    validate_archive_project_analysis, validate_repository_analysis, validate_social_analysis,
};
pub use family_pipeline::{
    ArchiveAnalysisExecution, FamilyPipeline, FamilyPipelineError, RepositoryAnalysisExecution,
    RepositoryReadmeError, RepositoryReadmeResolver,
};
pub use github_readme::{GithubReadmeClient, GithubReadmeClientError, GithubReadmeClientSettings};
pub use indexer::{
    EMBEDDING_STORAGE_DIMENSIONS, EmbeddingWrite, Indexer, IndexerLimits, IndexingIdentity,
    IndexingOutcome, IndexingTarget, PendingSource, failure_attempt_count, pending_indexing_batch,
    record_indexing_failure, store_embeddings,
};
pub use note_analysis::{
    NOTES_OWNER, NoteAnalysis, NoteBlobError, NoteRevision, is_markdown, note_analysis_schema,
    note_context, note_generation_request, read_note_blob, validate_note_analysis,
};
pub use notes_inbox::{NoteAdmission, NoteChange, NotesInbox};
pub use openrouter::{
    OpenRouterProvider, OpenRouterSettings, OpenRouterWireError, RetryPolicy, chat_completion_body,
    classify_error, parse_success_envelope,
};
pub use pipeline::{ArticlePipeline, PipelineError};
pub use provider::{
    LlmProvider, ProviderError, ProviderFailure, ProviderFailureClass, ProviderIdentity,
    ProviderResponse, ProviderUsage, ScriptedProvider,
};
pub use provider_secret::ProviderSecret;
pub use rate_limit::RateLimiter;
pub use reindex::{
    ReindexScope, ReindexSourceOutcome, ReindexSummary, execute_reindex, plan_reindex,
    rebuild_search_documents,
};
pub use repository_analysis::{
    RepositoryAnalysisAdmission, RepositoryAnalysisConsumer, RepositoryAnalysisError,
};
pub use result_reader_secret::ResultReaderSecret;
pub use runs::{
    AnalysisIdentity, AnalysisRun, Attempt, AttemptInput, AttemptOutcome, AttemptReason, RunState,
    SourceReference, SourceRevision,
};
pub use search::{
    HybridRetriever, RankingPath, ReadStateFilter, SearchDocumentProjection, SearchError,
    SearchPage, SearchQuery, SearchResult, SemanticLeg, hybrid_search_page, record_search_document,
    record_search_projection_input, search_page,
};
pub use source_inbox::{
    ArchiveConversationSource, ArchiveProjectSource, SourceInbox, SourceInboxAdmission,
    SourceInboxError,
};
pub use telemetry::{TelemetryError, ValidationClass, init_telemetry, record_validation_failure};
pub use user_content::{
    AnalysisState, CollectionItem, CollectionTarget, FeedbackCategory, HighlightAnchor,
    HighlightStyle, ReadState, TagName, UserContentError, add_collection_item, create_collection,
    create_highlight, create_tag, list_collection_items, merge_tags, move_collection_item,
    record_feedback, set_analysis_state, set_read_state, tag_analysis, tag_name,
    validate_highlight_anchor,
};
