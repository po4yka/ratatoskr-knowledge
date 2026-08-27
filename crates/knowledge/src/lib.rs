#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod archive_events;
mod article;
mod blob_store;
mod budget;
mod chunking;
mod config;
mod context;
mod controlled;
mod database;
mod deletion;
mod embeddings;
mod evaluation;
mod family_analysis;
mod family_pipeline;
mod indexer;
mod openrouter;
mod pipeline;
mod provider;
mod rate_limit;
mod reindex;
mod repository_analysis;
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
pub use chunking::{CHUNKING_VERSION, Chunk, ChunkPolicy, ChunkPolicyError, chunk_article};
pub use config::{AdminConfig, Config, ConfigError, Limits, ProviderSecret, StorageConfig};
pub use context::{
    ContextError, GenerationRequest, PreparedContext, build_generation_request, prepare_context,
};
pub use controlled::{ControlledProvider, SpendControls};
pub use database::{Database, PersistenceError};
pub use deletion::{
    DeletionCounts, DeletionError, DeletionReceipt, DeletionScope, delete_source, delete_tenant,
    execute_deletion,
};
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
pub use indexer::{
    EMBEDDING_STORAGE_DIMENSIONS, EmbeddingWrite, Indexer, IndexerLimits, IndexingIdentity,
    IndexingOutcome, IndexingTarget, PendingSource, failure_attempt_count, pending_indexing_batch,
    record_indexing_failure, store_embeddings,
};
pub use openrouter::{
    OpenRouterProvider, OpenRouterSettings, OpenRouterWireError, RetryPolicy, chat_completion_body,
    classify_error, parse_success_envelope,
};
pub use pipeline::{ArticlePipeline, PipelineError};
pub use provider::{
    LlmProvider, ProviderError, ProviderFailure, ProviderFailureClass, ProviderIdentity,
    ProviderResponse, ProviderUsage, ScriptedProvider,
};
pub use rate_limit::RateLimiter;
pub use reindex::{
    ReindexScope, ReindexSourceOutcome, ReindexSummary, execute_reindex, plan_reindex,
    rebuild_search_documents,
};
pub use repository_analysis::{
    RepositoryAnalysisAdmission, RepositoryAnalysisConsumer, RepositoryAnalysisError,
};
pub use runs::{
    AnalysisIdentity, AnalysisRun, Attempt, AttemptInput, AttemptOutcome, AttemptReason, RunState,
    SourceReference, SourceRevision,
};
pub use search::{
    HybridRetriever, RankingPath, SearchDocumentProjection, SearchError, SearchPage, SearchQuery,
    SearchResult, SemanticLeg, hybrid_search_page, record_search_document,
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
    record_feedback, set_analysis_state, tag_analysis, tag_name, validate_highlight_anchor,
};
