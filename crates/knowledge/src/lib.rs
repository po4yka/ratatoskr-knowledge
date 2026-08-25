#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Durable first-slice article analysis for Ratatoskr Knowledge.

mod article;
mod blob_store;
mod budget;
mod chunking;
mod config;
mod context;
mod controlled;
mod database;
mod embeddings;
mod indexer;
mod openrouter;
mod pipeline;
mod provider;
mod rate_limit;
mod runs;
pub mod search;
mod telemetry;

#[cfg(feature = "test-support")]
pub mod test_support;

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
pub use embeddings::{
    ControlledEmbeddings, EmbeddingIdentity, EmbeddingProvider, EmbeddingResponse,
    EmbeddingsSettings, EmbeddingsWireError, OpenAiCompatibleEmbeddings, ScriptedEmbeddingProvider,
    ScriptedEmbeddingSuccess, embeddings_request_body, parse_embeddings_envelope,
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
pub use runs::{
    AnalysisIdentity, AnalysisRun, Attempt, AttemptInput, AttemptOutcome, AttemptReason, RunState,
    SourceReference, SourceRevision,
};
pub use search::{
    HybridRetriever, RankingPath, SearchDocumentProjection, SearchError, SearchPage, SearchQuery,
    SearchResult, SemanticLeg, hybrid_search_page, record_search_document, search_page,
};
pub use telemetry::{TelemetryError, ValidationClass, init_telemetry, record_validation_failure};
