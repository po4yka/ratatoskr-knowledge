use std::future::Future;
use std::pin::Pin;

use super::{
    AiArchiveAnalysisCompleted, BlobError, BlobRef, PersistenceError, ProviderError,
    RepositoryAnalysis, RepositoryAnalysisCompleted, RepositoryAnalysisRequested, SourceInboxError,
};

/// Authorized byte resolver for a GitHub-owned README reference.
///
/// The resolver is a boundary adapter. It may call only the GitHub service's documented blob
/// endpoint; Knowledge never reads another bounded context's database or fetches GitHub URLs.
pub trait RepositoryReadmeResolver: Send + Sync {
    /// Resolves exactly the supplied immutable README reference.
    fn read_readme<'a>(
        &'a self,
        request: &'a RepositoryAnalysisRequested,
        reference: &'a BlobRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RepositoryReadmeError>> + Send + 'a>>;
}

/// Safe repository README retrieval failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RepositoryReadmeError {
    /// The referenced source bytes cannot currently be resolved.
    #[error("the repository README source is unavailable")]
    Unavailable,
    /// The owner service refused the caller's scope or credential.
    #[error("the repository README source is unauthorized")]
    Unauthorized,
    /// The immutable README no longer exists at its owner boundary.
    #[error("the repository README source is missing")]
    Missing,
    /// The response exceeded the configured finite byte limit.
    #[error("the repository README source is oversized")]
    Oversized,
    /// The resolver returned bytes that do not match the immutable reference.
    #[error("the repository README source is corrupt")]
    Integrity,
    /// The resolver cannot be safely constructed from its runtime configuration.
    #[error("the repository README resolver configuration is invalid")]
    InvalidConfiguration,
}

/// Durable family-analysis execution failure without source contents.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FamilyPipelineError {
    /// Knowledge-owned state could not be written.
    #[error("the family analysis state could not be persisted")]
    Persistence(#[from] PersistenceError),
    /// Raw source or response storage failed.
    #[error("the family analysis blob could not be stored")]
    Blob(#[from] BlobError),
    /// The provider failed.
    #[error("the family analysis provider failed")]
    Provider(#[from] ProviderError),
    /// The provider response is not valid for the requested family.
    #[error("the family analysis response is invalid")]
    Invalid,
    /// The provider request exceeded its finite deadline.
    #[error("the family analysis provider timed out")]
    Timeout,
    /// The provider may have accepted a billable request and replay is unsafe.
    #[error("the family analysis provider outcome is unknown")]
    ProviderOutcomeUnknown,
    /// A contract snapshot could not be encoded or decoded.
    #[error("the family analysis contract could not be encoded")]
    Contract(#[from] serde_json::Error),
    /// Inbox state could not be loaded safely.
    #[error("the family source inbox could not be read")]
    Inbox(#[from] SourceInboxError),
    /// A GitHub README could not be acquired from its authorized source owner.
    #[error(transparent)]
    RepositorySource(#[from] RepositoryReadmeError),
    /// The immutable family source identity is invalid.
    #[error("the family analysis source identity is invalid")]
    Source,
}

/// Result of executing a repository request, including at-most-once terminal fact creation.
#[derive(Debug, Clone)]
pub struct RepositoryAnalysisExecution {
    /// Persisted, validated repository analysis.
    pub analysis: RepositoryAnalysis,
    /// Completion fact to publish through the transactional outbox, if this call linked it.
    pub completion: Option<RepositoryAnalysisCompleted>,
}

/// Result of executing one published archive conversation, including its producer-linkable
/// completion fact.
#[derive(Debug, Clone)]
pub struct ArchiveAnalysisExecution {
    /// Grounded analysis persisted for this immutable conversation revision.
    pub analysis: crate::ArchiveAnalysis,
    /// Completion naming precisely the archive subject revision that was analysed.
    pub completion: AiArchiveAnalysisCompleted,
}
