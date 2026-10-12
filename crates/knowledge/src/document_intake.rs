//! Intake of `content.document.extracted.v1` facts into article analysis (XR-021 CONTRACTS.md
//! S09).

use ratatoskr_document_contracts::ContentDocumentExtracted;
use ratatoskr_event_envelope::{EventEnvelope, EventPayload as _};
use ratatoskr_identifiers::DocumentId;

use crate::{
    AnalysisIdentity, ArticlePipeline, ArticleValidationError, BlobStore, ContextError, Database,
    LlmProvider, PersistenceError, PipelineError, SourceReference, build_generation_request,
    prepare_context,
};

const PRODUCER: &str = "ratatoskr-extractor";

/// Result of accepting one extracted-document event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentIntakeOutcome {
    /// The document was registered and analysed by this call.
    Analysed,
    /// The same document revision already had an analysis; no provider call was made.
    AlreadyAnalysed,
}

/// Why an extracted-document event was refused or could not be completed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DocumentIntakeError {
    /// The envelope is not a `content.document.extracted.v1` fact.
    #[error("the event is not an extracted-document fact")]
    UnsupportedEvent,
    /// The envelope was not produced by the extractor.
    #[error("the extracted-document fact has an unexpected producer")]
    WrongProducer,
    /// The envelope names no tenant, so the document has no owner.
    #[error("the extracted-document fact carries no tenant")]
    MissingTenant,
    /// The payload does not satisfy the published contract.
    #[error("the extracted-document payload is invalid")]
    InvalidPayload,
    /// Knowledge-owned state could not be written.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// The document cannot be rendered into a provider context.
    #[error(transparent)]
    Context(#[from] ContextError),
    /// The provider request could not be built.
    #[error(transparent)]
    Request(#[from] ArticleValidationError),
    /// The article analysis failed.
    #[error(transparent)]
    Analysis(#[from] PipelineError),
}

impl DocumentIntakeError {
    /// Whether redelivering the same event can never succeed after this failure.
    ///
    /// A fact that is not a valid extracted document, a document that cannot be rendered into a
    /// context or a request, and an analysis that failed for good are properties of the delivery;
    /// a storage failure, a timeout and a transient provider failure are not.
    #[must_use]
    pub const fn is_permanent(&self) -> bool {
        match self {
            Self::UnsupportedEvent
            | Self::WrongProducer
            | Self::MissingTenant
            | Self::InvalidPayload
            | Self::Context(_)
            | Self::Request(_) => true,
            Self::Persistence(error) => error.is_permanent(),
            Self::Analysis(error) => error.is_permanent(),
        }
    }
}

/// Registers extracted documents and runs the article analysis over them.
#[derive(Debug)]
pub struct DocumentIntake<'a> {
    database: &'a Database,
    context_characters: usize,
    provider_timeout: std::time::Duration,
}

impl<'a> DocumentIntake<'a> {
    /// Creates an intake over Knowledge-owned storage with its context and call budgets.
    #[must_use]
    pub const fn new(
        database: &'a Database,
        context_characters: usize,
        provider_timeout: std::time::Duration,
    ) -> Self {
        Self {
            database,
            context_characters,
            provider_timeout,
        }
    }

    /// Accepts one extracted-document envelope and analyses its document once.
    ///
    /// The source is registered under the extractor's context with the extractor-owned Document
    /// IR blob as its provenance, the run identity is fixed by the document revision, and a
    /// redelivery of a document that already has its analysis makes no provider call.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentIntakeError`] when the envelope is refused or the analysis fails.
    pub async fn accept<P: LlmProvider>(
        &self,
        envelope: &EventEnvelope,
        provider: &P,
        blobs: &BlobStore,
    ) -> Result<DocumentIntakeOutcome, DocumentIntakeError> {
        if envelope.event_type.to_wire() != ContentDocumentExtracted::EVENT_TYPE {
            return Err(DocumentIntakeError::UnsupportedEvent);
        }
        if envelope.producer.as_str() != PRODUCER {
            return Err(DocumentIntakeError::WrongProducer);
        }
        let tenant = envelope
            .tenant_id
            .ok_or(DocumentIntakeError::MissingTenant)?;
        let payload = envelope
            .payload_as::<ContentDocumentExtracted>()
            .map_err(|_| DocumentIntakeError::InvalidPayload)?;
        payload
            .validate()
            .map_err(|_| DocumentIntakeError::InvalidPayload)?;
        let source = self
            .database
            .register_source(&SourceReference {
                tenant,
                owner_context: PRODUCER.to_owned(),
                ai_archive_id: String::new(),
                document_id: payload.document.document_id,
                content_digest: payload.document.content_digest.clone(),
                source_blob: payload.document_blob.clone(),
            })
            .await?;
        let run = self
            .database
            .create_run(&AnalysisIdentity {
                source_revision_id: source.id,
                contract_version: "article_v1".to_owned(),
                prompt_version: "article_prompt_v1".to_owned(),
                context_builder_version: "document_context_v1".to_owned(),
                model_policy: "family_default_v1".to_owned(),
            })
            .await?;
        let state: String =
            sqlx::query_scalar("select state from knowledge.analysis_runs where run_id = $1")
                .bind(run.id)
                .fetch_one(self.database.pool())
                .await
                .map_err(PersistenceError::Query)?;
        if matches!(state.as_str(), "persisted" | "indexed" | "completed") {
            return Ok(DocumentIntakeOutcome::AlreadyAnalysed);
        }
        // The source row is the identity of the document: when it was registered under another
        // document id (an extractor from before the identity became derived, XR-021 CONTRACTS.md
        // R5-02 rule 4), the analysis and the search document keep that first id.
        let stored_document_id: String = sqlx::query_scalar(
            "select source_document_id from knowledge.source_refs where source_ref_id = $1",
        )
        .bind(source.id)
        .fetch_one(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let mut document = payload.document.clone();
        document.document_id =
            DocumentId::parse(&stored_document_id).map_err(|_| PersistenceError::InvalidSource)?;
        let context = prepare_context(&document, self.context_characters)?;
        let request = build_generation_request(&context)?;
        ArticlePipeline::new(self.database, provider, blobs, self.provider_timeout)
            .execute(run.id, request, &context, &document)
            .await?;
        Ok(DocumentIntakeOutcome::Analysed)
    }
}
