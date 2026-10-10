use ratatoskr_document_contracts::Document;
use ratatoskr_identifiers::BlobRef;
use uuid::Uuid;

use crate::run_driver::{DriveFailure, Rejected, RunDriver};
use crate::search::{
    SearchDocumentProjection, extract_search_text, record_search_document,
    record_search_projection_input,
};
use crate::{
    ArticleAnalysis, ArticleValidationError, BlobError, BlobStore, Database, GenerationRequest,
    LlmProvider, PersistenceError, PreparedContext, ProviderError, ValidationClass,
};

/// First-slice article pipeline failure with no source or response content.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PipelineError {
    /// Durable state could not be written.
    #[error("the analysis state could not be persisted")]
    Persistence(#[from] PersistenceError),
    /// Provider execution failed.
    #[error("the analysis provider failed")]
    Provider(#[from] ProviderError),
    /// Raw response storage failed.
    #[error("the raw response could not be stored")]
    Blob(#[from] BlobError),
    /// Provider output failed validation.
    #[error("the provider response is invalid")]
    Invalid,
    /// Provider execution exceeded its finite deadline.
    #[error("the provider request timed out")]
    Timeout,
    /// Another executor holds the run or advanced it; a later delivery may succeed.
    #[error("the analysis run is held by another executor")]
    Conflict,
}

impl From<DriveFailure> for PipelineError {
    fn from(failure: DriveFailure) -> Self {
        match failure {
            DriveFailure::Persistence(error) => Self::Persistence(error),
            DriveFailure::Blob(error) => Self::Blob(error),
            DriveFailure::Provider(error) => Self::Provider(error),
            DriveFailure::Invalid => Self::Invalid,
            DriveFailure::Timeout => Self::Timeout,
            DriveFailure::Conflict => Self::Conflict,
        }
    }
}

/// Durable fake-provider article pipeline.
#[derive(Debug)]
pub struct ArticlePipeline<'a, P> {
    database: &'a Database,
    provider: &'a P,
    blobs: &'a BlobStore,
    provider_timeout: std::time::Duration,
}

impl<'a, P: LlmProvider> ArticlePipeline<'a, P> {
    /// Creates the finite first-slice pipeline.
    #[must_use]
    pub const fn new(
        database: &'a Database,
        provider: &'a P,
        blobs: &'a BlobStore,
        provider_timeout: std::time::Duration,
    ) -> Self {
        Self {
            database,
            provider,
            blobs,
            provider_timeout,
        }
    }

    /// Executes one article analysis identity, resuming the run from its durable state.
    ///
    /// A run that failed transiently, was left mid-call by a stopped executor, or stored a
    /// response it never validated continues instead of failing (see `run_driver`).
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError`] for durable state, provider, blob, timeout, or validation failure.
    pub async fn execute(
        &self,
        run_id: Uuid,
        request: GenerationRequest,
        context: &PreparedContext,
        document: &Document,
    ) -> Result<ArticleAnalysis, PipelineError> {
        let driver = RunDriver {
            database: self.database,
            provider: self.provider,
            blobs: self.blobs,
            provider_timeout: self.provider_timeout,
            model_policy: "fake_default_v1",
        };
        let value = driver
            .drive(
                run_id,
                request,
                |bytes| validate_article(bytes, context),
                |value, raw| self.persist(run_id, value, raw, document),
            )
            .await?;
        Ok(serde_json::from_value(value).map_err(PersistenceError::Encode)?)
    }

    async fn persist(
        &self,
        run_id: Uuid,
        result: serde_json::Value,
        reference: BlobRef,
        document: &Document,
    ) -> Result<(), DriveFailure> {
        let raw_response = serde_json::to_value(reference).map_err(PersistenceError::Encode)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let output_id = Uuid::now_v7();
        sqlx::query(
            "insert into knowledge.analysis_outputs (
                output_id, run_id, result, raw_response
             ) values ($1, $2, $3, $4)",
        )
        .bind(output_id)
        .bind(run_id)
        .bind(result)
        .bind(raw_response)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let changed = sqlx::query(
            "update knowledge.analysis_runs set state = 'persisted', updated_at = now()
             where run_id = $1 and state = 'schema_validated'",
        )
        .bind(run_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            return Err(DriveFailure::Conflict);
        }
        let (source_ref_id, tenant_ref, owner_context): (Uuid, String, String) = sqlx::query_as(
            "select r.source_ref_id, s.tenant_ref, s.owner_context
                 from knowledge.analysis_runs r
                 join knowledge.source_refs s on s.source_ref_id = r.source_ref_id
                 where r.run_id = $1",
        )
        .bind(run_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let extracted = extract_search_text(document);
        let projection = SearchDocumentProjection {
            source_ref_id,
            latest_output_id: output_id,
            tenant_ref,
            owner_context,
            document_id: document.document_id.0,
            title: extracted.title,
            lead: extracted.lead,
            body: extracted.body,
        };
        record_search_projection_input(&mut *transaction, &projection)
            .await
            .map_err(PersistenceError::Query)?;
        record_search_document(&mut *transaction, &projection)
            .await
            .map_err(PersistenceError::Query)?;
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }
}

/// Parses and validates one provider response against the prepared context.
fn validate_article(
    bytes: &[u8],
    context: &PreparedContext,
) -> Result<serde_json::Value, Rejected> {
    let value = serde_json::from_slice::<serde_json::Value>(bytes).map_err(|_| Rejected {
        code: "json_syntax",
        class: Some(ValidationClass::JsonSyntax),
    })?;
    let article = crate::validate_article_json(&value)
        .and_then(|article| {
            crate::validate_article_citations(&article, context)?;
            Ok(article)
        })
        .map_err(|error| Rejected {
            code: validation_code(error),
            class: Some(validation_class(error)),
        })?;
    serde_json::to_value(article).map_err(|_| Rejected {
        code: "decode",
        class: Some(ValidationClass::Schema),
    })
}

const fn validation_code(error: ArticleValidationError) -> &'static str {
    match error {
        ArticleValidationError::SchemaDefinition | ArticleValidationError::Structural => "schema",
        ArticleValidationError::Decode => "decode",
        ArticleValidationError::Citation => "citation",
    }
}

const fn validation_class(error: ArticleValidationError) -> ValidationClass {
    match error {
        ArticleValidationError::Citation => ValidationClass::Citation,
        ArticleValidationError::SchemaDefinition
        | ArticleValidationError::Structural
        | ArticleValidationError::Decode => ValidationClass::Schema,
    }
}
