use uuid::Uuid;

use crate::runs::AttemptUpdate;
use crate::{
    ArticleAnalysis, ArticleValidationError, AttemptInput, AttemptOutcome, AttemptReason,
    BlobError, BlobStore, Database, GenerationRequest, LlmProvider, PersistenceError,
    PreparedContext, ProviderError, RunState, ValidationClass, record_validation_failure,
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
}

/// Durable fake-provider article pipeline.
#[derive(Debug)]
pub struct ArticlePipeline<'a, P> {
    database: &'a Database,
    provider: &'a P,
    blobs: &'a BlobStore,
}

impl<'a, P: LlmProvider> ArticlePipeline<'a, P> {
    /// Creates the finite first-slice pipeline.
    #[must_use]
    pub const fn new(database: &'a Database, provider: &'a P, blobs: &'a BlobStore) -> Self {
        Self {
            database,
            provider,
            blobs,
        }
    }

    /// Executes one article analysis identity.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError`] for durable state, provider, blob, timeout, or validation failure.
    pub async fn execute(
        &self,
        run_id: Uuid,
        request: GenerationRequest,
        context: &PreparedContext,
    ) -> Result<ArticleAnalysis, PipelineError> {
        self.database
            .transition_run(run_id, RunState::Queued, RunState::ContextPrepared)
            .await?;
        self.database
            .transition_run(run_id, RunState::ContextPrepared, RunState::ModelRequested)
            .await?;
        self.database
            .record_attempt(
                run_id,
                &AttemptInput {
                    reason: AttemptReason::Initial,
                    provider: "scripted_fake".to_owned(),
                    model_policy: "fake_default_v1".to_owned(),
                    provider_request_id: None,
                    outcome: AttemptOutcome::Requested,
                },
            )
            .await?;
        let response = self.provider.generate_json(request).await?;
        let reference = self.blobs.store_raw(&response.bytes).await?;
        self.database
            .update_attempt(
                run_id,
                1,
                &AttemptUpdate {
                    raw_response: &reference,
                    request_id: response.request_id.as_deref(),
                    input_tokens: response.usage.input_tokens,
                    output_tokens: response.usage.output_tokens,
                    outcome: AttemptOutcome::ResponseReceived,
                    validation_code: None,
                },
            )
            .await?;
        self.database
            .transition_run(run_id, RunState::ModelRequested, RunState::ResponseReceived)
            .await?;

        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&response.bytes) else {
            self.reject(run_id, &reference, &response, "json_syntax")
                .await?;
            record_validation_failure(ValidationClass::JsonSyntax, "", "");
            return Err(PipelineError::Invalid);
        };
        let article = match crate::validate_article_json(&value).and_then(|article| {
            crate::validate_article_citations(&article, context)?;
            Ok(article)
        }) {
            Ok(article) => article,
            Err(error) => {
                let code = validation_code(error);
                self.reject(run_id, &reference, &response, code).await?;
                return Err(PipelineError::Invalid);
            }
        };
        self.database
            .update_attempt(
                run_id,
                1,
                &AttemptUpdate {
                    raw_response: &reference,
                    request_id: response.request_id.as_deref(),
                    input_tokens: response.usage.input_tokens,
                    output_tokens: response.usage.output_tokens,
                    outcome: AttemptOutcome::Accepted,
                    validation_code: None,
                },
            )
            .await?;
        self.database
            .transition_run(
                run_id,
                RunState::ResponseReceived,
                RunState::SchemaValidated,
            )
            .await?;
        Ok(article)
    }

    async fn reject(
        &self,
        run_id: Uuid,
        reference: &ratatoskr_identifiers::BlobRef,
        response: &crate::ProviderResponse,
        code: &'static str,
    ) -> Result<(), PipelineError> {
        self.database
            .update_attempt(
                run_id,
                1,
                &AttemptUpdate {
                    raw_response: reference,
                    request_id: response.request_id.as_deref(),
                    input_tokens: response.usage.input_tokens,
                    output_tokens: response.usage.output_tokens,
                    outcome: AttemptOutcome::Invalid,
                    validation_code: Some(code),
                },
            )
            .await?;
        self.database
            .transition_run(run_id, RunState::ResponseReceived, RunState::Failed)
            .await?;
        Ok(())
    }
}

const fn validation_code(error: ArticleValidationError) -> &'static str {
    match error {
        ArticleValidationError::SchemaDefinition | ArticleValidationError::Structural => "schema",
        ArticleValidationError::Decode => "decode",
        ArticleValidationError::Citation => "citation",
    }
}
