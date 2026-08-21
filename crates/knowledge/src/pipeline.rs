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
        let mut reason = AttemptReason::Initial;
        for call in 0..2 {
            let attempt = self
                .database
                .record_attempt(run_id, &attempt_input(reason))
                .await?;
            let generated = tokio::time::timeout(
                self.provider_timeout,
                self.provider.generate_json(request.clone()),
            )
            .await;
            match generated {
                Err(_) => {
                    self.database
                        .update_attempt_failure(
                            run_id,
                            attempt.ordinal,
                            AttemptOutcome::TransientFailure,
                        )
                        .await?;
                    if call == 0 {
                        reason = AttemptReason::Retry;
                    } else {
                        self.fail_requested_run(run_id).await?;
                        return Err(PipelineError::Timeout);
                    }
                }
                Ok(Err(error)) => {
                    let transient = error == ProviderError::Transient;
                    let outcome = if transient {
                        AttemptOutcome::TransientFailure
                    } else {
                        AttemptOutcome::PermanentFailure
                    };
                    self.database
                        .update_attempt_failure(run_id, attempt.ordinal, outcome)
                        .await?;
                    if transient && call == 0 {
                        reason = AttemptReason::Retry;
                    } else {
                        self.fail_requested_run(run_id).await?;
                        return Err(PipelineError::Provider(error));
                    }
                }
                Ok(Ok(response)) => {
                    return self
                        .process_response(run_id, attempt.ordinal, &response, context)
                        .await;
                }
            }
        }
        Err(PersistenceError::AttemptBudgetExhausted.into())
    }

    async fn process_response(
        &self,
        run_id: Uuid,
        ordinal: i16,
        response: &crate::ProviderResponse,
        context: &PreparedContext,
    ) -> Result<ArticleAnalysis, PipelineError> {
        let reference = self.blobs.store_raw(&response.bytes).await?;
        self.database
            .update_attempt(
                run_id,
                ordinal,
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
            self.reject(run_id, ordinal, &reference, response, "json_syntax")
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
                self.reject(run_id, ordinal, &reference, response, code)
                    .await?;
                return Err(PipelineError::Invalid);
            }
        };
        self.database
            .update_attempt(
                run_id,
                ordinal,
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
        self.persist(run_id, &article, &reference).await?;
        self.database
            .transition_run(run_id, RunState::Persisted, RunState::Completed)
            .await?;
        Ok(article)
    }

    async fn reject(
        &self,
        run_id: Uuid,
        ordinal: i16,
        reference: &ratatoskr_identifiers::BlobRef,
        response: &crate::ProviderResponse,
        code: &'static str,
    ) -> Result<(), PipelineError> {
        self.database
            .update_attempt(
                run_id,
                ordinal,
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

    async fn persist(
        &self,
        run_id: Uuid,
        article: &ArticleAnalysis,
        reference: &ratatoskr_identifiers::BlobRef,
    ) -> Result<(), PipelineError> {
        let result = serde_json::to_value(article).map_err(PersistenceError::Encode)?;
        let raw_response = serde_json::to_value(reference).map_err(PersistenceError::Encode)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        sqlx::query(
            "insert into knowledge.analysis_outputs (
                output_id, run_id, result, raw_response
             ) values ($1, $2, $3, $4)",
        )
        .bind(Uuid::now_v7())
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
            return Err(PersistenceError::InvalidAnalysisIdentity.into());
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    async fn fail_requested_run(&self, run_id: Uuid) -> Result<(), PipelineError> {
        self.database
            .transition_run(run_id, RunState::ModelRequested, RunState::Failed)
            .await?;
        Ok(())
    }
}

fn attempt_input(reason: AttemptReason) -> AttemptInput {
    AttemptInput {
        reason,
        provider: "scripted_fake".to_owned(),
        model_policy: "fake_default_v1".to_owned(),
        provider_request_id: None,
        outcome: AttemptOutcome::Requested,
    }
}

const fn validation_code(error: ArticleValidationError) -> &'static str {
    match error {
        ArticleValidationError::SchemaDefinition | ArticleValidationError::Structural => "schema",
        ArticleValidationError::Decode => "decode",
        ArticleValidationError::Citation => "citation",
    }
}
