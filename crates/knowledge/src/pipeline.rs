use ratatoskr_document_contracts::Document;
use uuid::Uuid;

use crate::runs::AttemptUpdate;
use crate::search::{SearchDocumentProjection, extract_search_text, record_search_document};
use crate::{
    ArticleAnalysis, ArticleValidationError, AttemptInput, AttemptOutcome, AttemptReason,
    BlobError, BlobStore, Database, GenerationRequest, LlmProvider, PersistenceError,
    PreparedContext, ProviderError, ProviderFailureClass, ProviderIdentity, RunState,
    ValidationClass, record_validation_failure,
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

enum ResponseOutcome {
    Accepted(ArticleAnalysis),
    Invalid(&'static str),
}

enum AttemptFlow {
    Accepted(ArticleAnalysis),
    Retry,
    Repair(&'static str),
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
        mut request: GenerationRequest,
        context: &PreparedContext,
        document: &Document,
    ) -> Result<ArticleAnalysis, PipelineError> {
        if let Some(article) = self.resume_output(run_id).await? {
            return Ok(article);
        }
        self.database
            .transition_run(run_id, RunState::Queued, RunState::ContextPrepared)
            .await?;
        self.database
            .transition_run(run_id, RunState::ContextPrepared, RunState::ModelRequested)
            .await?;
        let mut reason = AttemptReason::Initial;
        for call in 0..2 {
            let flow = self
                .execute_attempt(run_id, call, request.clone(), context, document, reason)
                .await?;
            match flow {
                AttemptFlow::Accepted(article) => return Ok(article),
                AttemptFlow::Retry => reason = AttemptReason::Retry,
                AttemptFlow::Repair(code) => {
                    self.database
                        .transition_run(run_id, RunState::ResponseReceived, RunState::Repaired)
                        .await?;
                    self.database
                        .transition_run(run_id, RunState::Repaired, RunState::ModelRequested)
                        .await?;
                    request = repair_request(&request, code);
                    reason = AttemptReason::Repair;
                }
            }
        }
        self.fail_requested_run(run_id).await?;
        Err(PersistenceError::AttemptBudgetExhausted.into())
    }

    /// Runs one recorded provider attempt and reports how the loop continues.
    async fn execute_attempt(
        &self,
        run_id: Uuid,
        call: u8,
        request: GenerationRequest,
        context: &PreparedContext,
        document: &Document,
        reason: AttemptReason,
    ) -> Result<AttemptFlow, PipelineError> {
        let identity = self.provider.identity();
        let attempt = match self
            .database
            .record_attempt(run_id, &attempt_input(&identity, reason))
            .await
        {
            Ok(attempt) => attempt,
            Err(PersistenceError::AttemptBudgetExhausted) => {
                self.fail_requested_run(run_id).await?;
                return Err(PersistenceError::AttemptBudgetExhausted.into());
            }
            Err(error) => return Err(error.into()),
        };
        let started = std::time::Instant::now();
        let generated =
            tokio::time::timeout(self.provider_timeout, self.provider.generate_json(request)).await;
        let duration_ms = i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX);
        let Ok(generated) = generated else {
            self.database
                .update_attempt_failure(
                    run_id,
                    attempt.ordinal,
                    AttemptOutcome::TransientFailure,
                    Some(ProviderFailureClass::Timeout.as_str()),
                    None,
                    duration_ms,
                )
                .await?;
            if call == 0 {
                return Ok(AttemptFlow::Retry);
            }
            self.fail_requested_run(run_id).await?;
            return Err(PipelineError::Timeout);
        };
        let response = match generated {
            Ok(response) => response,
            Err(failure) => {
                let outcome = if failure.is_transient() {
                    AttemptOutcome::TransientFailure
                } else {
                    AttemptOutcome::PermanentFailure
                };
                self.database
                    .update_attempt_failure(
                        run_id,
                        attempt.ordinal,
                        outcome,
                        Some(failure.class.as_str()),
                        failure
                            .http_status
                            .and_then(|status| i16::try_from(status).ok()),
                        duration_ms,
                    )
                    .await?;
                if failure.is_transient() && call == 0 {
                    return Ok(AttemptFlow::Retry);
                }
                self.fail_requested_run(run_id).await?;
                return Err(PipelineError::Provider(failure.error));
            }
        };
        match self
            .process_or_fail(
                run_id,
                attempt.ordinal,
                &response,
                context,
                document,
                duration_ms,
            )
            .await?
        {
            ResponseOutcome::Accepted(article) => Ok(AttemptFlow::Accepted(article)),
            ResponseOutcome::Invalid(code) if call == 0 => Ok(AttemptFlow::Repair(code)),
            ResponseOutcome::Invalid(_) => {
                self.database
                    .transition_run(run_id, RunState::ResponseReceived, RunState::Failed)
                    .await?;
                Err(PipelineError::Invalid)
            }
        }
    }

    async fn resume_output(&self, run_id: Uuid) -> Result<Option<ArticleAnalysis>, PipelineError> {
        let stored: Option<(String, Option<serde_json::Value>)> = sqlx::query_as(
            "select runs.state, outputs.result
             from knowledge.analysis_runs runs
             left join knowledge.analysis_outputs outputs
                on outputs.run_id = runs.run_id and outputs.accepted
             where runs.run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let Some((state, result)) = stored else {
            return Ok(None);
        };
        if !matches!(state.as_str(), "persisted" | "completed") {
            return Ok(None);
        }
        let article =
            serde_json::from_value(result.ok_or(PersistenceError::InvalidAnalysisIdentity)?)
                .map_err(PersistenceError::Encode)?;
        if state == "persisted" {
            self.database
                .transition_run(run_id, RunState::Persisted, RunState::Completed)
                .await?;
        }
        Ok(Some(article))
    }

    async fn process_or_fail(
        &self,
        run_id: Uuid,
        ordinal: i16,
        response: &crate::ProviderResponse,
        context: &PreparedContext,
        document: &Document,
        duration_ms: i32,
    ) -> Result<ResponseOutcome, PipelineError> {
        match self
            .process_response(run_id, ordinal, response, context, document, duration_ms)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(PipelineError::Blob(error)) => {
                self.database
                    .update_attempt_failure(
                        run_id,
                        ordinal,
                        AttemptOutcome::PermanentFailure,
                        Some(ProviderFailureClass::SizeLimit.as_str()),
                        None,
                        duration_ms,
                    )
                    .await?;
                self.fail_requested_run(run_id).await?;
                Err(error.into())
            }
            Err(error) => Err(error),
        }
    }

    async fn process_response(
        &self,
        run_id: Uuid,
        ordinal: i16,
        response: &crate::ProviderResponse,
        context: &PreparedContext,
        document: &Document,
        duration_ms: i32,
    ) -> Result<ResponseOutcome, PipelineError> {
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
                    duration_ms,
                },
            )
            .await?;
        self.database
            .transition_run(run_id, RunState::ModelRequested, RunState::ResponseReceived)
            .await?;

        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&response.bytes) else {
            self.mark_invalid(
                run_id,
                ordinal,
                &reference,
                response,
                "json_syntax",
                duration_ms,
            )
            .await?;
            record_validation_failure(ValidationClass::JsonSyntax, "", "");
            return Ok(ResponseOutcome::Invalid("json_syntax"));
        };
        let article = match crate::validate_article_json(&value).and_then(|article| {
            crate::validate_article_citations(&article, context)?;
            Ok(article)
        }) {
            Ok(article) => article,
            Err(error) => {
                let code = validation_code(error);
                self.mark_invalid(run_id, ordinal, &reference, response, code, duration_ms)
                    .await?;
                record_validation_failure(validation_class(error), "", "");
                return Ok(ResponseOutcome::Invalid(code));
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
                    duration_ms,
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
        self.persist(run_id, &article, &reference, document).await?;
        self.database
            .transition_run(run_id, RunState::Persisted, RunState::Completed)
            .await?;
        Ok(ResponseOutcome::Accepted(article))
    }

    async fn mark_invalid(
        &self,
        run_id: Uuid,
        ordinal: i16,
        reference: &ratatoskr_identifiers::BlobRef,
        response: &crate::ProviderResponse,
        code: &'static str,
        duration_ms: i32,
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
                    duration_ms,
                },
            )
            .await?;
        Ok(())
    }

    async fn persist(
        &self,
        run_id: Uuid,
        article: &ArticleAnalysis,
        reference: &ratatoskr_identifiers::BlobRef,
        document: &Document,
    ) -> Result<(), PipelineError> {
        let result = serde_json::to_value(article).map_err(PersistenceError::Encode)?;
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
            return Err(PersistenceError::InvalidAnalysisIdentity.into());
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
        record_search_document(
            &mut *transaction,
            &SearchDocumentProjection {
                source_ref_id,
                latest_output_id: output_id,
                tenant_ref,
                owner_context,
                document_id: document.document_id.0,
                title: extracted.title,
                lead: extracted.lead,
                body: extracted.body,
            },
        )
        .await
        .map_err(PersistenceError::Query)?;
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

fn attempt_input(identity: &ProviderIdentity, reason: AttemptReason) -> AttemptInput {
    AttemptInput {
        reason,
        provider: identity.provider.clone(),
        model: identity.model.clone(),
        model_policy: "fake_default_v1".to_owned(),
        provider_request_id: None,
        outcome: AttemptOutcome::Requested,
    }
}

fn repair_request(request: &GenerationRequest, code: &str) -> GenerationRequest {
    let mut repaired = request.clone();
    repaired
        .task_instruction
        .push_str("\nRepair validation code: ");
    repaired.task_instruction.push_str(code);
    repaired.task_instruction.push('.');
    repaired
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
