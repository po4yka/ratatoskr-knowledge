//! Durable raw-response-first provider pipeline for channel recap analysis.

use crate::{
    BlobStore, ChannelDigestRecap, ChannelRecapOutputLanguage, ChannelRecapProviderRequest,
    ChannelRecapResultError, ChannelRecapRunState, ChannelRecapRunStore, Database,
    GenerationRequest, LlmProvider, PersistenceError, PreparedChannelRecapContext,
    ProviderFailureClass, validate_channel_recap_model_output,
};
use ratatoskr_channel_digest_contracts::{
    ChannelDigestRecapFailureCode, KnowledgeChannelDigestRecapCompleted,
    KnowledgeChannelDigestRecapFailed, KnowledgeChannelDigestRecapRequested,
};
use ratatoskr_identifiers::{BlobRef, WireTimestamp};
use sha2::{Digest as _, Sha256};

use crate::{blob_registry::lock_and_verify, run_lock::RunLock};

const REPAIR_INSTRUCTION: &str = "\nRepair validation code: channel_recap_schema.";

/// What a provider response is validated against: the prepared evidence and requested language.
#[derive(Debug, Clone, Copy)]
struct ValidationContext<'a> {
    context: &'a PreparedChannelRecapContext,
    manifest_digest_hex: &'a str,
    output_language: ChannelRecapOutputLanguage,
}

impl ValidationContext<'_> {
    fn validate(&self, bytes: &[u8]) -> Result<ChannelDigestRecap, &'static str> {
        let value =
            serde_json::from_slice::<serde_json::Value>(bytes).map_err(|_| "json_syntax")?;
        validate_channel_recap_model_output(
            &value,
            self.context,
            self.manifest_digest_hex,
            self.output_language,
        )
        .map_err(validation_code)
    }
}

/// Where a run continues after its durable state was inspected.
#[derive(Debug)]
enum Recovery {
    /// The run reached its terminal fact without another provider call.
    Completed(ChannelDigestRecap),
    /// The next provider call is attempt `ordinal` with the stated reason.
    Call { ordinal: i16, reason: &'static str },
}

/// Safe channel-recap provider pipeline failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChannelRecapPipelineError {
    /// Durable state could not be read or committed.
    #[error("the channel recap state could not be persisted")]
    Persistence(#[from] crate::PersistenceError),
    /// Raw provider bytes could not be stored before parsing.
    #[error("the channel recap raw response could not be stored")]
    Blob(#[from] crate::BlobError),
    /// Provider execution failed within the bounded attempt.
    #[error("the channel recap provider failed")]
    Provider(#[from] crate::ProviderError),
    /// Provider execution exceeded the finite call deadline.
    #[error("the channel recap provider timed out")]
    Timeout,
    /// Provider JSON failed structural or grounding validation.
    #[error("the channel recap provider response is invalid")]
    Invalid,
    /// Durable expected state did not match the pipeline stage.
    #[error("the channel recap pipeline state changed concurrently")]
    State,
    /// Provider request or typed result could not be encoded deterministically.
    #[error("the channel recap pipeline value could not be encoded")]
    Encode(#[from] serde_json::Error),
}

/// Raw-response-first channel recap execution pipeline.
#[derive(Debug)]
pub struct ChannelRecapPipeline<'a, P> {
    database: &'a Database,
    provider: &'a P,
    blobs: &'a BlobStore,
    provider_timeout: std::time::Duration,
}

impl<'a, P: LlmProvider> ChannelRecapPipeline<'a, P> {
    /// Creates one bounded recap pipeline over process-owned dependencies.
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

    /// Executes or resumes one durable recap run.
    ///
    /// A session advisory lock on a dedicated connection keeps a second executor from touching a
    /// run another one holds; the held case is a [`ChannelRecapPipelineError::State`], which the
    /// worker answers with a delayed redelivery. Holding the lock, the run is resumed from its
    /// durable state (XR-021 CONTRACTS.md S08), so a redelivery after a kill converges instead of
    /// failing on an already-advanced state.
    ///
    /// # Errors
    ///
    /// Returns a safe durable, provider, timeout, blob, validation, or state failure.
    pub async fn execute(
        &self,
        recap_run_id: uuid::Uuid,
        request: ChannelRecapProviderRequest,
        context: &PreparedChannelRecapContext,
        manifest_digest_hex: &str,
        output_language: ChannelRecapOutputLanguage,
    ) -> Result<ChannelDigestRecap, ChannelRecapPipelineError> {
        let lock = RunLock::acquire(self.database, recap_run_id)
            .await?
            .ok_or(ChannelRecapPipelineError::State)?;
        let outcome = self
            .execute_locked(
                recap_run_id,
                request,
                context,
                manifest_digest_hex,
                output_language,
            )
            .await;
        lock.release().await;
        outcome
    }

    async fn execute_locked(
        &self,
        recap_run_id: uuid::Uuid,
        mut request: ChannelRecapProviderRequest,
        context: &PreparedChannelRecapContext,
        manifest_digest_hex: &str,
        output_language: ChannelRecapOutputLanguage,
    ) -> Result<ChannelDigestRecap, ChannelRecapPipelineError> {
        if let Some(result) = self.resume_result(recap_run_id).await? {
            return Ok(result);
        }
        let check = ValidationContext {
            context,
            manifest_digest_hex,
            output_language,
        };
        let (first_ordinal, mut reason) =
            match self.recover(recap_run_id, &mut request, &check).await? {
                Recovery::Completed(result) => return Ok(result),
                Recovery::Call { ordinal, reason } => (ordinal, reason),
            };
        for ordinal in first_ordinal..=2 {
            self.insert_attempt(recap_run_id, ordinal, reason).await?;
            let started = std::time::Instant::now();
            let generated = tokio::time::timeout(
                self.provider_timeout,
                self.provider
                    .generate_json(generation_request(request.clone())?),
            )
            .await;
            let duration_ms = i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX);
            let response = match generated {
                Err(_) => {
                    self.record_attempt_failure(
                        recap_run_id,
                        ordinal,
                        true,
                        ProviderFailureClass::Timeout,
                        duration_ms,
                    )
                    .await?;
                    if ordinal == 1 {
                        reason = "retry";
                        continue;
                    }
                    self.settle_failure(
                        recap_run_id,
                        ChannelRecapRunState::ModelRequested,
                        ChannelDigestRecapFailureCode::ProviderTimeout,
                    )
                    .await?;
                    return Err(ChannelRecapPipelineError::Timeout);
                }
                Ok(Err(failure)) => {
                    self.record_attempt_failure(
                        recap_run_id,
                        ordinal,
                        failure.is_transient(),
                        failure.class,
                        duration_ms,
                    )
                    .await?;
                    if failure.is_transient() && ordinal == 1 {
                        reason = "retry";
                        continue;
                    }
                    let provider_error = failure.error;
                    self.settle_failure(
                        recap_run_id,
                        ChannelRecapRunState::ModelRequested,
                        ChannelDigestRecapFailureCode::ProviderUnavailable,
                    )
                    .await?;
                    return Err(ChannelRecapPipelineError::Provider(provider_error));
                }
                Ok(Ok(response)) => response,
            };
            self.record_raw_response(recap_run_id, ordinal, &response, duration_ms)
                .await?;
            let result = match check.validate(&response.bytes) {
                Ok(result) => result,
                Err(validation_code) => {
                    if self
                        .handle_invalid_attempt(
                            recap_run_id,
                            ordinal,
                            validation_code,
                            &mut request,
                        )
                        .await?
                    {
                        reason = "repair";
                        continue;
                    }
                    return Err(ChannelRecapPipelineError::Invalid);
                }
            };
            self.accept_validated(recap_run_id, ordinal, &result)
                .await?;
            crate::telemetry::record_channel_recap_pipeline("completed", "accepted", duration_ms);
            return Ok(result);
        }
        Err(ChannelRecapPipelineError::Invalid)
    }

    async fn accept_validated(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        result: &ChannelDigestRecap,
    ) -> Result<(), ChannelRecapPipelineError> {
        self.transition(recap_run_id, "response_received", "schema_validated")
            .await?;
        self.persist_result(recap_run_id, ordinal, result).await?;
        self.settle_completion(recap_run_id).await
    }

    /// Decides where a run that may have been interrupted continues, from durable state alone.
    async fn recover(
        &self,
        recap_run_id: uuid::Uuid,
        request: &mut ChannelRecapProviderRequest,
        check: &ValidationContext<'_>,
    ) -> Result<Recovery, ChannelRecapPipelineError> {
        let (state, attempt_count): (String, i16) = sqlx::query_as(
            "select state, attempt_count from knowledge.channel_recap_runs where recap_run_id = $1",
        )
        .bind(recap_run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?
        .ok_or(ChannelRecapPipelineError::State)?;
        match state.as_str() {
            "context_prepared" => {
                self.transition(recap_run_id, "context_prepared", "model_requested")
                    .await?;
                Ok(Recovery::Call {
                    ordinal: 1,
                    reason: "initial",
                })
            }
            "repaired" => {
                self.transition(recap_run_id, "repaired", "model_requested")
                    .await?;
                self.recover_model_requested(recap_run_id, attempt_count, request)
                    .await
            }
            "model_requested" => {
                self.recover_model_requested(recap_run_id, attempt_count, request)
                    .await
            }
            "response_received" | "schema_validated" => {
                self.recover_stored_response(recap_run_id, attempt_count, &state, request, check)
                    .await
            }
            _ => Err(ChannelRecapPipelineError::State),
        }
    }

    /// A call was in flight, or between attempts, when the process stopped.
    async fn recover_model_requested(
        &self,
        recap_run_id: uuid::Uuid,
        attempt_count: i16,
        request: &mut ChannelRecapProviderRequest,
    ) -> Result<Recovery, ChannelRecapPipelineError> {
        let newest: Option<(i16, String)> = sqlx::query_as(
            "select ordinal, outcome from knowledge.channel_recap_attempts
             where recap_run_id = $1 order by ordinal desc limit 1",
        )
        .bind(recap_run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let newest_outcome = newest.as_ref().map(|(_, outcome)| outcome.as_str());
        if let Some((ordinal, _)) = newest
            .as_ref()
            .filter(|_| newest_outcome == Some("requested"))
        {
            sqlx::query(
                "update knowledge.channel_recap_attempts set outcome = 'interrupted'
                 where recap_run_id = $1 and ordinal = $2 and outcome = 'requested'",
            )
            .bind(recap_run_id)
            .bind(ordinal)
            .execute(self.database.pool())
            .await
            .map_err(PersistenceError::Query)?;
        }
        if attempt_count >= 2 || newest_outcome == Some("permanent_failure") {
            self.settle_failure(
                recap_run_id,
                ChannelRecapRunState::ModelRequested,
                ChannelDigestRecapFailureCode::ProviderUnavailable,
            )
            .await?;
            return Err(ChannelRecapPipelineError::Provider(
                if newest_outcome == Some("permanent_failure") {
                    crate::ProviderError::Permanent
                } else {
                    crate::ProviderError::Transient
                },
            ));
        }
        let repair = newest_outcome == Some("invalid");
        if repair {
            request.task_instruction.push_str(REPAIR_INSTRUCTION);
        }
        Ok(Recovery::Call {
            ordinal: attempt_count + 1,
            reason: match (attempt_count, repair) {
                (0, _) => "initial",
                (_, true) => "repair",
                (_, false) => "retry",
            },
        })
    }

    /// The raw response is durable: validate the stored bytes and never call the provider for it.
    async fn recover_stored_response(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        state: &str,
        request: &mut ChannelRecapProviderRequest,
        check: &ValidationContext<'_>,
    ) -> Result<Recovery, ChannelRecapPipelineError> {
        let stored: (Option<serde_json::Value>, String, Option<String>) = sqlx::query_as(
            "select raw_response, outcome, validation_code
             from knowledge.channel_recap_attempts
             where recap_run_id = $1 and ordinal = $2
               and outcome in ('response_received', 'invalid', 'accepted')",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?
        .ok_or(ChannelRecapPipelineError::State)?;
        let (raw, outcome, recorded_code) = stored;
        let verdict = if outcome == "invalid" {
            Err(match recorded_code.as_deref() {
                Some("json_syntax") => "json_syntax",
                Some("grounding") => "grounding",
                _ => "schema",
            })
        } else {
            let reference: BlobRef =
                serde_json::from_value(raw.ok_or(ChannelRecapPipelineError::State)?)?;
            let bytes = self.blobs.read(&reference).await?;
            check.validate(&bytes)
        };
        match (verdict, state) {
            (Ok(result), "response_received") => {
                self.accept_validated(recap_run_id, ordinal, &result)
                    .await?;
                Ok(Recovery::Completed(result))
            }
            (Ok(result), _) => {
                self.persist_result(recap_run_id, ordinal, &result).await?;
                self.settle_completion(recap_run_id).await?;
                Ok(Recovery::Completed(result))
            }
            (Err(validation_code), "response_received") => {
                if self
                    .handle_invalid_attempt(recap_run_id, ordinal, validation_code, request)
                    .await?
                {
                    Ok(Recovery::Call {
                        ordinal: ordinal + 1,
                        reason: "repair",
                    })
                } else {
                    Err(ChannelRecapPipelineError::Invalid)
                }
            }
            (Err(_), _) => {
                self.settle_failure(
                    recap_run_id,
                    ChannelRecapRunState::SchemaValidated,
                    ChannelDigestRecapFailureCode::InvalidOutput,
                )
                .await?;
                Err(ChannelRecapPipelineError::Invalid)
            }
        }
    }

    async fn resume_result(
        &self,
        recap_run_id: uuid::Uuid,
    ) -> Result<Option<ChannelDigestRecap>, ChannelRecapPipelineError> {
        let Some((result, completed)) = self.resume(recap_run_id).await? else {
            return Ok(None);
        };
        if !completed {
            self.settle_completion(recap_run_id).await?;
        }
        Ok(Some(result))
    }

    async fn insert_attempt(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        reason: &str,
    ) -> Result<(), ChannelRecapPipelineError> {
        let identity = self.provider.identity();
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        sqlx::query(
            "insert into knowledge.channel_recap_attempts
                (recap_run_id, ordinal, reason, provider, model, outcome, duration_ms)
             values ($1, $2, $3, $4, $5, 'requested', 0)",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .bind(reason)
        .bind(identity.provider)
        .bind(identity.model)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let changed = sqlx::query(
            "update knowledge.channel_recap_runs set attempt_count = $2, updated_at = now()
             where recap_run_id = $1 and state = 'model_requested' and attempt_count < $2",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapPipelineError::State);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    async fn record_attempt_failure(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        transient: bool,
        failure_class: ProviderFailureClass,
        duration_ms: i32,
    ) -> Result<(), ChannelRecapPipelineError> {
        let outcome = if transient {
            "transient_failure"
        } else {
            "permanent_failure"
        };
        sqlx::query(
            "update knowledge.channel_recap_attempts
             set outcome = $3, failure_class = $4, duration_ms = $5
             where recap_run_id = $1 and ordinal = $2 and outcome = 'requested'",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .bind(outcome)
        .bind(failure_class.as_str())
        .bind(duration_ms)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        crate::telemetry::record_channel_recap_pipeline("model_requested", outcome, duration_ms);
        Ok(())
    }

    async fn record_raw_response(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        response: &crate::ProviderResponse,
        duration_ms: i32,
    ) -> Result<(), ChannelRecapPipelineError> {
        let raw = self.blobs.store_raw(&response.bytes).await?;
        let raw_value = serde_json::to_value(&raw)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        lock_and_verify(&mut transaction, self.blobs, &raw, Some(&response.bytes)).await?;
        let changed = sqlx::query(
            "update knowledge.channel_recap_attempts
             set provider_request_id = $3, raw_response = $4, raw_response_digest_hex = $5,
                 input_tokens = $6, output_tokens = $7,
                 outcome = 'response_received', duration_ms = $8
             where recap_run_id = $1 and ordinal = $2 and outcome = 'requested'",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .bind(response.request_id.as_deref())
        .bind(raw_value)
        .bind(raw.digest.hex.as_str())
        .bind(i64::try_from(response.usage.input_tokens).unwrap_or(i64::MAX))
        .bind(i64::try_from(response.usage.output_tokens).unwrap_or(i64::MAX))
        .bind(duration_ms)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapPipelineError::State);
        }
        let transitioned = sqlx::query(
            "update knowledge.channel_recap_runs set state = 'response_received', updated_at = now()
             where recap_run_id = $1 and state = 'model_requested'",
        )
        .bind(recap_run_id).execute(&mut *transaction).await.map_err(PersistenceError::Query)?;
        if transitioned.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapPipelineError::State);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }

    async fn handle_invalid_attempt(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        validation_code: &'static str,
        request: &mut ChannelRecapProviderRequest,
    ) -> Result<bool, ChannelRecapPipelineError> {
        sqlx::query(
            "update knowledge.channel_recap_attempts set outcome = 'invalid', validation_code = $3
             where recap_run_id = $1 and ordinal = $2 and outcome = 'response_received'",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .bind(validation_code)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        crate::telemetry::record_channel_recap_pipeline("response_received", "invalid", 0);
        if ordinal == 1 {
            self.transition(recap_run_id, "response_received", "repaired")
                .await?;
            self.transition(recap_run_id, "repaired", "model_requested")
                .await?;
            request.task_instruction.push_str(REPAIR_INSTRUCTION);
            return Ok(true);
        }
        self.settle_failure(
            recap_run_id,
            ChannelRecapRunState::ResponseReceived,
            ChannelDigestRecapFailureCode::InvalidOutput,
        )
        .await?;
        Ok(false)
    }

    async fn settle_failure(
        &self,
        recap_run_id: uuid::Uuid,
        expected: ChannelRecapRunState,
        failure_code: ChannelDigestRecapFailureCode,
    ) -> Result<(), ChannelRecapPipelineError> {
        let request_value: serde_json::Value = sqlx::query_scalar(
            "select inbox.request_payload from knowledge.channel_recap_runs runs
             join knowledge.channel_recap_inbox inbox on inbox.command_id = runs.inbox_command_id
             where runs.recap_run_id = $1",
        )
        .bind(recap_run_id)
        .fetch_one(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let request: KnowledgeChannelDigestRecapRequested = serde_json::from_value(request_value)?;
        let fact: KnowledgeChannelDigestRecapFailed = serde_json::from_value(serde_json::json!({
            "owner": request.owner,
            "operation_id": request.operation_id,
            "digest_run_id": request.digest_run_id,
            "manifest_digest": request.manifest_digest,
            "failure_code": failure_code,
            "failed_at": WireTimestamp::now(),
        }))?;
        ChannelRecapRunStore::new(self.database)
            .settle_failed(recap_run_id, expected, &fact)
            .await
            .map_err(|error| match error {
                crate::ChannelRecapRunError::Persistence(error) => {
                    ChannelRecapPipelineError::Persistence(error)
                }
                _ => ChannelRecapPipelineError::State,
            })?;
        Ok(())
    }

    async fn settle_completion(
        &self,
        recap_run_id: uuid::Uuid,
    ) -> Result<(), ChannelRecapPipelineError> {
        let row: (serde_json::Value, uuid::Uuid, String, serde_json::Value) = sqlx::query_as(
            "select inbox.request_payload, result.result_id, result.result_digest_hex,
                    result.coverage
             from knowledge.channel_recap_runs runs
             join knowledge.channel_recap_inbox inbox on inbox.command_id = runs.inbox_command_id
             join knowledge.channel_recap_results result on result.recap_run_id = runs.recap_run_id
             where runs.recap_run_id = $1 and runs.state = 'persisted'",
        )
        .bind(recap_run_id)
        .fetch_one(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let request: KnowledgeChannelDigestRecapRequested = serde_json::from_value(row.0)?;
        let fact: KnowledgeChannelDigestRecapCompleted =
            serde_json::from_value(serde_json::json!({
                "owner": request.owner,
                "operation_id": request.operation_id,
                "digest_run_id": request.digest_run_id,
                "manifest_digest": request.manifest_digest,
                "analysis_ref": format!("analysis:{}", row.1),
                "digest_result_id": row.1,
                "result_ref": format!("channel-digest-result:{}", row.1),
                "result_digest": {"algorithm": "sha256", "hex": row.2},
                "completed_at": WireTimestamp::now(),
                "coverage": row.3,
            }))?;
        ChannelRecapRunStore::new(self.database)
            .settle_completed(recap_run_id, ChannelRecapRunState::Persisted, &fact)
            .await
            .map_err(|error| match error {
                crate::ChannelRecapRunError::Persistence(error) => {
                    ChannelRecapPipelineError::Persistence(error)
                }
                _ => ChannelRecapPipelineError::State,
            })?;
        Ok(())
    }

    async fn transition(
        &self,
        recap_run_id: uuid::Uuid,
        expected: &str,
        next: &str,
    ) -> Result<(), ChannelRecapPipelineError> {
        let changed = sqlx::query(
            "update knowledge.channel_recap_runs set state = $3, updated_at = now()
             where recap_run_id = $1 and state = $2",
        )
        .bind(recap_run_id)
        .bind(expected)
        .bind(next)
        .execute(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            return Err(ChannelRecapPipelineError::State);
        }
        Ok(())
    }

    async fn resume(
        &self,
        recap_run_id: uuid::Uuid,
    ) -> Result<Option<(ChannelDigestRecap, bool)>, ChannelRecapPipelineError> {
        let stored: Option<(String, Option<serde_json::Value>)> = sqlx::query_as(
            "select r.state, o.result from knowledge.channel_recap_runs r
             left join knowledge.channel_recap_results o on o.recap_run_id = r.recap_run_id
             where r.recap_run_id = $1",
        )
        .bind(recap_run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let Some((state, result)) = stored else {
            return Err(ChannelRecapPipelineError::State);
        };
        if matches!(state.as_str(), "persisted" | "completed") {
            let result = result.ok_or(ChannelRecapPipelineError::State)?;
            let decoded = serde_json::from_value(result)?;
            return Ok(Some((decoded, state == "completed")));
        }
        Ok(None)
    }

    async fn persist_result(
        &self,
        recap_run_id: uuid::Uuid,
        ordinal: i16,
        result: &ChannelDigestRecap,
    ) -> Result<(), ChannelRecapPipelineError> {
        let result_value = serde_json::to_value(result)?;
        let result_bytes = serde_json::to_vec(&result_value)?;
        let result_digest_hex = format!("{:x}", Sha256::digest(result_bytes));
        let coverage = serde_json::to_value(result.coverage)?;
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        sqlx::query(
            "insert into knowledge.channel_recap_results
                (result_id, recap_run_id, result, result_digest_hex, coverage)
             values ($1, $2, $3, $4, $5)",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(recap_run_id)
        .bind(result_value)
        .bind(result_digest_hex)
        .bind(coverage)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        sqlx::query(
            "update knowledge.channel_recap_attempts set outcome = 'accepted'
             where recap_run_id = $1 and ordinal = $2 and outcome = 'response_received'",
        )
        .bind(recap_run_id)
        .bind(ordinal)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        let changed = sqlx::query(
            "update knowledge.channel_recap_runs set state = 'persisted', updated_at = now()
             where recap_run_id = $1 and state = 'schema_validated'",
        )
        .bind(recap_run_id)
        .execute(&mut *transaction)
        .await
        .map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PersistenceError::Query)?;
            return Err(ChannelRecapPipelineError::State);
        }
        transaction
            .commit()
            .await
            .map_err(PersistenceError::Query)?;
        Ok(())
    }
}

fn generation_request(
    request: ChannelRecapProviderRequest,
) -> Result<GenerationRequest, ChannelRecapPipelineError> {
    let source_content = serde_json::to_string(&serde_json::json!({
        "source_labels": request.source_labels,
        "untrusted_sources": request.untrusted_sources,
        "output_language": request.output_language,
        "included_source_count": request.untrusted_sources.len(),
        "allow_external_fetch": request.allow_external_fetch,
        "max_output_tokens": request.max_output_tokens,
    }))?;
    Ok(GenerationRequest {
        prompt_version: request.prompt_version.to_owned(),
        system_policy: request.system_policy,
        task_instruction: request.task_instruction,
        output_schema: request.output_schema,
        source_content,
    })
}

const fn validation_code(error: ChannelRecapResultError) -> &'static str {
    match error {
        ChannelRecapResultError::SchemaDefinition
        | ChannelRecapResultError::Structural
        | ChannelRecapResultError::Decode => "schema",
        ChannelRecapResultError::Citation
        | ChannelRecapResultError::Linkage
        | ChannelRecapResultError::ForbiddenLink => "grounding",
    }
}
