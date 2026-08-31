//! Durable execution and search projection for repository, social, and archive analyses.

use ratatoskr_ai_archive_contracts::{
    AiArchiveAnalysisCompleted, AiArchiveSubject, AiConversation,
};
use ratatoskr_github_contracts::{
    ReadmeRevision, RepositoryAnalysisCompleted, RepositoryAnalysisRequested,
};
use ratatoskr_identifiers::{
    BlobRef, ContentDigest, DocumentId, EntityRef, Extensions, TenantRef, WireTimestamp,
};
use ratatoskr_social_contracts::SocialSourceSnapshot;
use uuid::Uuid;

use crate::runs::{AttemptUpdate, RunResumeAction};
use crate::search::{
    SearchDocumentProjection, record_search_document, record_search_projection_input,
};
use crate::{
    AnalysisIdentity, AttemptInput, AttemptOutcome, AttemptReason, BlobError, BlobStore, Database,
    FamilyValidationError, GenerationRequest, LlmProvider, PersistenceError, ProviderError,
    ProviderFailureClass, ProviderIdentity, ProviderResponse, ProviderRetrySafety, ProviderUsage,
    RepositoryAnalysis, RepositoryAnalysisAdmission, RepositoryAnalysisConsumer, RunState,
    SocialAnalysis, SourceInbox, SourceInboxError, SourceReference, archive_generation_request,
    archive_project_generation_request, repository_generation_request, social_generation_request,
    validate_archive_analysis, validate_archive_project_analysis, validate_repository_analysis,
    validate_social_analysis,
};

mod helpers;
mod types;
use helpers::{repository_digest, verify_readme};
pub use types::{
    ArchiveAnalysisExecution, FamilyPipelineError, RepositoryAnalysisExecution,
    RepositoryReadmeError, RepositoryReadmeResolver,
};

/// Durable, idempotent family-analysis worker.
#[derive(Debug)]
pub struct FamilyPipeline<'a, P> {
    database: &'a Database,
    provider: &'a P,
    blobs: &'a BlobStore,
    provider_timeout: std::time::Duration,
}

impl<'a, P: LlmProvider> FamilyPipeline<'a, P> {
    /// Creates a worker. `provider` must be composed with [`crate::ControlledProvider`] in
    /// production so every family shares the same durable budget ledger.
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

    /// Executes one social delivery already claimed by the durable inbox.
    ///
    /// # Errors
    ///
    /// Returns [`FamilyPipelineError`] for absent inbox state, provider, validation, or persistence failure.
    pub async fn execute_social_event(
        &self,
        inbox: &SourceInbox<'_>,
        event_id: Uuid,
    ) -> Result<SocialAnalysis, FamilyPipelineError> {
        let snapshot = inbox.social_snapshot(event_id).await?;
        self.execute_social(&snapshot).await
    }

    /// Executes one social snapshot revision idempotently.
    ///
    /// # Errors
    ///
    /// Returns [`FamilyPipelineError`] for provider, validation, or persistence failure.
    pub async fn execute_social(
        &self,
        snapshot: &SocialSourceSnapshot,
    ) -> Result<SocialAnalysis, FamilyPipelineError> {
        let source = self
            .snapshot_source(
                snapshot.owner,
                &snapshot.social_source_id.to_string(),
                snapshot.content_digest.clone(),
                snapshot,
                "",
            )
            .await?;
        let run = self
            .create_run(
                source,
                "social_analysis_v1",
                "social_prompt_v1",
                "social_context_v1",
            )
            .await?;
        let request = social_generation_request(snapshot)?;
        let value = self
            .execute_value(
                run,
                request,
                SearchFields {
                    title: format!("{} post", snapshot.platform.as_str()),
                    lead: snapshot
                        .text
                        .as_ref()
                        .map_or_else(String::new, |text| text.as_str().to_owned()),
                    body: snapshot
                        .text
                        .as_ref()
                        .map_or_else(String::new, |text| text.as_str().to_owned()),
                },
                |value| {
                    validate_social_analysis(value, snapshot).and_then(|analysis| {
                        serde_json::to_value(analysis).map_err(|_| FamilyValidationError::Decode)
                    })
                },
            )
            .await?;
        serde_json::from_value(value).map_err(FamilyPipelineError::Contract)
    }

    /// Executes one archive delivery already claimed by the durable inbox.
    ///
    /// # Errors
    ///
    /// Returns [`FamilyPipelineError`] for absent inbox state, provider, validation, or persistence failure.
    pub async fn execute_archive_event(
        &self,
        inbox: &SourceInbox<'_>,
        event_id: Uuid,
    ) -> Result<ArchiveAnalysisExecution, FamilyPipelineError> {
        let source = inbox.archive_conversation(event_id).await?;
        let analysis = self
            .execute_archive_with_provenance(
                &source.conversation,
                &source.provenance.ai_archive_id.to_string(),
            )
            .await?;
        Ok(ArchiveAnalysisExecution {
            analysis,
            completion: AiArchiveAnalysisCompleted {
                ai_archive_id: source.provenance.ai_archive_id,
                owner: source.conversation.owner,
                subject: AiArchiveSubject::Conversation {
                    ai_conversation_id: source.conversation.ai_conversation_id,
                },
                content_digest: source.conversation.content_digest,
                completed_at: WireTimestamp::now(),
                extensions: Extensions::new(),
            },
        })
    }

    /// Executes one conversation revision idempotently.
    ///
    /// # Errors
    ///
    /// Returns [`FamilyPipelineError`] for provider, validation, or persistence failure.
    pub async fn execute_archive(
        &self,
        conversation: &AiConversation,
    ) -> Result<crate::ArchiveAnalysis, FamilyPipelineError> {
        self.execute_archive_with_provenance(conversation, "").await
    }

    async fn execute_archive_with_provenance(
        &self,
        conversation: &AiConversation,
        ai_archive_id: &str,
    ) -> Result<crate::ArchiveAnalysis, FamilyPipelineError> {
        let source = self
            .snapshot_source(
                conversation.owner,
                &conversation.ai_conversation_id.to_string(),
                conversation.content_digest.clone(),
                conversation,
                ai_archive_id,
            )
            .await?;
        let run = self
            .create_run(
                source,
                "archive_analysis_v1",
                "archive_prompt_v1",
                "archive_context_v1",
            )
            .await?;
        let request = archive_generation_request(conversation)?;
        let title = conversation.title.as_ref().map_or_else(
            || "AI conversation".to_owned(),
            |title| title.as_str().to_owned(),
        );
        let body = crate::archive_context(conversation);
        let value = self
            .execute_value(
                run,
                request,
                SearchFields {
                    title,
                    lead: String::new(),
                    body,
                },
                |value| {
                    validate_archive_analysis(value, conversation).and_then(|analysis| {
                        serde_json::to_value(analysis).map_err(|_| FamilyValidationError::Decode)
                    })
                },
            )
            .await?;
        serde_json::from_value(value).map_err(FamilyPipelineError::Contract)
    }

    /// Executes one archive project delivery already claimed by the durable inbox.
    ///
    /// # Errors
    ///
    /// Returns [`FamilyPipelineError`] for absent inbox state, provider, validation, or persistence failure.
    pub async fn execute_archive_project_event(
        &self,
        inbox: &SourceInbox<'_>,
        event_id: Uuid,
    ) -> Result<crate::ArchiveProjectAnalysis, FamilyPipelineError> {
        let source = inbox.archive_project(event_id).await?;
        let project = &source.project;
        let source_ref = self
            .snapshot_source(
                source.provenance.owner,
                &project.ai_project_id.to_string(),
                source.content_digest,
                project,
                &source.provenance.ai_archive_id.to_string(),
            )
            .await?;
        let run = self
            .create_run(
                source_ref,
                "archive_project_analysis_v1",
                "archive_project_prompt_v1",
                "archive_project_context_v1",
            )
            .await?;
        let request = archive_project_generation_request(project)?;
        let title = project.title.as_str().to_owned();
        let lead = project
            .description
            .as_ref()
            .map_or_else(String::new, |value| value.as_str().to_owned());
        let body = project
            .instructions
            .as_ref()
            .map_or_else(|| lead.clone(), |value| value.as_str().to_owned());
        let value = self
            .execute_value(run, request, SearchFields { title, lead, body }, |value| {
                validate_archive_project_analysis(value, project).and_then(|analysis| {
                    serde_json::to_value(analysis).map_err(|_| FamilyValidationError::Decode)
                })
            })
            .await?;
        serde_json::from_value(value).map_err(FamilyPipelineError::Contract)
    }

    /// Accepts and executes one repository request using an authorized README resolver.
    ///
    /// # Errors
    ///
    /// Returns [`FamilyPipelineError`] for invalid request/source identity, README acquisition,
    /// provider, validation, or persistence failure.
    pub async fn execute_repository<R: RepositoryReadmeResolver>(
        &self,
        request: &RepositoryAnalysisRequested,
        resolver: &R,
    ) -> Result<RepositoryAnalysisExecution, FamilyPipelineError> {
        let consumer = RepositoryAnalysisConsumer::new(self.database);
        let _admission: RepositoryAnalysisAdmission = consumer
            .accept(request)
            .await
            .map_err(|_| FamilyPipelineError::Source)?;
        let (readme, source) = match &request.source_revision.readme {
            ReadmeRevision::Present { content_ref } => {
                let bytes = resolver.read_readme(request, content_ref).await?;
                verify_readme(content_ref, &bytes)?;
                let text =
                    String::from_utf8(bytes).map_err(|_| RepositoryReadmeError::Integrity)?;
                (
                    Some(text),
                    self.external_source(
                        request.owner,
                        &request.repository_id.to_string(),
                        repository_digest(request)?,
                        content_ref.clone(),
                    )
                    .await?,
                )
            }
            ReadmeRevision::Absent { .. } => {
                let source = self
                    .snapshot_source(
                        request.owner,
                        &request.repository_id.to_string(),
                        repository_digest(request)?,
                        request,
                        "",
                    )
                    .await?;
                (None, source)
            }
            _ => return Err(FamilyPipelineError::Source),
        };
        let run = self
            .create_run(
                source,
                "repository_analysis_v1",
                "repository_prompt_v1",
                "repository_context_v1",
            )
            .await?;
        let request_json = repository_generation_request(request, readme.as_deref())?;
        let full_name = request.repository_attributes.repository_full_name.as_str();
        let lead = request
            .repository_attributes
            .description
            .as_ref()
            .map_or_else(String::new, |value| value.as_str().to_owned());
        let body = readme.as_ref().map_or_else(|| lead.clone(), Clone::clone);
        let value = self
            .execute_value(
                run,
                request_json,
                SearchFields {
                    title: full_name.to_owned(),
                    lead,
                    body,
                },
                |value| {
                    validate_repository_analysis(value, request, readme.as_deref()).and_then(
                        |analysis| {
                            serde_json::to_value(analysis)
                                .map_err(|_| FamilyValidationError::Decode)
                        },
                    )
                },
            )
            .await?;
        let analysis: RepositoryAnalysis =
            serde_json::from_value(value).map_err(FamilyPipelineError::Contract)?;
        let result_ref = EntityRef::parse(&format!("analysis:{run}"))
            .map_err(|_| FamilyPipelineError::Source)?;
        let completion = Some(RepositoryAnalysisCompleted {
            owner: request.owner,
            repository_id: request.repository_id,
            github_repository_numeric_id: request.github_repository_numeric_id,
            request_id: request.request_id,
            source_revision: request.source_revision.clone(),
            analysis_result_ref: result_ref,
            completed_at: WireTimestamp::now(),
            extensions: Extensions::new(),
        });
        Ok(RepositoryAnalysisExecution {
            analysis,
            completion,
        })
    }

    async fn snapshot_source<T: serde::Serialize>(
        &self,
        tenant: TenantRef,
        source_id: &str,
        digest: ContentDigest,
        snapshot: &T,
        ai_archive_id: &str,
    ) -> Result<Uuid, FamilyPipelineError> {
        let bytes = serde_json::to_vec(snapshot)?;
        let blob = self.blobs.store_raw(&bytes).await?;
        self.register_source(
            tenant,
            source_id,
            digest,
            "ratatoskr-knowledge",
            ai_archive_id,
            blob,
        )
        .await
    }

    async fn external_source(
        &self,
        tenant: TenantRef,
        source_id: &str,
        digest: ContentDigest,
        blob: BlobRef,
    ) -> Result<Uuid, FamilyPipelineError> {
        let owner_context = blob.owner_service.as_str().to_owned();
        self.register_source(tenant, source_id, digest, &owner_context, "", blob)
            .await
    }

    async fn register_source(
        &self,
        tenant: TenantRef,
        source_id: &str,
        digest: ContentDigest,
        owner_context: &str,
        ai_archive_id: &str,
        blob: BlobRef,
    ) -> Result<Uuid, FamilyPipelineError> {
        let document_id = DocumentId::parse(source_id).map_err(|_| FamilyPipelineError::Source)?;
        Ok(self
            .database
            .register_source(&SourceReference {
                tenant,
                owner_context: owner_context.to_owned(),
                ai_archive_id: ai_archive_id.to_owned(),
                document_id,
                content_digest: digest,
                source_blob: blob,
            })
            .await?
            .id)
    }

    async fn create_run(
        &self,
        source_ref: Uuid,
        contract: &str,
        prompt: &str,
        context: &str,
    ) -> Result<Uuid, FamilyPipelineError> {
        Ok(self
            .database
            .create_run(&AnalysisIdentity {
                source_revision_id: source_ref,
                contract_version: contract.to_owned(),
                prompt_version: prompt.to_owned(),
                context_builder_version: context.to_owned(),
                model_policy: "family_default_v1".to_owned(),
            })
            .await?
            .id)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one place owns every explicit durable state transition for a provider attempt"
    )]
    async fn execute_value<F>(
        &self,
        run_id: Uuid,
        mut request: GenerationRequest,
        fields: SearchFields,
        validate: F,
    ) -> Result<serde_json::Value, FamilyPipelineError>
    where
        F: Fn(&serde_json::Value) -> Result<serde_json::Value, FamilyValidationError>,
    {
        let action = self
            .database
            .prepare_run_resume(run_id, self.provider.retry_safety())
            .await?;
        let mut reason = AttemptReason::Initial;
        let mut first_call = 0_u8;
        let mut call_limit = 2_u8;
        let mut stored_state = None;
        let mut stored_attempt = None;
        match action {
            RunResumeAction::Output(value) => return Ok(value),
            RunResumeAction::ProviderOutcomeUnknown => {
                return Err(FamilyPipelineError::ProviderOutcomeUnknown);
            }
            RunResumeAction::Failed => return Err(FamilyPipelineError::Invalid),
            RunResumeAction::StoredResponse { state, attempt } => {
                stored_state = Some(state);
                stored_attempt = Some(attempt);
            }
            RunResumeAction::Call {
                first_call: next_call,
                call_limit: next_limit,
                reason: next_reason,
                repair_code,
            } => {
                first_call = next_call;
                call_limit = next_limit;
                reason = next_reason;
                if repair_code.is_some() {
                    request
                        .task_instruction
                        .push_str("\nRepair validation code: family_schema.");
                }
            }
        }

        if let Some(attempt) = stored_attempt.as_ref() {
            let raw = attempt
                .raw_response
                .as_ref()
                .ok_or(FamilyPipelineError::Source)?;
            let response = ProviderResponse {
                bytes: self.blobs.read(raw).await?,
                request_id: attempt.request_id.clone(),
                usage: ProviderUsage {
                    input_tokens: attempt.input_tokens,
                    output_tokens: attempt.output_tokens,
                },
            };
            if stored_state == Some(RunState::ModelRequested) {
                self.transition(run_id, RunState::ModelRequested, RunState::ResponseReceived)
                    .await?;
            }
            let value = serde_json::from_slice::<serde_json::Value>(&response.bytes)
                .ok()
                .and_then(|value| validate(&value).ok());
            if let Some(value) = value {
                self.database
                    .update_attempt(
                        run_id,
                        attempt.ordinal,
                        &AttemptUpdate {
                            raw_response: raw,
                            request_id: response.request_id.as_deref(),
                            input_tokens: response.usage.input_tokens,
                            output_tokens: response.usage.output_tokens,
                            outcome: AttemptOutcome::Accepted,
                            validation_code: None,
                            duration_ms: attempt.duration_ms,
                        },
                    )
                    .await?;
                if stored_state != Some(RunState::SchemaValidated) {
                    self.transition(
                        run_id,
                        RunState::ResponseReceived,
                        RunState::SchemaValidated,
                    )
                    .await?;
                }
                self.persist(run_id, value.clone(), raw.clone(), fields)
                    .await?;
                return Ok(value);
            }
            self.database
                .update_attempt(
                    run_id,
                    attempt.ordinal,
                    &AttemptUpdate {
                        raw_response: raw,
                        request_id: response.request_id.as_deref(),
                        input_tokens: response.usage.input_tokens,
                        output_tokens: response.usage.output_tokens,
                        outcome: AttemptOutcome::Invalid,
                        validation_code: Some("family_schema"),
                        duration_ms: attempt.duration_ms,
                    },
                )
                .await?;
            if stored_state == Some(RunState::SchemaValidated) {
                self.transition(run_id, RunState::SchemaValidated, RunState::Failed)
                    .await?;
                return Err(FamilyPipelineError::Invalid);
            }
            if attempt.ordinal >= 2 {
                self.transition(run_id, RunState::ResponseReceived, RunState::Failed)
                    .await?;
                return Err(FamilyPipelineError::Invalid);
            }
            self.transition(run_id, RunState::ResponseReceived, RunState::Repaired)
                .await?;
            self.transition(run_id, RunState::Repaired, RunState::ModelRequested)
                .await?;
            request
                .task_instruction
                .push_str("\nRepair validation code: family_schema.");
            first_call = u8::try_from(attempt.ordinal).unwrap_or(2);
            reason = AttemptReason::Repair;
        }

        for call in first_call..call_limit {
            let attempt = self
                .database
                .record_attempt(run_id, &attempt_input(&self.provider.identity(), reason))
                .await?;
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(
                self.provider_timeout,
                self.provider.generate_json(request.clone()),
            )
            .await;
            let duration_ms = i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX);
            let response = match result {
                Err(_) => {
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
                    if call == 0 && self.provider.retry_safety() == ProviderRetrySafety::Idempotent
                    {
                        reason = AttemptReason::Retry;
                        continue;
                    }
                    if self.provider.retry_safety() == ProviderRetrySafety::Uncertain {
                        self.transition(
                            run_id,
                            RunState::ModelRequested,
                            RunState::ProviderOutcomeUnknown,
                        )
                        .await?;
                        return Err(FamilyPipelineError::ProviderOutcomeUnknown);
                    }
                    self.fail(run_id).await?;
                    return Err(FamilyPipelineError::Timeout);
                }
                Ok(Err(failure)) => {
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
                    if failure.is_transient()
                        && call == 0
                        && self.provider.retry_safety() == ProviderRetrySafety::Idempotent
                    {
                        reason = AttemptReason::Retry;
                        continue;
                    }
                    if failure.is_transient()
                        && self.provider.retry_safety() == ProviderRetrySafety::Uncertain
                    {
                        self.transition(
                            run_id,
                            RunState::ModelRequested,
                            RunState::ProviderOutcomeUnknown,
                        )
                        .await?;
                        return Err(FamilyPipelineError::ProviderOutcomeUnknown);
                    }
                    self.fail(run_id).await?;
                    return Err(FamilyPipelineError::Provider(failure.error));
                }
                Ok(Ok(response)) => response,
            };
            let raw = self.blobs.store_raw(&response.bytes).await?;
            self.database
                .update_attempt(
                    run_id,
                    attempt.ordinal,
                    &AttemptUpdate {
                        raw_response: &raw,
                        request_id: response.request_id.as_deref(),
                        input_tokens: response.usage.input_tokens,
                        output_tokens: response.usage.output_tokens,
                        outcome: AttemptOutcome::ResponseReceived,
                        validation_code: None,
                        duration_ms,
                    },
                )
                .await?;
            self.transition(run_id, RunState::ModelRequested, RunState::ResponseReceived)
                .await?;
            let value = serde_json::from_slice::<serde_json::Value>(&response.bytes)
                .ok()
                .and_then(|value| validate(&value).ok());
            let Some(value) = value else {
                self.database
                    .update_attempt(
                        run_id,
                        attempt.ordinal,
                        &AttemptUpdate {
                            raw_response: &raw,
                            request_id: response.request_id.as_deref(),
                            input_tokens: response.usage.input_tokens,
                            output_tokens: response.usage.output_tokens,
                            outcome: AttemptOutcome::Invalid,
                            validation_code: Some("family_schema"),
                            duration_ms,
                        },
                    )
                    .await?;
                if call == 0 {
                    self.transition(run_id, RunState::ResponseReceived, RunState::Repaired)
                        .await?;
                    self.transition(run_id, RunState::Repaired, RunState::ModelRequested)
                        .await?;
                    request
                        .task_instruction
                        .push_str("\nRepair validation code: family_schema.");
                    reason = AttemptReason::Repair;
                    continue;
                }
                self.transition(run_id, RunState::ResponseReceived, RunState::Failed)
                    .await?;
                return Err(FamilyPipelineError::Invalid);
            };
            self.database
                .update_attempt(
                    run_id,
                    attempt.ordinal,
                    &AttemptUpdate {
                        raw_response: &raw,
                        request_id: response.request_id.as_deref(),
                        input_tokens: response.usage.input_tokens,
                        output_tokens: response.usage.output_tokens,
                        outcome: AttemptOutcome::Accepted,
                        validation_code: None,
                        duration_ms,
                    },
                )
                .await?;
            self.transition(
                run_id,
                RunState::ResponseReceived,
                RunState::SchemaValidated,
            )
            .await?;
            self.persist(run_id, value.clone(), raw, fields).await?;
            return Ok(value);
        }
        Err(FamilyPipelineError::Invalid)
    }

    async fn transition(
        &self,
        run_id: Uuid,
        expected: RunState,
        next: RunState,
    ) -> Result<(), FamilyPipelineError> {
        if self.database.transition_run(run_id, expected, next).await? {
            Ok(())
        } else {
            Err(FamilyPipelineError::Source)
        }
    }

    async fn fail(&self, run_id: Uuid) -> Result<(), FamilyPipelineError> {
        self.transition(run_id, RunState::ModelRequested, RunState::Failed)
            .await
    }

    async fn persist(
        &self,
        run_id: Uuid,
        result: serde_json::Value,
        raw: BlobRef,
        fields: SearchFields,
    ) -> Result<(), FamilyPipelineError> {
        let raw = serde_json::to_value(raw)?;
        let mut tx = self
            .database
            .pool()
            .begin()
            .await
            .map_err(PersistenceError::Query)?;
        let output_id = Uuid::now_v7();
        sqlx::query("insert into knowledge.analysis_outputs (output_id, run_id, result, raw_response) values ($1, $2, $3, $4)")
            .bind(output_id).bind(run_id).bind(result).bind(raw).execute(&mut *tx).await.map_err(PersistenceError::Query)?;
        let changed = sqlx::query("update knowledge.analysis_runs set state = 'persisted', updated_at = now() where run_id = $1 and state = 'schema_validated'")
            .bind(run_id).execute(&mut *tx).await.map_err(PersistenceError::Query)?;
        if changed.rows_affected() != 1 {
            return Err(FamilyPipelineError::Source);
        }
        let (source_ref_id, tenant_ref, owner_context, document_id): (Uuid, String, String, String) = sqlx::query_as(
            "select s.source_ref_id, s.tenant_ref, s.owner_context, s.source_document_id from knowledge.analysis_runs r join knowledge.source_refs s on s.source_ref_id = r.source_ref_id where r.run_id = $1",
        ).bind(run_id).fetch_one(&mut *tx).await.map_err(PersistenceError::Query)?;
        let document_id = Uuid::parse_str(&document_id).map_err(|_| FamilyPipelineError::Source)?;
        let projection = SearchDocumentProjection {
            source_ref_id,
            latest_output_id: output_id,
            tenant_ref,
            owner_context,
            document_id,
            title: fields.title,
            lead: fields.lead,
            body: fields.body,
        };
        record_search_projection_input(&mut *tx, &projection)
            .await
            .map_err(PersistenceError::Query)?;
        record_search_document(&mut *tx, &projection)
            .await
            .map_err(PersistenceError::Query)?;
        tx.commit().await.map_err(PersistenceError::Query)?;
        Ok(())
    }
}

#[derive(Debug)]
struct SearchFields {
    title: String,
    lead: String,
    body: String,
}

fn attempt_input(identity: &ProviderIdentity, reason: AttemptReason) -> AttemptInput {
    AttemptInput {
        reason,
        provider: identity.provider.clone(),
        model: identity.model.clone(),
        model_policy: "family_default_v1".to_owned(),
        provider_request_id: None,
        outcome: AttemptOutcome::Requested,
    }
}
