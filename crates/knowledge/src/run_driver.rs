//! The durable provider-attempt loop that the article and the family pipelines share.
//!
//! A run is driven under a per-run advisory lock and always starts from what the database says,
//! never from what the caller expects (XR-021 CONTRACTS.md S02 rule 7 and S09). A redelivery
//! after a failure or a kill therefore finishes the same run instead of failing on a state that
//! has already advanced:
//!
//! - a run at `persisted`, `indexed` or `completed` returns its accepted output;
//! - a run at `failed` whose last call failed transiently, was refused by the provider spend
//!   budget, or was refused for a reason an operator can fix (a rejected credential, an empty
//!   balance, a withdrawn model), is reopened to `queued`, within the per-run budget of spent
//!   calls [`MAX_RUN_ATTEMPTS`] (none of the three is spent, so a provider outage, a budget
//!   window or a wait for the operator never exhausts it); any other failed run reports its
//!   terminal cause;
//! - a run at `model_requested` had a call in flight or between calls: an unfinished call is
//!   recorded as interrupted and the next call continues the run, and a run whose calls were
//!   abandoned twice is failed rather than tried a third time;
//! - a run at `response_received` or `schema_validated` has its raw response on disk: that
//!   response is validated again and the provider is not called for it, and so is a run still at
//!   `model_requested` whose newest attempt already holds its response (a stop between the two
//!   writes that follow an answer).
//!
//! Each drive makes at most two provider calls: the first, and one retry after a transient
//! failure or one repair after invalid output.

use std::future::Future;
use std::time::{Duration, Instant};

use ratatoskr_identifiers::BlobRef;
use uuid::Uuid;

use crate::run_lock::RunLock;
use crate::runs::{AttemptUpdate, MAX_RUN_ATTEMPTS};
use crate::{
    AttemptInput, AttemptOutcome, AttemptReason, BlobError, BlobStore, Database, GenerationRequest,
    LlmProvider, PersistenceError, ProviderError, ProviderFailure, ProviderFailureClass,
    ProviderResponse, RunState, ValidationClass, record_validation_failure,
};

/// Provider calls one drive may make.
const CALLS_PER_DRIVE: u8 = 2;

/// Calls that never recorded an outcome, after which a run is failed instead of called again: a
/// call that keeps ending the process is not a call to repeat.
const MAX_ABANDONED_CALLS: i64 = 2;

/// Why a drive did not produce an accepted output.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DriveFailure {
    /// Durable state could not be read or written.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// A raw response could not be stored or read.
    #[error(transparent)]
    Blob(#[from] BlobError),
    /// The provider failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// The provider output was not valid after the permitted repair.
    #[error("the provider response is invalid")]
    Invalid,
    /// A provider call exceeded its deadline and no call was left.
    #[error("the provider request timed out")]
    Timeout,
    /// Another executor holds the run or advanced it; a later delivery may succeed.
    #[error("the analysis run is held by another executor")]
    Conflict,
}

/// A validator's refusal of one provider response.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Rejected {
    /// Closed code stored on the attempt and used to repair the request.
    pub(crate) code: &'static str,
    /// Telemetry class, when the pipeline reports one.
    pub(crate) class: Option<ValidationClass>,
}

/// Executes the attempts of one run against a provider.
#[derive(Debug)]
pub(crate) struct RunDriver<'a, P> {
    pub(crate) database: &'a Database,
    pub(crate) provider: &'a P,
    pub(crate) blobs: &'a BlobStore,
    pub(crate) provider_timeout: Duration,
    /// Model policy recorded on every attempt of this pipeline.
    pub(crate) model_policy: &'static str,
}

/// What the durable state of a run says.
#[derive(Debug)]
struct Snapshot {
    state: String,
    /// Calls that count against [`MAX_RUN_ATTEMPTS`]: every attempt but a transient failure.
    spent: i64,
    abandoned: i64,
    newest: Option<Newest>,
}

/// The most recent attempt of a run.
#[derive(Debug)]
struct Newest {
    ordinal: i16,
    reason: String,
    outcome: String,
    validation_code: Option<String>,
    raw_response: Option<serde_json::Value>,
}

type SnapshotRow = (
    String,
    i64,
    i64,
    Option<i16>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
);

/// How a stored response resolved.
enum Stored {
    Accepted {
        value: serde_json::Value,
        raw: BlobRef,
    },
    Repair,
}

/// How one attempt ended, when it did not end the drive.
enum Flow {
    Done(serde_json::Value),
    Retry,
    Repair(&'static str),
}

/// What a provider call produced.
enum Called {
    Response(ProviderResponse),
    Retry,
}

/// The identity of one attempt in flight.
#[derive(Debug, Clone, Copy)]
struct Call {
    run_id: Uuid,
    ordinal: i16,
    duration_ms: i32,
    reason: AttemptReason,
    last: bool,
}

impl<P: LlmProvider> RunDriver<'_, P> {
    /// Drives `run_id` to an accepted output, calling `persist` once for a response accepted now.
    ///
    /// `validate` turns the provider bytes into the output value or refuses them; `persist`
    /// stores an accepted value and must move the run from `schema_validated` to `persisted`.
    pub(crate) async fn drive<V, S, F>(
        &self,
        run_id: Uuid,
        request: GenerationRequest,
        validate: V,
        persist: S,
    ) -> Result<serde_json::Value, DriveFailure>
    where
        V: Fn(&[u8]) -> Result<serde_json::Value, Rejected>,
        S: Fn(serde_json::Value, BlobRef) -> F,
        F: Future<Output = Result<(), DriveFailure>>,
    {
        let lock = RunLock::acquire(self.database, run_id)
            .await?
            .ok_or(DriveFailure::Conflict)?;
        let outcome = self
            .drive_locked(run_id, request, &validate, &persist)
            .await;
        lock.release().await;
        outcome
    }

    async fn drive_locked<V, S, F>(
        &self,
        run_id: Uuid,
        mut request: GenerationRequest,
        validate: &V,
        persist: &S,
    ) -> Result<serde_json::Value, DriveFailure>
    where
        V: Fn(&[u8]) -> Result<serde_json::Value, Rejected>,
        S: Fn(serde_json::Value, BlobRef) -> F,
        F: Future<Output = Result<(), DriveFailure>>,
    {
        let mut snapshot = self.snapshot(run_id).await?;
        if matches!(
            snapshot.state.as_str(),
            "persisted" | "indexed" | "completed"
        ) {
            return self.accepted_output(run_id).await;
        }
        if snapshot.state == "failed" {
            self.reopen(run_id, &snapshot).await?;
            snapshot = self.snapshot(run_id).await?;
        }
        if snapshot.state == "model_requested"
            && snapshot
                .newest
                .as_ref()
                .is_some_and(|newest| newest.outcome == "response_received")
        {
            // A stop between storing the response and recording the state change: the response is
            // durable, so the run catches up to it instead of asking the provider again.
            self.transition(run_id, RunState::ModelRequested, RunState::ResponseReceived)
                .await?;
            snapshot = self.snapshot(run_id).await?;
        }
        let mut reason = match snapshot.state.as_str() {
            "response_received" | "schema_validated" => {
                match self
                    .recover_stored(run_id, &snapshot, &mut request, validate)
                    .await?
                {
                    Stored::Accepted { value, raw } => {
                        persist(value.clone(), raw).await?;
                        return Ok(value);
                    }
                    Stored::Repair => AttemptReason::Repair,
                }
            }
            "queued" | "context_prepared" | "repaired" | "model_requested" => {
                self.enter_model_requested(run_id, &snapshot.state).await?;
                self.settle_in_flight(run_id, &snapshot, &mut request)
                    .await?
            }
            _ => return Err(DriveFailure::Conflict),
        };
        for call in 0..CALLS_PER_DRIVE {
            let last = call + 1 == CALLS_PER_DRIVE;
            match self
                .attempt(run_id, &request, reason, last, validate, persist)
                .await?
            {
                Flow::Done(value) => return Ok(value),
                Flow::Retry => reason = AttemptReason::Retry,
                Flow::Repair(code) => {
                    push_repair(&mut request, code);
                    reason = AttemptReason::Repair;
                }
            }
        }
        // Every path that spends the last call has already returned its own failure.
        Err(DriveFailure::Invalid)
    }

    /// Runs one recorded provider call and says how the drive continues.
    async fn attempt<V, S, F>(
        &self,
        run_id: Uuid,
        request: &GenerationRequest,
        reason: AttemptReason,
        last: bool,
        validate: &V,
        persist: &S,
    ) -> Result<Flow, DriveFailure>
    where
        V: Fn(&[u8]) -> Result<serde_json::Value, Rejected>,
        S: Fn(serde_json::Value, BlobRef) -> F,
        F: Future<Output = Result<(), DriveFailure>>,
    {
        let ordinal = self.start_attempt(run_id, reason).await?;
        let started = Instant::now();
        let generated = tokio::time::timeout(
            self.provider_timeout,
            self.provider.generate_json(request.clone()),
        )
        .await;
        let call = Call {
            run_id,
            ordinal,
            duration_ms: i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX),
            reason,
            last,
        };
        let response = match self.settle_call(call, generated).await? {
            Called::Response(response) => response,
            Called::Retry => return Ok(Flow::Retry),
        };
        self.accept_response(call, &response, validate, persist)
            .await
    }

    async fn start_attempt(
        &self,
        run_id: Uuid,
        reason: AttemptReason,
    ) -> Result<i16, DriveFailure> {
        let identity = self.provider.identity();
        let input = AttemptInput {
            reason,
            provider: identity.provider,
            model: identity.model,
            model_policy: self.model_policy.to_owned(),
            provider_request_id: None,
            outcome: AttemptOutcome::Requested,
        };
        match self.database.record_attempt(run_id, &input).await {
            Ok(attempt) => Ok(attempt.ordinal),
            Err(PersistenceError::AttemptBudgetExhausted) => {
                self.fail_requested(run_id).await?;
                Err(PersistenceError::AttemptBudgetExhausted.into())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Records a failed call, or hands back the response of a successful one.
    async fn settle_call(
        &self,
        call: Call,
        generated: Result<Result<ProviderResponse, ProviderFailure>, tokio::time::error::Elapsed>,
    ) -> Result<Called, DriveFailure> {
        // A recoverable failure is recorded as a transient one, which does not count as a spent
        // call. Only a transient failure is worth a second call in the same drive: a spend-budget
        // refusal would be refused again, so it waits for a later delivery.
        let (outcome, retry_now, class, status, failure) = match generated {
            Ok(Ok(response)) => return Ok(Called::Response(response)),
            Err(_) => (
                AttemptOutcome::TransientFailure,
                true,
                ProviderFailureClass::Timeout,
                None,
                DriveFailure::Timeout,
            ),
            Ok(Err(error)) => (
                if error.is_recoverable() {
                    AttemptOutcome::TransientFailure
                } else {
                    AttemptOutcome::PermanentFailure
                },
                error.is_transient(),
                error.class,
                error
                    .http_status
                    .and_then(|status| i16::try_from(status).ok()),
                DriveFailure::Provider(error.error),
            ),
        };
        self.database
            .update_attempt_failure(
                call.run_id,
                call.ordinal,
                outcome,
                Some(class.as_str()),
                status,
                call.duration_ms,
            )
            .await?;
        if retry_now && !call.last {
            return Ok(Called::Retry);
        }
        self.fail_requested(call.run_id).await?;
        Err(failure)
    }

    /// Stores a response, validates it and either persists it or arranges the repair.
    async fn accept_response<V, S, F>(
        &self,
        call: Call,
        response: &ProviderResponse,
        validate: &V,
        persist: &S,
    ) -> Result<Flow, DriveFailure>
    where
        V: Fn(&[u8]) -> Result<serde_json::Value, Rejected>,
        S: Fn(serde_json::Value, BlobRef) -> F,
        F: Future<Output = Result<(), DriveFailure>>,
    {
        let raw = match self.blobs.store_raw(&response.bytes).await {
            Ok(raw) => raw,
            Err(BlobError::TooLarge) => {
                self.database
                    .update_attempt_failure(
                        call.run_id,
                        call.ordinal,
                        AttemptOutcome::PermanentFailure,
                        Some(ProviderFailureClass::SizeLimit.as_str()),
                        None,
                        call.duration_ms,
                    )
                    .await?;
                self.fail_requested(call.run_id).await?;
                return Err(BlobError::TooLarge.into());
            }
            Err(error) => return Err(error.into()),
        };
        self.record_response(call, &raw, response, AttemptOutcome::ResponseReceived, None)
            .await?;
        self.transition(
            call.run_id,
            RunState::ModelRequested,
            RunState::ResponseReceived,
        )
        .await?;
        let rejected = match validate(&response.bytes) {
            Ok(value) => {
                self.record_response(call, &raw, response, AttemptOutcome::Accepted, None)
                    .await?;
                self.transition(
                    call.run_id,
                    RunState::ResponseReceived,
                    RunState::SchemaValidated,
                )
                .await?;
                persist(value.clone(), raw).await?;
                return Ok(Flow::Done(value));
            }
            Err(rejected) => rejected,
        };
        if let Some(class) = rejected.class {
            record_validation_failure(class, "", "");
        }
        self.record_response(
            call,
            &raw,
            response,
            AttemptOutcome::Invalid,
            Some(rejected.code),
        )
        .await?;
        if call.last || call.reason == AttemptReason::Repair {
            self.transition(call.run_id, RunState::ResponseReceived, RunState::Failed)
                .await?;
            return Err(DriveFailure::Invalid);
        }
        self.begin_repair(call.run_id).await?;
        Ok(Flow::Repair(rejected.code))
    }

    async fn record_response(
        &self,
        call: Call,
        raw: &BlobRef,
        response: &ProviderResponse,
        outcome: AttemptOutcome,
        validation_code: Option<&str>,
    ) -> Result<(), DriveFailure> {
        self.database
            .update_attempt(
                call.run_id,
                call.ordinal,
                &AttemptUpdate {
                    raw_response: raw,
                    blobs: self.blobs,
                    held_bytes: &response.bytes,
                    request_id: response.request_id.as_deref(),
                    input_tokens: response.usage.input_tokens,
                    output_tokens: response.usage.output_tokens,
                    outcome,
                    validation_code,
                    duration_ms: call.duration_ms,
                },
            )
            .await?;
        Ok(())
    }

    async fn begin_repair(&self, run_id: Uuid) -> Result<(), DriveFailure> {
        self.transition(run_id, RunState::ResponseReceived, RunState::Repaired)
            .await?;
        self.transition(run_id, RunState::Repaired, RunState::ModelRequested)
            .await
    }

    async fn enter_model_requested(&self, run_id: Uuid, state: &str) -> Result<(), DriveFailure> {
        if state == "queued" {
            self.transition(run_id, RunState::Queued, RunState::ContextPrepared)
                .await?;
        }
        match state {
            "queued" | "context_prepared" => {
                self.transition(run_id, RunState::ContextPrepared, RunState::ModelRequested)
                    .await
            }
            "repaired" => {
                self.transition(run_id, RunState::Repaired, RunState::ModelRequested)
                    .await
            }
            _ => Ok(()),
        }
    }

    /// Decides the reason of the next call of a run that rests at `model_requested`.
    async fn settle_in_flight(
        &self,
        run_id: Uuid,
        snapshot: &Snapshot,
        request: &mut GenerationRequest,
    ) -> Result<AttemptReason, DriveFailure> {
        let Some(newest) = &snapshot.newest else {
            return Ok(AttemptReason::Initial);
        };
        match newest.outcome.as_str() {
            "requested" | "interrupted" => {
                self.database
                    .mark_requested_attempts_interrupted(run_id)
                    .await?;
                if snapshot.abandoned >= MAX_ABANDONED_CALLS {
                    self.fail_requested(run_id).await?;
                    return Err(PersistenceError::AttemptBudgetExhausted.into());
                }
                Ok(AttemptReason::Retry)
            }
            "transient_failure" => Ok(AttemptReason::Retry),
            "invalid" => {
                push_repair(
                    request,
                    newest.validation_code.as_deref().unwrap_or("schema"),
                );
                Ok(AttemptReason::Repair)
            }
            "permanent_failure" => {
                self.fail_requested(run_id).await?;
                Err(ProviderError::Permanent.into())
            }
            _ => Err(DriveFailure::Conflict),
        }
    }

    /// The raw response is durable: validate the stored bytes and never call the provider for it.
    async fn recover_stored<V>(
        &self,
        run_id: Uuid,
        snapshot: &Snapshot,
        request: &mut GenerationRequest,
        validate: &V,
    ) -> Result<Stored, DriveFailure>
    where
        V: Fn(&[u8]) -> Result<serde_json::Value, Rejected>,
    {
        let newest = snapshot.newest.as_ref().ok_or(DriveFailure::Conflict)?;
        let raw: BlobRef =
            serde_json::from_value(newest.raw_response.clone().ok_or(DriveFailure::Conflict)?)
                .map_err(PersistenceError::Encode)?;
        let bytes = self.blobs.read(&raw).await?;
        let validated = snapshot.state == "schema_validated";
        let rejected = match validate(&bytes) {
            Ok(value) => {
                if !validated {
                    self.database
                        .set_attempt_outcome(run_id, newest.ordinal, AttemptOutcome::Accepted, None)
                        .await?;
                    self.transition(
                        run_id,
                        RunState::ResponseReceived,
                        RunState::SchemaValidated,
                    )
                    .await?;
                }
                return Ok(Stored::Accepted { value, raw });
            }
            Err(rejected) => rejected,
        };
        if validated {
            self.transition(run_id, RunState::SchemaValidated, RunState::Failed)
                .await?;
            return Err(DriveFailure::Invalid);
        }
        self.database
            .set_attempt_outcome(
                run_id,
                newest.ordinal,
                AttemptOutcome::Invalid,
                Some(rejected.code),
            )
            .await?;
        if newest.reason == "repair" {
            self.transition(run_id, RunState::ResponseReceived, RunState::Failed)
                .await?;
            return Err(DriveFailure::Invalid);
        }
        self.begin_repair(run_id).await?;
        push_repair(request, rejected.code);
        Ok(Stored::Repair)
    }

    /// Reopens a failed run whose last call failed transiently, or reports why it cannot be.
    async fn reopen(&self, run_id: Uuid, snapshot: &Snapshot) -> Result<(), DriveFailure> {
        let outcome = snapshot
            .newest
            .as_ref()
            .map(|newest| newest.outcome.as_str());
        match outcome {
            Some("transient_failure") if snapshot.spent < i64::from(MAX_RUN_ATTEMPTS) => {
                if self.database.reopen_failed_run(run_id).await? {
                    Ok(())
                } else {
                    Err(DriveFailure::Conflict)
                }
            }
            Some("permanent_failure") => Err(ProviderError::Permanent.into()),
            Some("invalid" | "response_received" | "accepted") => Err(DriveFailure::Invalid),
            _ => Err(PersistenceError::AttemptBudgetExhausted.into()),
        }
    }

    async fn accepted_output(&self, run_id: Uuid) -> Result<serde_json::Value, DriveFailure> {
        let result: Option<serde_json::Value> = sqlx::query_scalar(
            "select result from knowledge.analysis_outputs where run_id = $1 and accepted",
        )
        .bind(run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        result.ok_or_else(|| PersistenceError::InvalidAnalysisIdentity.into())
    }

    async fn snapshot(&self, run_id: Uuid) -> Result<Snapshot, DriveFailure> {
        let row: Option<SnapshotRow> = sqlx::query_as(
            "select r.state,
                    (select count(*) from knowledge.analysis_attempts
                      where run_id = r.run_id and outcome <> 'transient_failure'),
                    (select count(*) from knowledge.analysis_attempts
                      where run_id = r.run_id and outcome in ('requested', 'interrupted')),
                    n.ordinal, n.reason, n.outcome, n.validation_code, n.raw_response
             from knowledge.analysis_runs r
             left join lateral (
                select ordinal, reason, outcome, validation_code, raw_response
                from knowledge.analysis_attempts
                where run_id = r.run_id order by ordinal desc limit 1
             ) n on true
             where r.run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(PersistenceError::Query)?;
        let (state, spent, abandoned, ordinal, reason, outcome, validation_code, raw_response) =
            row.ok_or(PersistenceError::InvalidAnalysisIdentity)?;
        let newest = match (ordinal, reason, outcome) {
            (Some(ordinal), Some(reason), Some(outcome)) => Some(Newest {
                ordinal,
                reason,
                outcome,
                validation_code,
                raw_response,
            }),
            _ => None,
        };
        Ok(Snapshot {
            state,
            spent,
            abandoned,
            newest,
        })
    }

    async fn transition(
        &self,
        run_id: Uuid,
        expected: RunState,
        next: RunState,
    ) -> Result<(), DriveFailure> {
        if self.database.transition_run(run_id, expected, next).await? {
            Ok(())
        } else {
            Err(DriveFailure::Conflict)
        }
    }

    async fn fail_requested(&self, run_id: Uuid) -> Result<(), DriveFailure> {
        self.transition(run_id, RunState::ModelRequested, RunState::Failed)
            .await
    }
}

fn push_repair(request: &mut GenerationRequest, code: &str) {
    request
        .task_instruction
        .push_str("\nRepair validation code: ");
    request.task_instruction.push_str(code);
    request.task_instruction.push('.');
}
