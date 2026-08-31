use std::collections::VecDeque;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use crate::GenerationRequest;

/// Safe bounded token usage reported by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderUsage {
    /// Provider-counted input tokens.
    pub input_tokens: u64,
    /// Provider-counted output tokens.
    pub output_tokens: u64,
}

/// Raw provider response plus safe transport facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponse {
    /// Raw response bytes, still untrusted.
    pub bytes: Vec<u8>,
    /// Provider request identity when available.
    pub request_id: Option<String>,
    /// Bounded provider token usage.
    pub usage: ProviderUsage,
}

/// Safe provider failure classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// The call may be retried within the shared call budget.
    #[error("the provider failed transiently")]
    Transient,
    /// The call must not be retried.
    #[error("the provider failed permanently")]
    Permanent,
    /// The configured spend budget refuses further provider calls.
    #[error("the provider spend budget is exhausted")]
    BudgetExhausted,
    /// The deterministic script has no outcome left.
    #[error("the scripted provider is exhausted")]
    Exhausted,
    /// The deterministic fake's internal lock was poisoned.
    #[error("the scripted provider failed internally")]
    Internal,
}

/// Closed transport-failure vocabulary safe for durable attempt records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProviderFailureClass {
    /// The call deadline elapsed before a complete response.
    Timeout,
    /// Connection-level fault before an HTTP status existed.
    Network,
    /// The provider asked the caller to slow down.
    RateLimited,
    /// A server-side fault outside Knowledge's control.
    ServerError,
    /// Authorization to the provider failed.
    AuthError,
    /// Knowledge sent something the provider rejects permanently.
    RequestInvalid,
    /// The response exceeded the configured byte cap.
    SizeLimit,
    /// The durable spend ledger refused or ended this call.
    BudgetExhausted,
    /// No transport classification exists, as with scripted outcomes.
    Unclassified,
}

impl ProviderFailureClass {
    /// Returns the stable database spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Network => "network",
            Self::RateLimited => "rate_limited",
            Self::ServerError => "server_error",
            Self::AuthError => "auth_error",
            Self::RequestInvalid => "request_invalid",
            Self::SizeLimit => "size_limit",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Unclassified => "unclassified",
        }
    }
}

/// One failed provider call with its bounded classification facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{error}")]
pub struct ProviderFailure {
    /// Retry classification used by the pipeline call budget.
    pub error: ProviderError,
    /// Closed vocabulary recorded on the attempt row.
    pub class: ProviderFailureClass,
    /// Observed HTTP status when one exists.
    pub http_status: Option<u16>,
}

impl ProviderFailure {
    /// Builds a failure without transport facts.
    #[must_use]
    pub const fn unclassified(error: ProviderError) -> Self {
        Self {
            error,
            class: ProviderFailureClass::Unclassified,
            http_status: None,
        }
    }

    /// Reports whether the pipeline may retry within its shared call budget.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self.error, ProviderError::Transient)
    }
}

impl From<ProviderError> for ProviderFailure {
    fn from(error: ProviderError) -> Self {
        if error == ProviderError::BudgetExhausted {
            Self {
                error,
                class: ProviderFailureClass::BudgetExhausted,
                http_status: None,
            }
        } else {
            Self::unclassified(error)
        }
    }
}

/// Adapter and model identity declared by every provider implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    /// Bounded adapter name recorded on attempts.
    pub provider: String,
    /// Concrete upstream model id recorded on attempts.
    pub model: String,
}

/// Whether an ambiguous accepted provider request can be repeated automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderRetrySafety {
    /// The provider has no idempotency or reconciliation primitive.
    Uncertain,
    /// The adapter proves repeated request identities have one external effect.
    Idempotent,
}

/// Narrow provider-neutral JSON generation boundary.
pub trait LlmProvider: Send + Sync {
    /// Returns the stable adapter and model identity for attempt records.
    fn identity(&self) -> ProviderIdentity;

    /// Declares whether transport-ambiguous calls can be replayed automatically.
    fn retry_safety(&self) -> ProviderRetrySafety {
        ProviderRetrySafety::Uncertain
    }

    /// Generates one raw JSON response.
    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderFailure>> + Send;
}

/// Ordered deterministic provider fake used by default tests.
#[derive(Debug, Clone)]
pub struct ScriptedProvider {
    scripts: Arc<Mutex<VecDeque<Result<ProviderResponse, ProviderError>>>>,
    requests: Arc<Mutex<Vec<GenerationRequest>>>,
}

impl ScriptedProvider {
    /// Creates a fake from ordered outcomes.
    #[must_use]
    pub fn new(scripts: impl IntoIterator<Item = Result<ProviderResponse, ProviderError>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns the bounded requests observed so far.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] when the capture lock was poisoned.
    pub fn requests(&self) -> Result<Vec<GenerationRequest>, ProviderError> {
        let requests = self.requests.lock().map_err(|_| ProviderError::Internal)?;
        Ok(requests.clone())
    }

    /// Returns the observed call count.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] when the capture lock was poisoned.
    pub fn call_count(&self) -> Result<usize, ProviderError> {
        let requests = self.requests.lock().map_err(|_| ProviderError::Internal)?;
        Ok(requests.len())
    }
}

impl LlmProvider for ScriptedProvider {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
        }
    }

    fn retry_safety(&self) -> ProviderRetrySafety {
        ProviderRetrySafety::Idempotent
    }

    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderFailure>> + Send {
        let recorded = self
            .requests
            .lock()
            .map(|mut requests| requests.push(request))
            .map_err(|_| ProviderError::Internal);
        let outcome: Result<ProviderResponse, ProviderFailure> = match recorded {
            Ok(()) => self
                .scripts
                .lock()
                .map_err(|_| ProviderError::Internal)
                .and_then(|mut scripts| match scripts.pop_front() {
                    Some(outcome) => outcome,
                    None => Err(ProviderError::Exhausted),
                })
                .map_err(ProviderFailure::from),
            Err(error) => Err(ProviderFailure::unclassified(error)),
        };
        ready(outcome)
    }
}
