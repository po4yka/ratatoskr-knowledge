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
    /// The deterministic script has no outcome left.
    #[error("the scripted provider is exhausted")]
    Exhausted,
    /// The deterministic fake's internal lock was poisoned.
    #[error("the scripted provider failed internally")]
    Internal,
}

/// Narrow provider-neutral JSON generation boundary.
pub trait LlmProvider: Send + Sync {
    /// Generates one raw JSON response.
    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderError>> + Send;
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
    fn generate_json(
        &self,
        request: GenerationRequest,
    ) -> impl Future<Output = Result<ProviderResponse, ProviderError>> + Send {
        let recorded = self
            .requests
            .lock()
            .map(|mut requests| requests.push(request))
            .map_err(|_| ProviderError::Internal);
        let outcome = match recorded {
            Ok(()) => self
                .scripts
                .lock()
                .map_err(|_| ProviderError::Internal)
                .and_then(|mut scripts| match scripts.pop_front() {
                    Some(outcome) => outcome,
                    None => Err(ProviderError::Exhausted),
                }),
            Err(error) => Err(error),
        };
        ready(outcome)
    }
}
