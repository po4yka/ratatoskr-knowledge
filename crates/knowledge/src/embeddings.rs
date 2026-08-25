//! Narrow embeddings provider seam beside the chat-completions seam.
//!
//! The seam mirrors the chat-completions discipline of [`crate::provider`]
//! and [`crate::openrouter`]: one RPITIT trait, one deterministic scripted
//! fake, one `OpenAI`-compatible wire adapter with finite deadlines, a
//! streaming response byte cap, transient-only jittered retry, and closed
//! failure classification, plus one control wrapper composing rate
//! admission, durable budget refusal, bounded tracing, and usage recording.
//! No credential ever enters a serialized body; authorization is a transport
//! concern only.

use std::collections::VecDeque;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BudgetError, BudgetLedger, BudgetLimits, ProviderError, ProviderFailure, ProviderFailureClass,
    ProviderSecret, ProviderUsage, RateLimiter, RetryPolicy, TokenPrices, classify_error,
};

/// Adapter and model identity declared by every embeddings implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingIdentity {
    /// Stable adapter name recorded on attempts and ledger windows.
    pub provider: String,
    /// Concrete upstream embeddings model id.
    pub model: String,
    /// Declared vector dimensionality every response must satisfy.
    pub dimensions: u16,
    /// Opaque reviewed label documenting any input-prefixing policy.
    pub prompt_version: String,
}

/// One successful embeddings call: one vector per input plus bounded usage.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingResponse {
    /// One vector per requested input, in request order.
    pub vectors: Vec<Vec<f32>>,
    /// Provider-counted input tokens for spend accounting.
    pub input_tokens: u64,
}

/// Narrow provider-neutral embeddings boundary beside the chat seam.
pub trait EmbeddingProvider: Send + Sync {
    /// Returns the stable adapter, model, dimension, and prompt identity.
    fn identity(&self) -> EmbeddingIdentity;

    /// Embeds every input once, returning one vector per input in order.
    fn embed(
        &self,
        inputs: Vec<String>,
    ) -> impl Future<Output = Result<EmbeddingResponse, ProviderFailure>> + Send;
}

/// Bounded usage facts carried by one scripted embedding success.
///
/// Vectors never travel through the script: the fake derives them
/// deterministically from each input so identical inputs stay identical
/// across instances and calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptedEmbeddingSuccess {
    /// Provider-counted input tokens reported for the call.
    pub input_tokens: u64,
}

/// Ordered deterministic embeddings fake used by default tests.
#[derive(Debug, Clone)]
pub struct ScriptedEmbeddingProvider {
    dimensions: u16,
    scripts: Arc<Mutex<VecDeque<Result<ScriptedEmbeddingSuccess, ProviderError>>>>,
    requests: Arc<Mutex<Vec<Vec<String>>>>,
}

impl ScriptedEmbeddingProvider {
    /// Creates a fake with the declared dimensions and ordered outcomes.
    #[must_use]
    pub fn new(
        dimensions: u16,
        scripts: impl IntoIterator<Item = Result<ScriptedEmbeddingSuccess, ProviderError>>,
    ) -> Self {
        Self {
            dimensions,
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns the bounded inputs observed so far, one entry per call.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] when the capture lock was poisoned.
    pub fn requests(&self) -> Result<Vec<Vec<String>>, ProviderError> {
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

    fn record_and_pop(&self, inputs: &[String]) -> Result<EmbeddingResponse, ProviderFailure> {
        let recorded = self
            .requests
            .lock()
            .map(|mut requests| requests.push(inputs.to_vec()))
            .map_err(|_| ProviderError::Internal);
        let success = match recorded {
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
        match success {
            Ok(success) => Ok(EmbeddingResponse {
                vectors: inputs
                    .iter()
                    .map(|input| deterministic_vector(input, self.dimensions))
                    .collect(),
                input_tokens: success.input_tokens,
            }),
            Err(error) => Err(ProviderFailure::from(error)),
        }
    }
}

impl EmbeddingProvider for ScriptedEmbeddingProvider {
    fn identity(&self) -> EmbeddingIdentity {
        EmbeddingIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
            dimensions: self.dimensions,
            prompt_version: "none.v1".to_owned(),
        }
    }

    fn embed(
        &self,
        inputs: Vec<String>,
    ) -> impl Future<Output = Result<EmbeddingResponse, ProviderFailure>> + Send {
        ready(self.record_and_pop(&inputs))
    }
}

/// Derives one deterministic pseudo-vector from the SHA-256 of the input.
///
/// Digest bytes map linearly onto `[-1.0, 1.0]`; when the declared
/// dimensionality exceeds one digest, chained counter blocks extend the
/// stream, so identical inputs always yield identical vectors on every
/// instance and every call.
fn deterministic_vector(input: &str, dimensions: u16) -> Vec<f32> {
    let wanted = usize::from(dimensions);
    let mut vector = Vec::with_capacity(wanted);
    let mut block = 0_u32;
    while vector.len() < wanted {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        hasher.update(block.to_le_bytes());
        for byte in hasher.finalize() {
            if vector.len() == wanted {
                break;
            }
            vector.push((f32::from(byte) / 255.0) * 2.0 - 1.0);
        }
        block = block.saturating_add(1);
    }
    vector
}

/// Wire-mapping failure that carries no request or response content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EmbeddingsWireError {
    /// The success envelope is not the recorded embeddings shape.
    #[error("the embeddings envelope does not match the recorded shape")]
    EnvelopeShape,
    /// The success envelope lacks the bounded token usage facts.
    #[error("the embeddings envelope lacks usable token usage")]
    UsageMissing,
    /// The base URL is not HTTPS and not a loopback host.
    #[error("the embeddings base URL is invalid")]
    BaseUrlInvalid,
    /// The bounded HTTP client could not be built.
    #[error("the embeddings transport could not be initialized")]
    TransportInit,
}

#[derive(Serialize)]
struct EmbeddingsRequestBody<'a> {
    model: &'a str,
    input: &'a [String],
}

/// Serializes one embeddings request into the `OpenAI`-compatible body.
///
/// The mapping is deterministic and preserves input order; no credential or
/// instruction ever enters the body.
///
/// # Errors
///
/// Returns [`EmbeddingsWireError`] when the body cannot be encoded.
pub fn embeddings_request_body(
    model: &str,
    inputs: &[String],
) -> Result<serde_json::Value, EmbeddingsWireError> {
    serde_json::to_value(EmbeddingsRequestBody {
        model,
        input: inputs,
    })
    .map_err(|_| EmbeddingsWireError::EnvelopeShape)
}

#[derive(Deserialize)]
struct EmbeddingsEnvelope {
    data: Vec<EmbeddingDataEntry>,
    usage: EmbeddingsEnvelopeUsage,
}

#[derive(Deserialize)]
struct EmbeddingDataEntry {
    index: usize,
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct EmbeddingsEnvelopeUsage {
    prompt_tokens: u64,
}

/// Parses one recorded-shape embeddings success envelope.
///
/// Unknown envelope fields are tolerated exactly like the chat envelope
/// parser; vector count, ascending index order, and per-vector
/// dimensionality are validated against the request facts so malformed
/// output never becomes a trusted result.
///
/// # Errors
///
/// Returns [`EmbeddingsWireError`] when the envelope deviates from the
/// recorded contract; the error never contains response text.
pub fn parse_embeddings_envelope(
    bytes: &[u8],
    expected_vectors: usize,
    expected_dimensions: u16,
) -> Result<EmbeddingResponse, EmbeddingsWireError> {
    let envelope: EmbeddingsEnvelope =
        serde_json::from_slice(bytes).map_err(|_| EmbeddingsWireError::EnvelopeShape)?;
    if envelope.data.len() != expected_vectors {
        return Err(EmbeddingsWireError::EnvelopeShape);
    }
    let mut vectors = Vec::with_capacity(expected_vectors);
    for (position, entry) in envelope.data.into_iter().enumerate() {
        if entry.index != position || entry.embedding.len() != usize::from(expected_dimensions) {
            return Err(EmbeddingsWireError::EnvelopeShape);
        }
        vectors.push(entry.embedding);
    }
    Ok(EmbeddingResponse {
        vectors,
        input_tokens: envelope.usage.prompt_tokens,
    })
}

/// Adapter settings; the credential reaches only the authorization header.
#[derive(Debug, Clone)]
pub struct EmbeddingsSettings {
    /// Embeddings root, for example `https://api.openai.com/v1`.
    pub base_url: String,
    /// Concrete upstream embeddings model id.
    pub model: String,
    /// Credential that redacts itself everywhere but the header.
    pub credential: ProviderSecret,
    /// Declared vector dimensionality enforced on every response.
    pub dimensions: u16,
    /// Reviewed prompt-version label recorded with every produced vector.
    pub prompt_version: String,
    /// Input bound refused before any transport call.
    pub max_input_characters: usize,
    /// Response byte cap enforced during streaming reads.
    pub response_byte_cap: usize,
    /// Per-try deadline covering request and response.
    pub call_deadline: Duration,
    /// Connection establishment deadline.
    pub connect_timeout: Duration,
    /// Bounded transient-failure retry policy.
    pub retry: RetryPolicy,
}

/// The real `OpenAI`-compatible `/embeddings` adapter.
#[derive(Debug)]
pub struct OpenAiCompatibleEmbeddings {
    endpoint: String,
    model: String,
    credential: ProviderSecret,
    dimensions: u16,
    prompt_version: String,
    max_input_characters: usize,
    response_byte_cap: usize,
    call_deadline: Duration,
    retry: RetryPolicy,
    client: reqwest::Client,
}

impl OpenAiCompatibleEmbeddings {
    /// Validates settings and builds the bounded HTTP client.
    ///
    /// # Errors
    ///
    /// Returns [`EmbeddingsWireError::BaseUrlInvalid`] for a non-HTTPS base
    /// URL whose host is not loopback, and
    /// [`EmbeddingsWireError::TransportInit`] when the client cannot be built.
    pub fn new(settings: EmbeddingsSettings) -> Result<Self, EmbeddingsWireError> {
        let parsed = reqwest::Url::parse(&settings.base_url)
            .map_err(|_| EmbeddingsWireError::BaseUrlInvalid)?;
        let loopback_host = parsed
            .host_str()
            .is_some_and(|host| host == "localhost" || host == "127.0.0.1" || host == "[::1]");
        if parsed.scheme() != "https" && !loopback_host {
            return Err(EmbeddingsWireError::BaseUrlInvalid);
        }
        let endpoint = format!("{}/embeddings", settings.base_url.trim_end_matches('/'));
        let client = reqwest::Client::builder()
            .connect_timeout(settings.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| EmbeddingsWireError::TransportInit)?;
        Ok(Self {
            endpoint,
            model: settings.model,
            credential: settings.credential,
            dimensions: settings.dimensions,
            prompt_version: settings.prompt_version,
            max_input_characters: settings.max_input_characters,
            response_byte_cap: settings.response_byte_cap,
            call_deadline: settings.call_deadline,
            retry: settings.retry,
            client,
        })
    }

    async fn try_once(
        &self,
        body: &serde_json::Value,
        expected_vectors: usize,
    ) -> Result<EmbeddingResponse, ProviderFailure> {
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(self.credential.expose_secret())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(body).map_err(|_| ProviderFailure {
                error: ProviderError::Permanent,
                class: ProviderFailureClass::RequestInvalid,
                http_status: None,
            })?)
            .timeout(self.call_deadline)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) if error.is_timeout() => {
                return Err(ProviderFailure {
                    error: ProviderError::Transient,
                    class: ProviderFailureClass::Timeout,
                    http_status: None,
                });
            }
            Err(_) => return Err(network_failure()),
        };
        let status = response.status();
        if !status.is_success() {
            return Err(classify_error(status.as_u16()));
        }
        let bytes = read_capped(response, self.response_byte_cap).await?;
        parse_embeddings_envelope(&bytes, expected_vectors, self.dimensions).map_err(|wire_error| {
            ProviderFailure {
                error: ProviderError::Permanent,
                class: match wire_error {
                    EmbeddingsWireError::EnvelopeShape
                    | EmbeddingsWireError::UsageMissing
                    | EmbeddingsWireError::BaseUrlInvalid
                    | EmbeddingsWireError::TransportInit => ProviderFailureClass::RequestInvalid,
                },
                http_status: Some(status.as_u16()),
            }
        })
    }
}

impl EmbeddingProvider for OpenAiCompatibleEmbeddings {
    fn identity(&self) -> EmbeddingIdentity {
        EmbeddingIdentity {
            provider: "openai-compatible".to_owned(),
            model: self.model.clone(),
            dimensions: self.dimensions,
            prompt_version: self.prompt_version.clone(),
        }
    }

    async fn embed(&self, inputs: Vec<String>) -> Result<EmbeddingResponse, ProviderFailure> {
        let oversized_input = inputs
            .iter()
            .any(|input| input.chars().count() > self.max_input_characters);
        if oversized_input {
            return Err(ProviderFailure {
                error: ProviderError::Permanent,
                class: ProviderFailureClass::RequestInvalid,
                http_status: None,
            });
        }
        let body = embeddings_request_body(&self.model, &inputs).map_err(|_| ProviderFailure {
            error: ProviderError::Permanent,
            class: ProviderFailureClass::RequestInvalid,
            http_status: None,
        })?;
        let mut try_index = 0_u32;
        loop {
            match self.try_once(&body, inputs.len()).await {
                Ok(response) => return Ok(response),
                Err(failure) => {
                    let retryable = matches!(
                        failure.class,
                        ProviderFailureClass::Network
                            | ProviderFailureClass::RateLimited
                            | ProviderFailureClass::ServerError
                    );
                    if !retryable || try_index + 1 >= self.retry.max_tries {
                        return Err(failure);
                    }
                    let delay = jittered_backoff(&self.retry, try_index);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    try_index += 1;
                }
            }
        }
    }
}

/// Supplied-context characters per estimated input token.
const CHARACTERS_PER_TOKEN: usize = 4;

/// Wraps any embeddings provider with the shared rate limiter and durable
/// budget ledger.
///
/// Composition mirrors [`crate::ControlledProvider`]: admission is spaced by
/// the limiter; spend is refused before the call using a conservative
/// characters-per-token projection with zero output tokens; actual usage is
/// recorded after each successful response keyed by the inner identity's
/// provider string. A failed usage record never discards a valid response;
/// it is logged with bounded fields only.
#[derive(Debug)]
pub struct ControlledEmbeddings<P> {
    inner: P,
    limiter: Arc<RateLimiter>,
    ledger: BudgetLedger,
    limits: BudgetLimits,
    prices: TokenPrices,
}

impl<P> ControlledEmbeddings<P> {
    /// Composes the controls around one inner embeddings provider.
    #[must_use]
    pub fn new(
        inner: P,
        limiter: Arc<RateLimiter>,
        ledger: BudgetLedger,
        limits: BudgetLimits,
        prices: TokenPrices,
    ) -> Self {
        Self {
            inner,
            limiter,
            ledger,
            limits,
            prices,
        }
    }
}

fn projected_input_tokens(inputs: &[String]) -> u64 {
    let characters = inputs.iter().fold(0_usize, |total, input| {
        total.saturating_add(input.chars().count())
    });
    u64::try_from(characters.div_ceil(CHARACTERS_PER_TOKEN)).unwrap_or(u64::MAX)
}

impl<P: EmbeddingProvider> EmbeddingProvider for ControlledEmbeddings<P> {
    fn identity(&self) -> EmbeddingIdentity {
        self.inner.identity()
    }

    async fn embed(&self, inputs: Vec<String>) -> Result<EmbeddingResponse, ProviderFailure> {
        self.limiter.admit().await;
        let identity = self.inner.identity();
        let projection = projected_input_tokens(&inputs);
        if let Err(error) = self
            .ledger
            .ensure_within_budget(
                &identity.provider,
                projection,
                0,
                &self.prices,
                &self.limits,
            )
            .await
        {
            tracing::info!(
                operation = "embeddings_call",
                provider = %identity.provider,
                model = %identity.model,
                outcome = "refused",
                failure_class = ProviderFailureClass::BudgetExhausted.as_str()
            );
            return Err(budget_failure(&error));
        }
        let started = std::time::Instant::now();
        let outcome = self.inner.embed(inputs).await;
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        match &outcome {
            Ok(response) => tracing::info!(
                operation = "embeddings_call",
                provider = %identity.provider,
                model = %identity.model,
                outcome = "accepted",
                vectors = response.vectors.len(),
                input_tokens = response.input_tokens,
                duration_ms
            ),
            Err(failure) => tracing::info!(
                operation = "embeddings_call",
                provider = %identity.provider,
                model = %identity.model,
                outcome = "failed",
                failure_class = failure.class.as_str(),
                http_status = failure.http_status,
                duration_ms
            ),
        }
        if let Ok(response) = &outcome
            && let Some(cost) = self
                .prices
                .estimate_cost_micro_usd(response.input_tokens, 0)
            && let Err(BudgetError::Query(_)) = self
                .ledger
                .record_usage(
                    &identity.provider,
                    &identity.model,
                    ProviderUsage {
                        input_tokens: response.input_tokens,
                        output_tokens: 0,
                    },
                    cost,
                )
                .await
        {
            tracing::warn!(
                operation = "embeddings_call",
                outcome = "usage_record_failed",
                provider = %identity.provider,
                model = %identity.model
            );
        }
        outcome
    }
}

fn budget_failure(error: &BudgetError) -> ProviderFailure {
    match error {
        BudgetError::Exhausted => ProviderFailure {
            error: ProviderError::BudgetExhausted,
            class: ProviderFailureClass::BudgetExhausted,
            http_status: None,
        },
        // The ledger shares the analysis database; treat unavailability as a
        // retryable transport condition rather than losing the run.
        BudgetError::Query(_) => ProviderFailure {
            error: ProviderError::Transient,
            class: ProviderFailureClass::Unclassified,
            http_status: None,
        },
        BudgetError::Overflow => ProviderFailure {
            error: ProviderError::Permanent,
            class: ProviderFailureClass::Unclassified,
            http_status: None,
        },
    }
}

async fn read_capped(
    mut response: reqwest::Response,
    byte_cap: usize,
) -> Result<Vec<u8>, ProviderFailure> {
    let mut bytes = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => return Ok(bytes),
            Err(error) if error.is_timeout() => {
                return Err(ProviderFailure {
                    error: ProviderError::Transient,
                    class: ProviderFailureClass::Timeout,
                    http_status: None,
                });
            }
            Err(_) => return Err(network_failure()),
        };
        if bytes.len().saturating_add(chunk.len()) > byte_cap {
            return Err(ProviderFailure {
                error: ProviderError::Permanent,
                class: ProviderFailureClass::SizeLimit,
                http_status: None,
            });
        }
        bytes.extend_from_slice(&chunk);
    }
}

const fn network_failure() -> ProviderFailure {
    ProviderFailure {
        error: ProviderError::Transient,
        class: ProviderFailureClass::Network,
        http_status: None,
    }
}

fn jittered_backoff(policy: &RetryPolicy, try_index: u32) -> Duration {
    if policy.base_delay_ms == 0 {
        return Duration::ZERO;
    }
    let exponent = try_index.min(16);
    let ceiling = policy
        .base_delay_ms
        .saturating_mul(1_u64 << exponent)
        .min(policy.max_delay_ms.max(policy.base_delay_ms));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::from(elapsed.subsec_nanos()))
        .unwrap_or_default();
    let random = std::collections::hash_map::RandomState::new();
    let mut hasher = std::hash::BuildHasher::build_hasher(&random);
    std::hash::Hasher::write_u32(&mut hasher, try_index);
    std::hash::Hasher::write_u64(&mut hasher, nanos);
    Duration::from_millis(std::hash::Hasher::finish(&hasher) % (ceiling + 1))
}
