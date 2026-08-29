//! Authenticated retrieval and integrity verification for channel-digest manifests.

use ratatoskr_channel_digest_contracts::{
    ChannelDigestRecapFailureCode, DigestWindow, KnowledgeChannelDigestRecapRequested,
};
use ratatoskr_identifiers::{ContentDigest, DigestAlgorithm, WireTimestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::time::Duration;

use crate::channel_digest_recap_store::{
    ChannelRecapRunError, ChannelRecapRunState, ChannelRecapRunStore,
};

/// Redacting credential used only for the digest source service boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct DigestSourceSecret(String);

impl DigestSourceSecret {
    /// Wraps one service credential.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for DigestSourceSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Serialize for DigestSourceSecret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("[redacted]")
    }
}

/// Finite, loopback-only digest source client policy.
#[derive(Debug, Clone)]
pub struct DigestSourceClientSettings {
    /// Fixed loopback HTTP origin, including an optional deployment prefix.
    pub base_url: String,
    /// Service-to-service authorization secret.
    pub service_secret: DigestSourceSecret,
    /// DNS/TCP connection cap.
    pub connect_timeout: Duration,
    /// Absolute request and response-body deadline.
    pub request_deadline: Duration,
    /// Maximum decoded response bytes.
    pub response_byte_cap: usize,
    /// Persisted delay before a transient manifest retry may run.
    pub retry_delay: Duration,
}

/// Authority claims for one immutable manifest read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestManifestRequest {
    /// Internal Ratatoskr owner reference.
    pub owner: String,
    /// Digest run whose immutable source is being resolved.
    pub digest_run_id: uuid::Uuid,
    /// Opaque manifest reference minted by the digest service.
    pub manifest_ref: String,
    /// Expected canonical SHA-256 digest in lowercase hexadecimal.
    pub manifest_digest_hex: String,
}

/// Safe, content-free digest-source transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DigestSourceClientError {
    /// Client policy could not be constructed safely.
    #[error("the digest source client configuration is invalid")]
    InvalidConfiguration,
    /// One authority claim is malformed and cannot be represented safely.
    #[error("the digest source request is invalid")]
    InvalidRequest,
    /// The source response exceeded the configured decoded-byte cap.
    #[error("the digest source response is too large")]
    ResponseTooLarge,
    /// The operation-level request deadline was exhausted.
    #[error("the digest source request timed out")]
    Timeout,
    /// The source could not be retrieved through the configured boundary.
    #[error("the digest source is unavailable")]
    Unavailable,
}

/// Authenticated, bounded client for immutable digest manifests.
#[derive(Debug)]
pub struct DigestSourceClient {
    base_url: reqwest::Url,
    service_secret: DigestSourceSecret,
    request_deadline: Duration,
    response_byte_cap: usize,
    retry_delay: Duration,
    client: reqwest::Client,
}

impl DigestSourceClient {
    /// Validates policy and creates the source client.
    ///
    /// # Errors
    ///
    /// Returns [`DigestSourceClientError::InvalidConfiguration`] when policy is unsafe.
    pub fn new(settings: DigestSourceClientSettings) -> Result<Self, DigestSourceClientError> {
        let mut base_url = reqwest::Url::parse(&settings.base_url)
            .map_err(|_| DigestSourceClientError::InvalidConfiguration)?;
        let loopback = base_url
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1"));
        if base_url.scheme() != "http"
            || !loopback
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || settings.connect_timeout.is_zero()
            || settings.request_deadline.is_zero()
            || settings.response_byte_cap == 0
        {
            return Err(DigestSourceClientError::InvalidConfiguration);
        }
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(settings.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| DigestSourceClientError::InvalidConfiguration)?;
        Ok(Self {
            base_url,
            service_secret: settings.service_secret,
            request_deadline: settings.request_deadline,
            response_byte_cap: settings.response_byte_cap,
            retry_delay: settings.retry_delay,
            client,
        })
    }

    /// Retrieves the exact immutable source bytes for one authorized request.
    ///
    /// # Errors
    ///
    /// Returns a safe source failure without endpoint, credential, or source content.
    pub async fn fetch_manifest(
        &self,
        request: &DigestManifestRequest,
    ) -> Result<Vec<u8>, DigestSourceClientError> {
        tokio::time::timeout(self.request_deadline, self.fetch_manifest_once(request))
            .await
            .map_err(|_| DigestSourceClientError::Timeout)?
    }

    /// Checks the authenticated loopback source readiness endpoint.
    ///
    /// # Errors
    ///
    /// Returns a safe unavailable or timeout class without exposing the endpoint or credential.
    pub async fn probe(&self) -> Result<(), DigestSourceClientError> {
        tokio::time::timeout(self.request_deadline, async {
            let endpoint = self
                .base_url
                .join("ready")
                .map_err(|_| DigestSourceClientError::InvalidConfiguration)?;
            let response = self
                .client
                .get(endpoint)
                .bearer_auth(self.service_secret.expose_secret())
                .send()
                .await
                .map_err(|_| DigestSourceClientError::Unavailable)?;
            if response.status().is_success() {
                Ok(())
            } else {
                Err(DigestSourceClientError::Unavailable)
            }
        })
        .await
        .map_err(|_| DigestSourceClientError::Timeout)?
    }

    async fn fetch_manifest_once(
        &self,
        request: &DigestManifestRequest,
    ) -> Result<Vec<u8>, DigestSourceClientError> {
        let manifest_id = request
            .manifest_ref
            .strip_prefix("channel-digest-manifest:")
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .ok_or(DigestSourceClientError::InvalidRequest)?;
        if request.owner.is_empty()
            || request.manifest_digest_hex.len() != 64
            || !request
                .manifest_digest_hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DigestSourceClientError::InvalidRequest);
        }
        let endpoint = self
            .base_url
            .join(&format!("v1/channel-digest/manifests/{manifest_id}"))
            .map_err(|_| DigestSourceClientError::InvalidConfiguration)?;
        let response = self
            .client
            .get(endpoint)
            .bearer_auth(self.service_secret.expose_secret())
            .header("x-ratatoskr-owner", request.owner.as_str())
            .header(
                "x-ratatoskr-digest-run-id",
                request.digest_run_id.to_string(),
            )
            .header("x-ratatoskr-manifest-ref", request.manifest_ref.as_str())
            .header(
                "x-ratatoskr-manifest-digest",
                request.manifest_digest_hex.as_str(),
            )
            .send()
            .await
            .map_err(|_| DigestSourceClientError::Unavailable)?;
        if !response.status().is_success() {
            return Err(DigestSourceClientError::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.response_byte_cap as u64)
        {
            return Err(DigestSourceClientError::ResponseTooLarge);
        }
        read_digest_source_body(response, self.response_byte_cap).await
    }
}

async fn read_digest_source_body(
    mut response: reqwest::Response,
    byte_cap: usize,
) -> Result<Vec<u8>, DigestSourceClientError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| DigestSourceClientError::Unavailable)?
    {
        if bytes.len().saturating_add(chunk.len()) > byte_cap {
            return Err(DigestSourceClientError::ResponseTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Version marker for the canonical digest-source manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DigestManifestSchema {
    /// First and only development manifest schema.
    #[serde(rename = "channel_digest_manifest.v1")]
    ChannelDigestManifestV1,
}

/// One immutable selected public-channel post revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigestManifestSource {
    /// Stable immutable revision reference.
    pub revision_ref: String,
    /// Stable owner-independent public-channel reference.
    pub channel_ref: String,
    /// Bounded channel label for source attribution.
    pub channel_label: String,
    /// Stable provider message identity within the channel.
    pub message_id: String,
    /// Provider-authored canonical UTC publication instant.
    pub published_at: WireTimestamp,
    /// Complete normalized post revision bytes interpreted as UTF-8.
    pub content: String,
    /// SHA-256 of the exact normalized UTF-8 content bytes.
    pub content_digest: ContentDigest,
    /// Canonical bounded public Telegram link when one exists.
    pub public_link: Option<String>,
    /// Monotonic revision ordinal for the stable provider message.
    pub revision: u32,
}

/// Canonical immutable manifest resolved from the digest service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigestManifest {
    /// Manifest encoding marker.
    pub schema: DigestManifestSchema,
    /// Owner-authorized opaque manifest reference.
    pub manifest_ref: String,
    /// Internal Ratatoskr owner reference.
    pub owner: String,
    /// Digest run that selected the sources.
    pub digest_run_id: uuid::Uuid,
    /// Exact closed-open publication window.
    pub window: DigestWindow,
    /// Selected immutable source revisions.
    pub sources: Vec<DigestManifestSource>,
}

/// Parsed manifest whose request linkage and immutable bytes were verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDigestManifest {
    /// Verified exact SHA-256 of canonical response bytes.
    pub digest_hex: String,
    /// Closed decoded manifest.
    pub manifest: DigestManifest,
}

/// Safe manifest integrity failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DigestManifestError {
    /// Bytes do not decode as the closed canonical manifest schema.
    #[error("the digest manifest encoding is invalid")]
    InvalidEncoding,
    /// Canonical bytes or immutable source digests do not match.
    #[error("the digest manifest integrity check failed")]
    Integrity,
    /// Owner, run, reference, window, or count linkage differs from the request.
    #[error("the digest manifest linkage is invalid")]
    Linkage,
}

/// Durable outcome of one bounded manifest retrieval attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DigestManifestAttemptOutcome {
    /// Verified immutable bytes were committed and the run may prepare context.
    Accepted,
    /// One transient attempt was recorded and restart-safe retry remains.
    RetryScheduled,
    /// Bounded attempts were exhausted and one safe terminal fact was committed.
    Failed,
}

/// Safe manifest-attempt orchestration failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DigestManifestAttemptError {
    /// Digest-source transport failed before durable retry handling was available.
    #[error(transparent)]
    Client(#[from] DigestSourceClientError),
    /// Retrieved bytes did not satisfy immutable manifest integrity.
    #[error(transparent)]
    Manifest(#[from] DigestManifestError),
    /// Durable run state could not converge.
    #[error(transparent)]
    Run(#[from] ChannelRecapRunError),
}

/// Executes one restart-safe manifest retrieval attempt.
///
/// # Errors
///
/// Returns only closed transport, integrity, or durable-state error classes.
pub async fn attempt_digest_manifest(
    client: &DigestSourceClient,
    runs: &ChannelRecapRunStore<'_>,
    recap_run_id: uuid::Uuid,
    request: &KnowledgeChannelDigestRecapRequested,
) -> Result<DigestManifestAttemptOutcome, DigestManifestAttemptError> {
    let status = runs.manifest_attempt_status(recap_run_id).await?;
    if status.state == ChannelRecapRunState::Failed {
        return Ok(DigestManifestAttemptOutcome::Failed);
    }
    if !matches!(
        status.state,
        ChannelRecapRunState::Received | ChannelRecapRunState::ManifestRetry
    ) {
        return Ok(DigestManifestAttemptOutcome::Accepted);
    }
    if !status.retry_ready {
        return Ok(DigestManifestAttemptOutcome::RetryScheduled);
    }
    let authority = DigestManifestRequest {
        owner: request.owner.to_string(),
        digest_run_id: request.digest_run_id.as_uuid(),
        manifest_ref: request.manifest_ref.to_string(),
        manifest_digest_hex: request.manifest_digest.hex.to_string(),
    };
    match client.fetch_manifest(&authority).await {
        Ok(bytes) => {
            if let Ok(verified) = verify_digest_manifest(request, &bytes) {
                runs.accept_verified_manifest(recap_run_id, status.state, &verified)
                    .await?;
                Ok(DigestManifestAttemptOutcome::Accepted)
            } else {
                runs.settle_manifest_failure(
                    recap_run_id,
                    status.state,
                    request,
                    ChannelDigestRecapFailureCode::ManifestIntegrity,
                )
                .await?;
                Ok(DigestManifestAttemptOutcome::Failed)
            }
        }
        Err(DigestSourceClientError::Unavailable | DigestSourceClientError::Timeout)
            if status.attempt_count < 1 =>
        {
            runs.schedule_manifest_retry(
                recap_run_id,
                status.state,
                status.attempt_count,
                client.retry_delay,
            )
            .await?;
            Ok(DigestManifestAttemptOutcome::RetryScheduled)
        }
        Err(
            DigestSourceClientError::Unavailable
            | DigestSourceClientError::Timeout
            | DigestSourceClientError::InvalidConfiguration
            | DigestSourceClientError::InvalidRequest,
        ) => {
            runs.settle_manifest_failure(
                recap_run_id,
                status.state,
                request,
                ChannelDigestRecapFailureCode::ManifestUnavailable,
            )
            .await?;
            Ok(DigestManifestAttemptOutcome::Failed)
        }
        Err(DigestSourceClientError::ResponseTooLarge) => {
            runs.settle_manifest_failure(
                recap_run_id,
                status.state,
                request,
                ChannelDigestRecapFailureCode::ManifestIntegrity,
            )
            .await?;
            Ok(DigestManifestAttemptOutcome::Failed)
        }
    }
}

/// Parses canonical bytes as the closed digest manifest shape.
///
/// # Errors
///
/// Returns a safe encoding error when the closed manifest cannot be decoded.
pub fn verify_digest_manifest(
    request: &KnowledgeChannelDigestRecapRequested,
    bytes: &[u8],
) -> Result<VerifiedDigestManifest, DigestManifestError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| DigestManifestError::InvalidEncoding)?;
    let canonical = serde_json::to_vec(&value).map_err(|_| DigestManifestError::InvalidEncoding)?;
    if canonical != bytes {
        return Err(DigestManifestError::Integrity);
    }
    let digest_hex = format!("{:x}", Sha256::digest(bytes));
    if !matches!(request.manifest_digest.algorithm, DigestAlgorithm::Sha256)
        || request.manifest_digest.hex.as_str() != digest_hex
    {
        return Err(DigestManifestError::Integrity);
    }
    let manifest: DigestManifest =
        serde_json::from_value(value).map_err(|_| DigestManifestError::InvalidEncoding)?;
    if manifest.owner != request.owner.to_string()
        || manifest.digest_run_id != request.digest_run_id.as_uuid()
        || manifest.manifest_ref != request.manifest_ref.to_string()
        || manifest.window != request.window
        || manifest.sources.len() != usize::from(request.source_count.get())
    {
        return Err(DigestManifestError::Linkage);
    }
    validate_manifest_sources(request, &manifest)?;
    Ok(VerifiedDigestManifest {
        digest_hex,
        manifest,
    })
}

fn validate_manifest_sources(
    request: &KnowledgeChannelDigestRecapRequested,
    manifest: &DigestManifest,
) -> Result<(), DigestManifestError> {
    let mut revision_refs = std::collections::BTreeSet::new();
    let mut provider_revisions = std::collections::BTreeSet::new();
    let mut channels = std::collections::BTreeSet::new();
    for source in &manifest.sources {
        let identity = (
            source.channel_ref.as_str(),
            source.message_id.as_str(),
            source.revision,
        );
        channels.insert(source.channel_ref.as_str());
        if !revision_refs.insert(source.revision_ref.as_str())
            || !provider_revisions.insert(identity)
            || !valid_manifest_source(source, request.window)
        {
            return Err(DigestManifestError::Integrity);
        }
    }
    if channels.len() != usize::from(request.channel_count.get()) {
        return Err(DigestManifestError::Linkage);
    }
    Ok(())
}

fn valid_manifest_source(source: &DigestManifestSource, window: DigestWindow) -> bool {
    let content_digest = format!("{:x}", Sha256::digest(source.content.as_bytes()));
    let valid_link = source.public_link.as_deref().is_none_or(|link| {
        link.len() <= 512
            && link.starts_with("https://t.me/")
            && !link.contains(['?', '#'])
            && reqwest::Url::parse(link).is_ok()
    });
    source.revision_ref.starts_with("channel-post-revision:")
        && source.revision_ref.len() <= 160
        && source.channel_ref.starts_with("telegram-public-channel:")
        && source.channel_ref.len() <= 160
        && !source.message_id.is_empty()
        && source.message_id.len() <= 32
        && source.message_id.bytes().all(|byte| byte.is_ascii_digit())
        && (1..=80).contains(&source.channel_label.chars().count())
        && !source.content.is_empty()
        && source.content.len() <= 16_384
        && matches!(source.content_digest.algorithm, DigestAlgorithm::Sha256)
        && source.content_digest.hex.as_str() == content_digest
        && source.revision > 0
        && source.published_at >= window.start_at
        && source.published_at < window.end_at
        && valid_link
}
