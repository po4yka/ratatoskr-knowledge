//! Authenticated bounded client for GitHub Catalog-owned immutable README bytes.

use std::path::Path;
use std::time::Duration;

use futures_util::StreamExt as _;
use ratatoskr_github_contracts::RepositoryAnalysisRequested;
use ratatoskr_identifiers::{BlobRef, DigestAlgorithm};
use sha2::{Digest as _, Sha256};

use crate::{ProviderSecret, RepositoryReadmeError, RepositoryReadmeResolver};

const RESOLVE_PATH: &str = "internal/v1/repository-readmes/resolve";
const TOKEN_BYTES_MAX: u64 = 4_096;

/// Authenticated, finite GitHub README resolver configuration.
#[derive(Debug, Clone)]
pub struct GithubReadmeSettings {
    /// Internal GitHub Catalog origin.
    pub base_url: reqwest::Url,
    /// Service credential, redacted by its type.
    pub service_token: ProviderSecret,
    /// End-to-end request deadline.
    pub timeout: Duration,
    /// Maximum accepted body size.
    pub response_bytes: usize,
}

/// Production HTTP implementation of [`RepositoryReadmeResolver`].
#[derive(Debug, Clone)]
pub struct GithubRepositoryReadmeResolver {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    token: ProviderSecret,
    response_bytes: usize,
    probe_timeout: Duration,
}

impl GithubRepositoryReadmeResolver {
    /// Builds a client with connect and total deadlines from validated settings.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryReadmeError::InvalidConfiguration`] for an unsafe URL or client
    /// construction failure.
    pub fn new(settings: GithubReadmeSettings) -> Result<Self, RepositoryReadmeError> {
        validate_origin(&settings.base_url)?;
        if settings.timeout.is_zero() || settings.response_bytes == 0 {
            return Err(RepositoryReadmeError::InvalidConfiguration);
        }
        let endpoint = settings
            .base_url
            .join(RESOLVE_PATH)
            .map_err(|_| RepositoryReadmeError::InvalidConfiguration)?;
        let client = reqwest::Client::builder()
            .connect_timeout(settings.timeout.min(Duration::from_secs(5)))
            .timeout(settings.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| RepositoryReadmeError::InvalidConfiguration)?;
        Ok(Self {
            client,
            endpoint,
            token: settings.service_token,
            response_bytes: settings.response_bytes,
            probe_timeout: settings.timeout.min(Duration::from_secs(5)),
        })
    }

    /// Reads one bounded token file without retaining its path or exposing its contents.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryReadmeError::InvalidConfiguration`] for a relative, empty, oversized,
    /// non-regular, or unreadable file.
    pub async fn token_from_file(path: &Path) -> Result<ProviderSecret, RepositoryReadmeError> {
        if !path.is_absolute() {
            return Err(RepositoryReadmeError::InvalidConfiguration);
        }
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|_| RepositoryReadmeError::InvalidConfiguration)?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > TOKEN_BYTES_MAX {
            return Err(RepositoryReadmeError::InvalidConfiguration);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            if metadata.mode() & 0o077 != 0 {
                return Err(RepositoryReadmeError::InvalidConfiguration);
            }
        }
        let value = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| RepositoryReadmeError::InvalidConfiguration)?;
        let value = value.trim();
        if value.is_empty() || value.len() > usize::try_from(TOKEN_BYTES_MAX).unwrap_or(usize::MAX)
        {
            return Err(RepositoryReadmeError::InvalidConfiguration);
        }
        Ok(ProviderSecret::new(value.to_owned()))
    }

    /// Probes the authenticated owner-service role with a deliberately malformed empty request.
    /// A `400` proves the expected route authenticated the credential before body validation
    /// without naming or reading any repository content.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryReadmeError::Unavailable`] when DNS/connect cannot complete inside the
    /// caller's runtime deadline.
    pub async fn probe(&self) -> Result<(), RepositoryReadmeError> {
        let response = tokio::time::timeout(
            self.probe_timeout,
            self.client
                .post(self.endpoint.clone())
                .bearer_auth(self.token.expose_secret())
                .json(&serde_json::json!({}))
                .send(),
        )
        .await
        .map_err(|_| RepositoryReadmeError::Unavailable)?
        .map_err(|_| RepositoryReadmeError::Unavailable)?;
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            Ok(())
        } else if matches!(response.status().as_u16(), 401 | 403) {
            Err(RepositoryReadmeError::Unauthorized)
        } else {
            Err(RepositoryReadmeError::Unavailable)
        }
    }

    async fn resolve(
        &self,
        request: &RepositoryAnalysisRequested,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, RepositoryReadmeError> {
        if reference.owner_service.as_str() != "ratatoskr-github"
            || reference.media_type.as_str() != "text/markdown"
            || !matches!(reference.digest.algorithm, DigestAlgorithm::Sha256)
            || reference.length_bytes > u64::try_from(self.response_bytes).unwrap_or(u64::MAX)
        {
            return Err(RepositoryReadmeError::Integrity);
        }
        let body = serde_json::json!({
            "owner": request.owner,
            "repository_id": request.repository_id,
            "content_ref": reference,
        });
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(self.token.expose_secret())
            .json(&body)
            .send()
            .await
            .map_err(|_| RepositoryReadmeError::Unavailable)?;
        let status = response.status();
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            || status == reqwest::StatusCode::BAD_GATEWAY
            || status == reqwest::StatusCode::GATEWAY_TIMEOUT
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            return Err(RepositoryReadmeError::Unavailable);
        }
        if matches!(status.as_u16(), 401 | 403) {
            return Err(RepositoryReadmeError::Unauthorized);
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(RepositoryReadmeError::Missing);
        }
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            return Err(RepositoryReadmeError::Oversized);
        }
        if !status.is_success() {
            return Err(RepositoryReadmeError::Integrity);
        }
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if media_type != Some(reference.media_type.as_str()) {
            return Err(RepositoryReadmeError::Integrity);
        }
        if response
            .content_length()
            .is_some_and(|length| length != reference.length_bytes)
        {
            return Err(RepositoryReadmeError::Integrity);
        }
        let mut bytes = Vec::with_capacity(
            usize::try_from(reference.length_bytes)
                .unwrap_or(self.response_bytes)
                .min(self.response_bytes),
        );
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| RepositoryReadmeError::Unavailable)?;
            if bytes.len().saturating_add(chunk.len()) > self.response_bytes {
                return Err(RepositoryReadmeError::Oversized);
            }
            bytes.extend_from_slice(&chunk);
        }
        if u64::try_from(bytes.len()).map_err(|_| RepositoryReadmeError::Integrity)?
            != reference.length_bytes
            || format!("{:x}", Sha256::digest(&bytes)) != reference.digest.hex.as_str()
        {
            return Err(RepositoryReadmeError::Integrity);
        }
        Ok(bytes)
    }
}

impl RepositoryReadmeResolver for GithubRepositoryReadmeResolver {
    fn read_readme<'a>(
        &'a self,
        request: &'a RepositoryAnalysisRequested,
        reference: &'a BlobRef,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, RepositoryReadmeError>> + Send + 'a>,
    > {
        Box::pin(self.resolve(request, reference))
    }
}

fn validate_origin(url: &reqwest::Url) -> Result<(), RepositoryReadmeError> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(RepositoryReadmeError::InvalidConfiguration);
    }
    let host = url
        .host_str()
        .ok_or(RepositoryReadmeError::InvalidConfiguration)?;
    let private_http = url.scheme() == "http"
        && (matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
            || host.parse::<std::net::IpAddr>().is_ok_and(|ip| match ip {
                std::net::IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
                std::net::IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local(),
            })
            || (!host.contains('.') && !host.contains(':')));
    if url.scheme() == "https" || private_http {
        Ok(())
    } else {
        Err(RepositoryReadmeError::InvalidConfiguration)
    }
}
