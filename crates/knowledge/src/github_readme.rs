//! Authorized byte resolver for GitHub-owned README references (XR-021 CONTRACTS.md S09).
//!
//! The resolver is the only place Knowledge reads README bytes. It calls GitHub's documented
//! internal blob route over loopback with the service bearer, never follows a redirect, caps the
//! body at the reference's declared length, and refuses bytes that are not the referenced ones.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use ratatoskr_identifiers::{BlobRef, DigestAlgorithm};
use sha2::{Digest as _, Sha256};

use crate::{ProviderSecret, RepositoryReadmeError, RepositoryReadmeResolver};

const README_OWNER: &str = "ratatoskr-github";
const README_MEDIA_TYPE: &str = "text/markdown";

/// Finite, loopback-only README client policy.
#[derive(Debug, Clone)]
pub struct GithubReadmeClientSettings {
    /// Fixed loopback HTTP origin of the GitHub API listener, with an optional prefix.
    pub base_url: String,
    /// Service-to-service authorization secret.
    pub service_secret: ProviderSecret,
    /// DNS/TCP connection cap.
    pub connect_timeout: Duration,
    /// Absolute request and response-body deadline.
    pub request_deadline: Duration,
}

/// The client policy cannot be constructed safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the GitHub README client configuration is invalid")]
pub struct GithubReadmeClientError;

/// Authenticated, bounded client for GitHub README bytes.
#[derive(Debug)]
pub struct GithubReadmeClient {
    base_url: reqwest::Url,
    service_secret: ProviderSecret,
    request_deadline: Duration,
    client: reqwest::Client,
}

impl GithubReadmeClient {
    /// Validates the policy and creates the client.
    ///
    /// # Errors
    ///
    /// Returns [`GithubReadmeClientError`] when the policy is unsafe: a base URL that is not
    /// loopback plain HTTP without credentials, query or fragment, or a zero deadline.
    pub fn new(settings: GithubReadmeClientSettings) -> Result<Self, GithubReadmeClientError> {
        let mut base_url =
            reqwest::Url::parse(&settings.base_url).map_err(|_| GithubReadmeClientError)?;
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
        {
            return Err(GithubReadmeClientError);
        }
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(settings.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| GithubReadmeClientError)?;
        Ok(Self {
            base_url,
            service_secret: settings.service_secret,
            request_deadline: settings.request_deadline,
            client,
        })
    }

    async fn fetch(&self, reference: &BlobRef) -> Result<Vec<u8>, RepositoryReadmeError> {
        if reference.owner_service.as_str() != README_OWNER
            || reference.media_type.as_str() != README_MEDIA_TYPE
            || !matches!(reference.digest.algorithm, DigestAlgorithm::Sha256)
        {
            return Err(RepositoryReadmeError::Integrity);
        }
        let expected_length = usize::try_from(reference.length_bytes)
            .map_err(|_| RepositoryReadmeError::Integrity)?;
        let endpoint = self
            .base_url
            .join(&format!(
                "internal/v1/readme-blobs/{}",
                reference.digest.hex.as_str()
            ))
            .map_err(|_| RepositoryReadmeError::Unavailable)?;
        let mut response = self
            .client
            .get(endpoint)
            .bearer_auth(self.service_secret.expose_secret())
            .send()
            .await
            .map_err(|_| RepositoryReadmeError::Unavailable)?;
        if !response.status().is_success() {
            return Err(RepositoryReadmeError::Unavailable);
        }
        let mut bytes = Vec::with_capacity(expected_length.min(1_048_576));
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| RepositoryReadmeError::Unavailable)?
        {
            if bytes.len().saturating_add(chunk.len()) > expected_length {
                return Err(RepositoryReadmeError::Integrity);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != expected_length
            || format!("{:x}", Sha256::digest(&bytes)) != reference.digest.hex.as_str()
        {
            return Err(RepositoryReadmeError::Integrity);
        }
        Ok(bytes)
    }
}

impl RepositoryReadmeResolver for GithubReadmeClient {
    fn read_readme<'a>(
        &'a self,
        reference: &'a BlobRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RepositoryReadmeError>> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::timeout(self.request_deadline, self.fetch(reference))
                .await
                .map_err(|_| RepositoryReadmeError::Unavailable)?
        })
    }
}
