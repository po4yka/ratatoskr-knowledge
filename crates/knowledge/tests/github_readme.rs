//! The README byte client: a bearer-authenticated, loopback-only, digest-verifying resolver
//! (XR-021 CONTRACTS.md S09).

use std::time::Duration;

use ratatoskr_identifiers::{
    BlobOwner, BlobRef, ContentDigest, DigestAlgorithm, DigestHex, MediaType,
};
use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport};
use ratatoskr_knowledge::{
    GithubReadmeClient, GithubReadmeClientSettings, ProviderSecret, RepositoryReadmeError,
    RepositoryReadmeResolver,
};
use sha2::{Digest as _, Sha256};

const SECRET: &str = "readme-service-LEAKME";
const README: &[u8] = b"# Project\nA Rust library.\n";

fn reference(bytes: &[u8]) -> Result<BlobRef, Box<dyn std::error::Error>> {
    Ok(BlobRef {
        owner_service: BlobOwner::parse("ratatoskr-github")?,
        digest: ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: DigestHex::parse(&format!("{:x}", Sha256::digest(bytes)))?,
        },
        media_type: MediaType::parse("text/markdown")?,
        length_bytes: u64::try_from(bytes.len())?,
    })
}

fn settings(base_url: String, deadline: Duration) -> GithubReadmeClientSettings {
    GithubReadmeClientSettings {
        base_url,
        service_secret: ProviderSecret::new(SECRET.to_owned()),
        connect_timeout: Duration::from_millis(200),
        request_deadline: deadline,
    }
}

async fn client_for(
    replies: Vec<FakeReply>,
    deadline: Duration,
) -> Result<(GithubReadmeClient, FakeTransport), Box<dyn std::error::Error>> {
    let transport = FakeTransport::start(replies).await?;
    let client = GithubReadmeClient::new(settings(
        format!("http://{}/gh", transport.local_addr()),
        deadline,
    ))?;
    Ok((client, transport))
}

#[tokio::test]
async fn present_revision_returns_the_bytes_when_digest_and_length_match()
-> Result<(), Box<dyn std::error::Error>> {
    let (client, transport) = client_for(
        vec![FakeReply::bytes(200, README.to_vec())],
        Duration::from_secs(1),
    )
    .await?;
    let reference = reference(README)?;

    assert_eq!(client.read_readme(&reference).await?, README);

    let recorded = transport.recorded()?;
    let request = recorded.first().ok_or("no request was captured")?;
    assert_eq!(
        request.path,
        format!("/gh/internal/v1/readme-blobs/{}", reference.digest.hex)
    );
    assert_eq!(
        request.authorization.as_deref(),
        Some(format!("Bearer {SECRET}").as_str())
    );
    assert!(
        !format!("{client:?}").contains("LEAKME"),
        "the bearer must not reach diagnostics"
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_sources_map_to_a_retryable_class() -> Result<(), Box<dyn std::error::Error>> {
    let reference = reference(README)?;
    for reply in [
        FakeReply::bytes(404, Vec::new()),
        FakeReply::bytes(401, Vec::new()),
        FakeReply::bytes(503, Vec::new()),
    ] {
        let (client, _transport) = client_for(vec![reply], Duration::from_secs(1)).await?;
        assert!(matches!(
            client.read_readme(&reference).await,
            Err(RepositoryReadmeError::Unavailable)
        ));
    }
    let (stalled, _transport) =
        client_for(vec![FakeReply::stall()], Duration::from_millis(50)).await?;
    assert!(matches!(
        stalled.read_readme(&reference).await,
        Err(RepositoryReadmeError::Unavailable)
    ));
    Ok(())
}

#[tokio::test]
async fn bytes_that_do_not_match_the_reference_are_an_integrity_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let reference = reference(README)?;
    let same_length_other_bytes = vec![b'x'; README.len()];
    let longer = [README, b"extra"].concat();
    for body in [same_length_other_bytes, longer, Vec::new()] {
        let (client, _transport) =
            client_for(vec![FakeReply::bytes(200, body)], Duration::from_secs(1)).await?;
        assert!(matches!(
            client.read_readme(&reference).await,
            Err(RepositoryReadmeError::Integrity)
        ));
    }
    let mut foreign = reference.clone();
    foreign.owner_service = BlobOwner::parse("ratatoskr-extractor")?;
    let (client, transport) = client_for(
        vec![FakeReply::bytes(200, README.to_vec())],
        Duration::from_secs(1),
    )
    .await?;
    assert!(matches!(
        client.read_readme(&foreign).await,
        Err(RepositoryReadmeError::Integrity)
    ));
    assert_eq!(
        transport.request_count()?,
        0,
        "a reference owned by another service is never requested"
    );
    Ok(())
}

#[test]
fn only_a_loopback_plain_text_origin_is_accepted() {
    for unsafe_origin in [
        "https://127.0.0.1:8092/",
        "http://github.example.internal:8092/",
        "http://user:pass@127.0.0.1:8092/",
        "http://127.0.0.1:8092/?token=1",
        "ftp://127.0.0.1/",
        "not a url",
    ] {
        assert!(
            GithubReadmeClient::new(settings(unsafe_origin.to_owned(), Duration::from_secs(1)))
                .is_err(),
            "{unsafe_origin} must be refused"
        );
    }
    assert!(
        GithubReadmeClient::new(settings(
            "http://127.0.0.1:8092/".to_owned(),
            Duration::from_secs(1)
        ))
        .is_ok()
    );
    assert!(
        GithubReadmeClient::new(settings(
            "http://127.0.0.1:8092/".to_owned(),
            Duration::ZERO
        ))
        .is_err(),
        "a zero deadline is not a policy"
    );
}
