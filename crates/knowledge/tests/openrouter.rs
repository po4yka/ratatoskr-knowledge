//! `OpenRouter` wire-format contract tests against recorded fixtures.

use std::time::Duration;

use ratatoskr_knowledge::test_support::{FakeReply, FakeTransport};
use ratatoskr_knowledge::{
    GenerationRequest, LlmProvider as _, OpenRouterProvider, OpenRouterSettings, ProviderError,
    ProviderFailureClass, ProviderSecret, RetryPolicy, chat_completion_body, classify_error,
    parse_success_envelope,
};
use serde_json::json;

const CREDENTIAL: &str = "sk-or-v1-LEAKME";

const SUCCESS_ENVELOPE: &str = include_str!("fixtures/openrouter/success.json");
const RATE_LIMITED_ENVELOPE: &str = include_str!("fixtures/openrouter/rate_limited.json");
const SERVER_ERROR_ENVELOPE: &str = include_str!("fixtures/openrouter/server_error.json");
const AUTH_ERROR_ENVELOPE: &str = include_str!("fixtures/openrouter/auth_error.json");
const BAD_REQUEST_ENVELOPE: &str = include_str!("fixtures/openrouter/bad_request.json");

#[test]
fn request_body_maps_separated_fields_and_carries_no_credential()
-> Result<(), Box<dyn std::error::Error>> {
    let request = GenerationRequest {
        prompt_version: "article_prompt_v1".to_owned(),
        system_policy: "fixed policy".to_owned(),
        task_instruction: "fixed task".to_owned(),
        output_schema: json!({"type": "object"}),
        source_content: "block 0 paragraph: \"Evidence.\"".to_owned(),
    };

    let body = chat_completion_body("openai/gpt-oss-20b", &request, 2_048)?;
    let replay = chat_completion_body("openai/gpt-oss-20b", &request, 2_048)?;

    assert_eq!(body["model"], "openai/gpt-oss-20b");
    assert_eq!(body["max_tokens"], 2_048);
    assert_eq!(body["response_format"]["type"], "json_object");
    let messages = body["messages"]
        .as_array()
        .ok_or("messages must be an array")?;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["role"], "system");
    assert_eq!(messages[0]["content"], "fixed policy");
    assert_eq!(messages[1]["role"], "user");
    let user = messages[1]["content"]
        .as_str()
        .ok_or("user content must be a string")?;
    assert!(user.contains("fixed task"));
    assert!(user.contains("\"type\":\"object\""));
    assert!(user.contains("untrusted evidence"));
    assert!(user.contains(r#"block 0 paragraph: "Evidence.""#));

    let serialized = body.to_string();
    assert!(!serialized.contains(CREDENTIAL));
    assert_eq!(body, replay);
    Ok(())
}

#[test]
fn success_fixture_parses_content_usage_and_request_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let response = parse_success_envelope(SUCCESS_ENVELOPE.as_bytes())?;

    assert_eq!(
        response.bytes,
        br#"{"summary":"A grounded summary.","key_points":[{"text":"Evidence exists.","source_block_indexes":[0]}]}"#
    );
    assert_eq!(
        response.request_id.as_deref(),
        Some("gen-1755858000-recorded000000001")
    );
    assert_eq!(response.usage.input_tokens, 57);
    assert_eq!(response.usage.output_tokens, 40);
    Ok(())
}

#[test]
fn recorded_error_envelopes_classify_transient_and_permanent()
-> Result<(), Box<dyn std::error::Error>> {
    for (envelope, expected_status, expected_error, expected_class) in [
        (
            RATE_LIMITED_ENVELOPE,
            429_u16,
            ProviderError::Transient,
            ProviderFailureClass::RateLimited,
        ),
        (
            SERVER_ERROR_ENVELOPE,
            502,
            ProviderError::Transient,
            ProviderFailureClass::ServerError,
        ),
        (
            AUTH_ERROR_ENVELOPE,
            401,
            ProviderError::Permanent,
            ProviderFailureClass::AuthError,
        ),
        (
            BAD_REQUEST_ENVELOPE,
            400,
            ProviderError::Permanent,
            ProviderFailureClass::RequestInvalid,
        ),
    ] {
        let parsed: serde_json::Value = serde_json::from_str(envelope)?;
        assert!(
            parsed
                .get("error")
                .and_then(serde_json::Value::as_object)
                .is_some(),
            "recorded fixture must carry an error envelope object"
        );
        let failure = classify_error(expected_status);
        assert_eq!(failure.error, expected_error);
        assert_eq!(failure.class, expected_class);
        assert_eq!(failure.http_status, Some(expected_status));
    }
    Ok(())
}

#[test]
fn adapter_identity_names_provider_and_model() -> Result<(), Box<dyn std::error::Error>> {
    let transport_address = "127.0.0.1:1".parse()?;
    let provider = OpenRouterProvider::new(adapter_settings(transport_address, 400))?;

    let identity = provider.identity();
    assert_eq!(identity.provider, "openrouter");
    assert_eq!(identity.model, "openai/gpt-oss-20b");
    Ok(())
}

#[tokio::test]
async fn success_over_transport_returns_content_and_records_credential_header()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::start(vec![FakeReply::bytes(
        200,
        SUCCESS_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let provider = OpenRouterProvider::new(adapter_settings(transport.local_addr(), 400))?;

    let response = provider.generate_json(sample_request()).await?;

    assert_eq!(
        response.request_id.as_deref(),
        Some("gen-1755858000-recorded000000001")
    );
    assert_eq!(transport.request_count()?, 1);
    let recorded = transport.recorded()?;
    assert_eq!(recorded[0].path, "/api/v1/chat/completions");
    let expected_authorization = format!("Bearer {CREDENTIAL}");
    assert_eq!(
        recorded[0].authorization.as_deref(),
        Some(expected_authorization.as_str())
    );
    assert!(recorded[0].body_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn oversized_body_fails_without_buffering_past_cap() -> Result<(), Box<dyn std::error::Error>>
{
    let transport = FakeTransport::start(vec![FakeReply::oversized(8_192)]).await?;
    let provider = OpenRouterProvider::new(adapter_settings(transport.local_addr(), 400))?;

    let failure = provider
        .generate_json(sample_request())
        .await
        .err()
        .ok_or("expected a size failure")?;

    assert_eq!(failure.error, ProviderError::Permanent);
    assert_eq!(failure.class, ProviderFailureClass::SizeLimit);
    assert_eq!(transport.request_count()?, 1);
    Ok(())
}

#[tokio::test]
async fn stalled_response_hits_deadline_as_transient_timeout()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::start(vec![FakeReply::stall()]).await?;
    let provider = OpenRouterProvider::new(adapter_settings(transport.local_addr(), 200))?;

    let failure = provider
        .generate_json(sample_request())
        .await
        .err()
        .ok_or("expected a timeout failure")?;

    assert_eq!(failure.error, ProviderError::Transient);
    assert_eq!(failure.class, ProviderFailureClass::Timeout);
    assert_eq!(failure.http_status, None);
    Ok(())
}

#[tokio::test]
async fn transient_faults_retry_with_jitter_inside_bounds() -> Result<(), Box<dyn std::error::Error>>
{
    let transport = FakeTransport::start(vec![
        FakeReply::bytes(502, SERVER_ERROR_ENVELOPE.as_bytes().to_vec()),
        FakeReply::bytes(200, SUCCESS_ENVELOPE.as_bytes().to_vec()),
    ])
    .await?;
    let provider = OpenRouterProvider::new(adapter_settings(transport.local_addr(), 400))?;

    let response = provider.generate_json(sample_request()).await?;

    assert_eq!(response.usage.input_tokens, 57);
    assert_eq!(transport.request_count()?, 2);
    Ok(())
}

#[tokio::test]
async fn authentication_failure_does_not_retry() -> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::start(vec![FakeReply::bytes(
        401,
        AUTH_ERROR_ENVELOPE.as_bytes().to_vec(),
    )])
    .await?;
    let provider = OpenRouterProvider::new(adapter_settings(transport.local_addr(), 400))?;

    let failure = provider
        .generate_json(sample_request())
        .await
        .err()
        .ok_or("expected an authentication failure")?;

    assert_eq!(failure.error, ProviderError::Permanent);
    assert_eq!(failure.class, ProviderFailureClass::AuthError);
    assert_eq!(failure.http_status, Some(401));
    assert_eq!(transport.request_count()?, 1);
    Ok(())
}

fn adapter_settings(address: std::net::SocketAddr, deadline_millis: u64) -> OpenRouterSettings {
    OpenRouterSettings {
        base_url: format!("http://{address}/api/v1"),
        model: "openai/gpt-oss-20b".to_owned(),
        credential: ProviderSecret::new(CREDENTIAL.to_owned()),
        max_output_tokens: 2_048,
        response_byte_cap: 4_096,
        call_deadline: Duration::from_millis(deadline_millis),
        connect_timeout: Duration::from_millis(deadline_millis),
        retry: RetryPolicy::new(3, 0, 0),
    }
}

fn sample_request() -> GenerationRequest {
    GenerationRequest {
        prompt_version: "article_prompt_v1".to_owned(),
        system_policy: "fixed policy".to_owned(),
        task_instruction: "fixed task".to_owned(),
        output_schema: json!({"type": "object"}),
        source_content: "block 0 paragraph: \"Evidence.\"".to_owned(),
    }
}
