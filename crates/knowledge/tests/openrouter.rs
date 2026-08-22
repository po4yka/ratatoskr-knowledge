//! `OpenRouter` wire-format contract tests against recorded fixtures.

use ratatoskr_knowledge::{
    GenerationRequest, ProviderError, ProviderFailureClass, chat_completion_body, classify_error,
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
