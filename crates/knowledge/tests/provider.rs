//! Scripted provider boundary tests.

use ratatoskr_knowledge::{
    GenerationRequest, LlmProvider as _, ProviderResponse, ProviderUsage, ScriptedProvider,
};
use serde_json::json;

#[tokio::test]
async fn fake_provider_consumes_scripts_and_records_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = ScriptedProvider::new([
        Ok(response(br#"{"summary":"first"}"#, "request-1")),
        Ok(response(br#"{"summary":"second"}"#, "request-2")),
    ]);
    let first_request = request("first source");
    let second_request = request("second source");

    let first = provider.generate_json(first_request.clone()).await?;
    let second = provider.generate_json(second_request.clone()).await?;

    assert_eq!(first.request_id.as_deref(), Some("request-1"));
    assert_eq!(second.request_id.as_deref(), Some("request-2"));
    assert_eq!(provider.requests()?, [first_request, second_request]);
    Ok(())
}

fn response(bytes: &[u8], request_id: &str) -> ProviderResponse {
    ProviderResponse {
        bytes: bytes.to_vec(),
        request_id: Some(request_id.to_owned()),
        usage: ProviderUsage {
            input_tokens: 10,
            output_tokens: 5,
        },
    }
}

fn request(source_content: &str) -> GenerationRequest {
    GenerationRequest {
        prompt_version: "article_prompt_v1".to_owned(),
        system_policy: "fixed policy".to_owned(),
        task_instruction: "fixed task".to_owned(),
        output_schema: json!({"type": "object"}),
        source_content: source_content.to_owned(),
    }
}
