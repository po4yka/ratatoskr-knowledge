//! Scripted provider boundary tests.

use ratatoskr_knowledge::{
    EmbeddingIdentity, EmbeddingProvider as _, GenerationRequest, LlmProvider as _, ProviderError,
    ProviderFailureClass, ProviderResponse, ProviderUsage, ScriptedEmbeddingProvider,
    ScriptedEmbeddingSuccess, ScriptedProvider,
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

#[tokio::test]
async fn scripted_embedding_provider_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
    let inputs = vec!["chunk one".to_owned(), "chunk two".to_owned()];
    let success = ScriptedEmbeddingSuccess { input_tokens: 21 };
    let first = ScriptedEmbeddingProvider::new(8, [Ok(success), Ok(success)]);
    let second = ScriptedEmbeddingProvider::new(8, [Ok(success), Ok(success)]);

    assert_eq!(
        first.identity(),
        EmbeddingIdentity {
            provider: "scripted_fake".to_owned(),
            model: "fake_default_v1".to_owned(),
            dimensions: 8,
            prompt_version: "none.v1".to_owned(),
        }
    );

    let first_response = first.embed(inputs.clone()).await?;
    assert_eq!(first_response.input_tokens, 21);
    assert_eq!(first_response.vectors.len(), inputs.len());
    assert!(
        first_response
            .vectors
            .iter()
            .all(|vector| vector.len() == 8
                && vector.iter().all(|value| (-1.0..=1.0).contains(value))),
        "every vector must carry the declared dimensions inside [-1.0, 1.0]"
    );

    let second_response = second.embed(inputs.clone()).await?;
    assert_eq!(first_response, second_response);

    let replay = first.embed(inputs.clone()).await?;
    assert_eq!(replay, first_response);
    assert_ne!(
        first_response.vectors[0], first_response.vectors[1],
        "distinct inputs must derive distinct vectors"
    );

    assert_eq!(first.requests()?, [inputs.clone(), inputs]);
    assert_eq!(first.call_count()?, 2);

    let transient: Result<ScriptedEmbeddingSuccess, ProviderError> = Err(ProviderError::Transient);
    let failing = ScriptedEmbeddingProvider::new(8, [transient]);
    let failure = failing
        .embed(vec!["chunk".to_owned()])
        .await
        .err()
        .ok_or("expected a scripted failure")?;
    assert_eq!(failure.error, ProviderError::Transient);
    assert_eq!(failure.class, ProviderFailureClass::Unclassified);
    assert_eq!(failing.call_count()?, 1);
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
