//! `OpenRouter` wire-format contract tests against recorded fixtures.

use ratatoskr_knowledge::{GenerationRequest, chat_completion_body};
use serde_json::json;

const CREDENTIAL: &str = "sk-or-v1-LEAKME";

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
