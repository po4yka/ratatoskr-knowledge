//! Article result contract tests.

use ratatoskr_knowledge::{article_analysis_schema, validate_article_json};
use serde_json::{Value, json};

#[test]
fn schema_rejects_unknown_and_out_of_bounds_fields() {
    let valid = json!({
        "summary": "A bounded summary.",
        "key_points": [{"text": "One point.", "source_block_indexes": [0]}]
    });
    let cases = [
        with_member(&valid, "unknown", json!(true)),
        with_summary(&valid, String::new()),
        with_summary(&valid, "s".repeat(2_001)),
        with_key_points(&valid, Vec::new()),
        with_key_points(&valid, vec![valid_point(); 11]),
        with_key_points(
            &valid,
            vec![json!({"text": "", "source_block_indexes": [0]})],
        ),
        with_key_points(
            &valid,
            vec![json!({"text": "p".repeat(501), "source_block_indexes": [0]})],
        ),
        with_key_points(
            &valid,
            vec![json!({"text": "point", "source_block_indexes": []})],
        ),
        with_key_points(
            &valid,
            vec![json!({"text": "point", "source_block_indexes": (0..9).collect::<Vec<_>>()})],
        ),
    ];

    assert!(validate_article_json(&valid).is_ok());
    let accepted = cases
        .iter()
        .filter(|value| validate_article_json(value).is_ok())
        .count();
    assert_eq!(accepted, 0, "invalid values accepted: {accepted}");
}

#[test]
fn article_schema_matches_committed_file() -> Result<(), Box<dyn std::error::Error>> {
    let committed: Value = serde_json::from_str(include_str!(
        "../../../schemas/article-analysis.v1.schema.json"
    ))?;
    assert_eq!(article_analysis_schema()?, committed);
    Ok(())
}

fn valid_point() -> Value {
    json!({"text": "One point.", "source_block_indexes": [0]})
}

fn with_member(value: &Value, name: &str, member: Value) -> Value {
    let mut changed = value.clone();
    if let Some(object) = changed.as_object_mut() {
        object.insert(name.to_owned(), member);
    }
    changed
}

fn with_summary(value: &Value, summary: String) -> Value {
    with_member(value, "summary", Value::String(summary))
}

fn with_key_points(value: &Value, key_points: Vec<Value>) -> Value {
    with_member(value, "key_points", Value::Array(key_points))
}
