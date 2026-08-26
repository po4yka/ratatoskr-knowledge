//! Offline evaluation harness integration tests.

use std::path::Path;

use ratatoskr_knowledge::{
    ResponseSet, load_case_bytes, load_cases, render_report, run_committed_evaluation, score_case,
    score_response_sets,
};

#[test]
fn eval_cases_load_and_reject_malformed_fixtures() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/eval/cases");
    let cases = load_cases(&root)?;
    assert!(
        cases.len() >= 2,
        "the committed corpus needs representative cases"
    );
    assert!(cases.windows(2).all(|pair| pair[0].id < pair[1].id));

    let unknown = br#"{
        "id": "unknown",
        "source": {"title": "t", "blocks": ["evidence"]},
        "expectations": {
            "summary_max_characters": 10,
            "key_points_min": 1,
            "key_points_max": 1,
            "required_block_indexes": [0]
        },
        "unexpected": true
    }"#;
    let error = load_case_bytes(unknown).expect_err("unknown fields must be rejected");
    assert!(error.to_string().contains("unknown field"));
    let missing = br#"{"id": "missing", "source": {"title": "t", "blocks": []}}"#;
    let error = load_case_bytes(missing).expect_err("missing expectations must be rejected");
    assert!(error.to_string().contains("expectations"));
    Ok(())
}

#[test]
fn labeled_response_sets_group_side_by_side() -> Result<(), Box<dyn std::error::Error>> {
    let report = run_committed_evaluation()?;
    assert_eq!(report.sets.len(), 2);
    assert_eq!(report.sets[0].label, "candidate-article_prompt_v1");
    assert_eq!(report.sets[1].label, "scripted-article_prompt_v1");
    for set in &report.sets {
        assert_eq!(set.cases.len(), 2);
        assert_eq!(set.cases[0].case_id, "privacy-deletion");
        assert_eq!(set.cases[1].case_id, "release-notes");
    }
    Ok(())
}

#[test]
fn offline_run_scores_recorded_sets_without_network_or_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let report = run_committed_evaluation()?;
    assert!(
        report
            .sets
            .iter()
            .flat_map(|set| &set.cases)
            .flat_map(|case| &case.checks)
            .all(|check| check.passed)
    );
    Ok(())
}

#[test]
fn shuffled_scoring_orders_produce_identical_reports() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/eval/cases");
    let cases = load_cases(&root)?;
    let responses = ResponseSet {
        label: "scripted/article_prompt_v1".to_owned(),
        responses: vec![
            (
                "privacy-deletion".to_owned(),
                serde_json::json!({
                    "summary": "Deletion removes all owned derived artifacts.",
                    "key_points": [
                        {"text": "Owned artifacts are removed.", "source_block_indexes": [0]},
                        {"text": "Receipts record the deletion.", "source_block_indexes": [1]}
                    ]
                }),
            ),
            (
                "release-notes".to_owned(),
                serde_json::json!({
                    "summary": "The release adds offline evaluations.",
                    "key_points": [{"text": "Evaluations are deterministic.", "source_block_indexes": [0]}]
                }),
            ),
        ],
    };
    let mut reversed_cases = cases.clone();
    reversed_cases.reverse();
    let mut reversed_responses = responses.clone();
    reversed_responses.responses.reverse();
    let first = render_report(&score_response_sets(&cases, &[responses]));
    let second = render_report(&score_response_sets(&reversed_cases, &[reversed_responses]));
    assert_eq!(first, second);
    assert!(first.contains("scripted/article_prompt_v1"));
    assert!(first.contains("privacy-deletion"));
    Ok(())
}

#[test]
fn checks_fail_fabricated_out_of_bounds_and_overabundant_responses()
-> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/eval/cases");
    let case = load_cases(&root)?
        .into_iter()
        .find(|case| case.id == "release-notes")
        .ok_or("release-notes fixture missing")?;
    let fabricated = serde_json::json!({
        "summary": "x".repeat(181),
        "key_points": [
            {"text": "A", "source_block_indexes": [9]},
            {"text": "B", "source_block_indexes": [9]},
            {"text": "C", "source_block_indexes": [9]},
            {"text": "D", "source_block_indexes": [9]}
        ]
    });
    let fabricated_score = score_case(&case, &fabricated);
    assert!(
        fabricated_score
            .checks
            .iter()
            .any(|check| !check.passed && check.name == "grounding" && check.detail.contains('9'))
    );
    assert!(fabricated_score.checks.iter().any(|check| !check.passed
        && check.name == "summary_length"
        && check.detail.contains("181 > 180")));
    assert!(fabricated_score.checks.iter().any(|check| !check.passed
        && check.name == "key_point_cardinality"
        && check.detail.contains("4 > 3")));
    assert!(
        fabricated_score
            .checks
            .iter()
            .any(|check| check.name == "required_coverage"),
        "unrelated checks must still run"
    );

    let conforming = serde_json::json!({
        "summary": "The release adds offline evaluations.",
        "key_points": [{"text": "Evaluations are deterministic.", "source_block_indexes": [0]}]
    });
    let conforming_score = score_case(&case, &conforming);
    assert!(!conforming_score.checks.is_empty());
    assert!(conforming_score.checks.iter().all(|check| check.passed));
    Ok(())
}
