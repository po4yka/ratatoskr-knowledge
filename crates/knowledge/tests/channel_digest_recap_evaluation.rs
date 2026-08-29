//! Offline synthetic channel-recap evaluation gate.

use ratatoskr_knowledge::run_committed_channel_recap_evaluation;

#[test]
fn committed_channel_recap_evaluation_covers_risks_and_metrics()
-> Result<(), Box<dyn std::error::Error>> {
    let report = run_committed_channel_recap_evaluation()?;
    let ids = report
        .cases
        .iter()
        .map(|case| case.case_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        "empty-window",
        "partial-multi-channel",
        "full-multi-channel",
        "edited-revisions",
        "repeated-conflicting",
        "long-post-budget",
        "malformed-manifest",
        "prompt-injection",
    ] {
        assert!(
            ids.contains(required),
            "missing synthetic fixture {required}"
        );
    }
    let metrics = report
        .cases
        .iter()
        .flat_map(|case| &case.checks)
        .map(|check| check.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        "schema",
        "citations",
        "unsupported_claims",
        "coverage",
        "context_digest",
        "budgets",
    ] {
        assert!(
            metrics.contains(required),
            "missing recap metric {required}"
        );
    }
    assert!(
        report
            .cases
            .iter()
            .flat_map(|case| &case.checks)
            .all(|check| check.passed)
    );
    Ok(())
}
