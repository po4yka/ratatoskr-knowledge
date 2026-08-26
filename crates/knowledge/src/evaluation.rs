//! Offline, deterministic evaluation fixtures and quality scoring.

use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One committed source and its expected article-analysis qualities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalCase {
    /// Stable fixture identifier.
    pub id: String,
    /// Non-sensitive source evidence used by the recorded response.
    pub source: EvalSource,
    /// The quality envelope expected from a conforming result.
    pub expectations: EvalExpectations,
}

/// Minimal source projection needed for deterministic evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalSource {
    /// Human-readable source title.
    pub title: String,
    /// Provider-visible blocks, indexed by position.
    pub blocks: Vec<String>,
}

/// Explicit quality constraints for one fixture.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalExpectations {
    /// Maximum accepted Unicode character count for the summary.
    pub summary_max_characters: usize,
    /// Minimum accepted number of source-grounded key points.
    pub key_points_min: usize,
    /// Maximum accepted number of source-grounded key points.
    pub key_points_max: usize,
    /// Block indexes that every result must cite at least once.
    pub required_block_indexes: Vec<u32>,
}

/// Safe fixture-loading failure.
#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    /// Fixture files could not be enumerated.
    #[error("could not read evaluation fixture directory")]
    ReadDirectory,
    /// A fixture file could not be read.
    #[error("could not read evaluation fixture {path}")]
    ReadFile {
        /// Display-safe fixture path.
        path: String,
    },
    /// A fixture did not satisfy its strict typed contract.
    #[error("invalid evaluation fixture: {message}")]
    InvalidFixture {
        /// Serde's non-sensitive structural error.
        message: String,
    },
    /// A recorded-response directory or value could not be read.
    #[error("could not read recorded evaluation responses")]
    ReadResponses,
}

/// One independently reported quality check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckOutcome {
    /// Stable check identifier.
    pub name: String,
    /// Whether the response met this requirement.
    pub passed: bool,
    /// Compact deterministic observed-versus-allowed diagnostic.
    pub detail: String,
}

/// All quality checks for one fixture response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaseScore {
    /// Stable fixture identifier.
    pub case_id: String,
    /// Independently evaluated outcomes.
    pub checks: Vec<CheckOutcome>,
}

/// Recorded responses for one provider-and-prompt comparison label.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResponseSet {
    /// Immutable provider/prompt comparison label.
    pub label: String,
    /// Responses keyed by stable fixture identifier.
    pub responses: Vec<(String, serde_json::Value)>,
}

/// One labeled set's case-level evaluation results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SetScore {
    /// Immutable provider/prompt comparison label.
    pub label: String,
    /// Results in stable case order.
    pub cases: Vec<CaseScore>,
}

/// Complete, deterministic evaluation artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvaluationReport {
    /// Results in stable label order.
    pub sets: Vec<SetScore>,
}

/// Loads all JSON case files in stable filename order.
///
/// # Errors
///
/// Returns [`EvaluationError`] when the directory or any strict case fixture is invalid.
pub fn load_cases(root: &Path) -> Result<Vec<EvalCase>, EvaluationError> {
    let mut paths = std::fs::read_dir(root)
        .map_err(|_| EvaluationError::ReadDirectory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    paths.sort();
    let mut cases = paths
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).map_err(|_| EvaluationError::ReadFile {
                path: path.display().to_string(),
            })?;
            load_case_bytes(&bytes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    cases.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(cases)
}

/// Decodes one strict evaluation-case fixture.
///
/// # Errors
///
/// Returns [`EvaluationError`] when the bytes do not obey the typed fixture contract.
pub fn load_case_bytes(bytes: &[u8]) -> Result<EvalCase, EvaluationError> {
    serde_json::from_slice(bytes).map_err(|error| EvaluationError::InvalidFixture {
        message: error.to_string(),
    })
}

/// Scores one recorded JSON response without provider, transport, or credentials.
#[must_use]
pub fn score_case(case: &EvalCase, response: &serde_json::Value) -> CaseScore {
    let article = crate::validate_article_json(response);
    let mut checks = Vec::new();
    let Ok(article) = article else {
        checks.push(CheckOutcome {
            name: "structural_validity".to_owned(),
            passed: false,
            detail: "response does not satisfy the article contract".to_owned(),
        });
        return CaseScore {
            case_id: case.id.clone(),
            checks,
        };
    };
    checks.push(CheckOutcome {
        name: "structural_validity".to_owned(),
        passed: true,
        detail: "response satisfies the article contract".to_owned(),
    });

    let summary_length = article.summary.chars().count();
    checks.push(CheckOutcome {
        name: "summary_length".to_owned(),
        passed: summary_length <= case.expectations.summary_max_characters,
        detail: if summary_length <= case.expectations.summary_max_characters {
            format!(
                "{summary_length} <= {}",
                case.expectations.summary_max_characters
            )
        } else {
            format!(
                "{summary_length} > {}",
                case.expectations.summary_max_characters
            )
        },
    });

    let key_point_count = article.key_points.len();
    checks.push(CheckOutcome {
        name: "key_point_cardinality".to_owned(),
        passed: (case.expectations.key_points_min..=case.expectations.key_points_max)
            .contains(&key_point_count),
        detail: if key_point_count < case.expectations.key_points_min {
            format!("{key_point_count} < {}", case.expectations.key_points_min)
        } else if key_point_count > case.expectations.key_points_max {
            format!("{key_point_count} > {}", case.expectations.key_points_max)
        } else {
            format!(
                "{} <= {key_point_count} <= {}",
                case.expectations.key_points_min, case.expectations.key_points_max
            )
        },
    });

    let mut cited = Vec::new();
    let invalid = article
        .key_points
        .iter()
        .flat_map(|point| point.source_block_indexes.iter().copied())
        .find(|index| {
            usize::try_from(*index).map_or(true, |value| value >= case.source.blocks.len())
        });
    for index in article
        .key_points
        .iter()
        .flat_map(|point| point.source_block_indexes.iter().copied())
    {
        if !cited.contains(&index) {
            cited.push(index);
        }
    }
    checks.push(CheckOutcome {
        name: "grounding".to_owned(),
        passed: invalid.is_none(),
        detail: invalid.map_or_else(
            || "all citations refer to supplied blocks".to_owned(),
            |index| format!("block {index} is absent from supplied evidence"),
        ),
    });

    let missing = case
        .expectations
        .required_block_indexes
        .iter()
        .find(|index| !cited.contains(index));
    checks.push(CheckOutcome {
        name: "required_coverage".to_owned(),
        passed: missing.is_none(),
        detail: missing.map_or_else(
            || "all required blocks are cited".to_owned(),
            |index| format!("required block {index} is not cited"),
        ),
    });

    CaseScore {
        case_id: case.id.clone(),
        checks,
    }
}

/// Scores recorded sets without performing provider calls.
#[must_use]
pub fn score_response_sets(cases: &[EvalCase], sets: &[ResponseSet]) -> EvaluationReport {
    let mut ordered_cases = cases.to_vec();
    ordered_cases.sort_by(|left, right| left.id.cmp(&right.id));
    let mut ordered_sets = sets.to_vec();
    ordered_sets.sort_by(|left, right| left.label.cmp(&right.label));
    let sets = ordered_sets
        .into_iter()
        .map(|set| {
            let mut responses = set.responses;
            responses.sort_by(|left, right| left.0.cmp(&right.0));
            let cases = ordered_cases
                .iter()
                .map(|case| {
                    responses
                        .iter()
                        .find(|(case_id, _)| case_id == &case.id)
                        .map_or_else(
                            || CaseScore {
                                case_id: case.id.clone(),
                                checks: vec![CheckOutcome {
                                    name: "recorded_response".to_owned(),
                                    passed: false,
                                    detail: "no recorded response for fixture".to_owned(),
                                }],
                            },
                            |(_, response)| score_case(case, response),
                        )
                })
                .collect();
            SetScore {
                label: set.label,
                cases,
            }
        })
        .collect();
    EvaluationReport { sets }
}

/// Renders a timestamp-free report artifact suitable for diffing in CI.
#[must_use]
pub fn render_report(report: &EvaluationReport) -> String {
    let mut rendered = String::from("ratatoskr-evaluation-report-v1\n");
    for set in &report.sets {
        let _ignored = writeln!(rendered, "set {}", set.label);
        let mut set_passed = 0_usize;
        let mut set_failed = 0_usize;
        for case in &set.cases {
            let passed = case.checks.iter().filter(|check| check.passed).count();
            let failed = case.checks.len() - passed;
            set_passed += passed;
            set_failed += failed;
            let _ignored = writeln!(
                rendered,
                "case {} passed={passed} failed={failed}",
                case.case_id
            );
            for check in &case.checks {
                let detail = quote_report_value(&check.detail);
                let _ignored = writeln!(
                    rendered,
                    "check {} passed={} detail={detail}",
                    check.name, check.passed
                );
            }
        }
        let _ignored = writeln!(rendered, "totals passed={set_passed} failed={set_failed}");
    }
    rendered
}

/// Renders an arbitrary diagnostic as one escaped JSON string without fallible I/O.
fn quote_report_value(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            character if character.is_control() => {
                let _ignored = write!(quoted, "\\u{:04x}", u32::from(character));
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

/// Runs the committed recorded-response corpus without creating a provider client.
///
/// # Errors
///
/// Returns [`EvaluationError`] when committed fixtures cannot be loaded.
pub fn run_offline(root: &Path) -> Result<EvaluationReport, EvaluationError> {
    let cases = load_cases(&root.join("cases"))?;
    let responses_root = root.join("responses");
    let mut set_paths = std::fs::read_dir(responses_root)
        .map_err(|_| EvaluationError::ReadResponses)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(std::fs::FileType::is_dir)
                .map(|_| entry.path())
        })
        .collect::<Vec<_>>();
    set_paths.sort();
    let mut sets = Vec::with_capacity(set_paths.len());
    for path in set_paths {
        let label = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or(EvaluationError::ReadResponses)?
            .to_owned();
        let mut response_paths = std::fs::read_dir(&path)
            .map_err(|_| EvaluationError::ReadResponses)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .collect::<Vec<_>>();
        response_paths.sort();
        let mut responses = Vec::with_capacity(response_paths.len());
        for response_path in response_paths {
            let case_id = response_path
                .file_stem()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .ok_or(EvaluationError::ReadResponses)?
                .to_owned();
            let bytes = std::fs::read(response_path).map_err(|_| EvaluationError::ReadResponses)?;
            let value = serde_json::from_slice(&bytes).map_err(|error| {
                EvaluationError::InvalidFixture {
                    message: error.to_string(),
                }
            })?;
            responses.push((case_id, value));
        }
        sets.push(ResponseSet { label, responses });
    }
    Ok(score_response_sets(&cases, &sets))
}

/// Runs the fixture corpus shipped with this crate.
///
/// # Errors
///
/// Returns [`EvaluationError`] when committed fixtures cannot be loaded.
pub fn run_committed_evaluation() -> Result<EvaluationReport, EvaluationError> {
    run_offline(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/eval")
            .as_path(),
    )
}
