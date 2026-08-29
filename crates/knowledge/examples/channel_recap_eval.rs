//! Runs the committed synthetic channel-recap evaluation without network or credentials.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = ratatoskr_knowledge::run_committed_channel_recap_evaluation()?;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    for case in report.cases {
        for check in case.checks {
            std::io::Write::write_fmt(
                &mut output,
                format_args!(
                    "case={} metric={} passed={}\n",
                    case.case_id, check.name, check.passed
                ),
            )?;
        }
    }
    Ok(())
}
