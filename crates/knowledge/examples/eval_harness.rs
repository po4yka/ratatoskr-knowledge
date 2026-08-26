//! Runs the committed offline evaluation corpus and prints its stable report.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = ratatoskr_knowledge::run_committed_evaluation()?;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    match std::io::Write::write_all(
        &mut output,
        ratatoskr_knowledge::render_report(&report).as_bytes(),
    ) {
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        outcome => Ok(outcome?),
    }
}
