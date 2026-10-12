//! Progress acknowledgement of the recap worker (XR-021 CONTRACTS.md R2-05).
//!
//! A message that takes longer than the durable's ack wait stays with the worker because the worker
//! acknowledges progress while it processes it. The redelivery delay of a failing message is the
//! shared [`crate::RedeliverySchedule`].

use std::future::Future;
use std::time::Duration;

use async_nats::jetstream;

/// The shortest spacing of progress acknowledgements, for very small ack waits.
const MIN_PROGRESS_PERIOD: Duration = Duration::from_millis(100);

/// Runs `work` to completion while acknowledging progress on `message` every third of `ack_wait`.
///
/// `work` is polled to completion and never dropped; only the progress timer is cancelled when it
/// finishes, so a write in flight is never cut off. A progress acknowledgement that fails is
/// logged and ignored: the final acknowledgement reports a lost message.
pub async fn run_with_progress<T>(
    message: &jetstream::Message,
    ack_wait: Duration,
    work: impl Future<Output = T>,
) -> T {
    let period = (ack_wait / 3).max(MIN_PROGRESS_PERIOD);
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(work);
    loop {
        // cancel-safe: Interval::tick keeps no partial state, and `work` is polled by reference.
        tokio::select! {
            output = &mut work => return output,
            _ = ticker.tick() => {
                if message.ack_with(jetstream::AckKind::Progress).await.is_err() {
                    tracing::warn!(class = "progress_ack_failed", "channel recap progress acknowledgement failed");
                }
            }
        }
    }
}
