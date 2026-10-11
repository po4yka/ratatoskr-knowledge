//! Redelivery delay and progress acknowledgement of the recap worker (XR-021 CONTRACTS.md R2-05).
//!
//! A transiently failing message is redelivered with a delay that doubles with its delivery count
//! and stops growing at one minute, so a persistent fault never runs hotter than once a minute. A
//! message that takes longer than the durable's ack wait stays with the worker because the worker
//! acknowledges progress while it processes it.

use std::future::Future;
use std::time::Duration;

use async_nats::jetstream;

const BASE_DELAY: Duration = Duration::from_secs(2);
const MAX_DELAY: Duration = Duration::from_mins(1);
/// `BASE_DELAY << 5` already exceeds [`MAX_DELAY`], so a larger exponent changes nothing.
const MAX_EXPONENT: i64 = 5;
/// The shortest spacing of progress acknowledgements, for very small ack waits.
const MIN_PROGRESS_PERIOD: Duration = Duration::from_millis(100);

/// Delay before delivery number `delivered` fails and is delivered again:
/// `min(2 s * 2^(delivered-1), 60 s)`. A missing or zero count is the first delivery.
#[must_use]
pub fn redelivery_delay(delivered: i64) -> Duration {
    let exponent = delivered.saturating_sub(1).clamp(0, MAX_EXPONENT);
    let doubling = 1_u32 << u32::try_from(exponent).unwrap_or(0);
    BASE_DELAY.saturating_mul(doubling).min(MAX_DELAY)
}

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
