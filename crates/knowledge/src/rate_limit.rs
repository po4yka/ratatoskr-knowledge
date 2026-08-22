//! Fixed-spacing admission control shared by every real provider call.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Admits at most one provider request per configured interval.
///
/// The limiter is shared across tasks through `Arc`; each admission reserves
/// the next slot before waiting, so concurrent callers keep their order.
#[derive(Debug)]
pub struct RateLimiter {
    interval: Duration,
    next_admission: Mutex<Option<Instant>>,
}

impl RateLimiter {
    /// Creates a limiter with one admission per interval; zero admits freely.
    #[must_use]
    pub const fn new(interval: Duration) -> Self {
        Self {
            interval,
            next_admission: Mutex::new(None),
        }
    }

    /// Waits until the caller's reserved slot begins.
    ///
    /// Cancellation safe: dropping the future leaves the reservation consumed
    /// and never blocks other callers.
    pub async fn admit(&self) {
        loop {
            let wait = {
                let mut next = match self.next_admission.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                let now = Instant::now();
                let start = match *next {
                    Some(reserved) => reserved.max(now),
                    None => now,
                };
                *next = start.checked_add(self.interval).or(Some(start));
                start.saturating_duration_since(now)
            };
            if wait.is_zero() {
                return;
            }
            tokio::time::sleep(wait).await;
        }
    }
}
