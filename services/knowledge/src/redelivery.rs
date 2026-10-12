//! The delay and the visibility of a redelivered delivery (XR-021 CONTRACTS.md R2-05 and R4-05).
//!
//! A transiently failing message is redelivered after `min(base * 2^(delivered - 1), cap)`, so a
//! persistent fault never runs hotter than once a cap period. The schedule is a value, not a
//! configuration key: production uses [`RedeliverySchedule::STANDARD`], and a test that needs eight
//! redeliveries inside its deadline passes a faster one through the spawn seam.

use std::sync::Arc;
use std::time::Duration;

use crate::Metrics;

/// Doubling stops mattering long before this: `2 s << 20` is far beyond any cap.
const MAX_EXPONENT: i64 = 20;

/// How long a failing delivery waits before the broker delivers it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedeliverySchedule {
    base: Duration,
    cap: Duration,
}

impl RedeliverySchedule {
    /// Two seconds after the first failure, doubling, at most one minute.
    pub const STANDARD: Self = Self::new(Duration::from_secs(2), Duration::from_mins(1));

    /// A schedule that starts at `base` and never exceeds `cap`.
    #[must_use]
    pub const fn new(base: Duration, cap: Duration) -> Self {
        Self { base, cap }
    }

    /// Delay before a delivery that failed as delivery number `delivered` is delivered again.
    ///
    /// A missing or zero count is the first delivery.
    #[must_use]
    pub fn delay(&self, delivered: i64) -> Duration {
        let exponent = delivered.saturating_sub(1).clamp(0, MAX_EXPONENT);
        let doubling = 1_u32 << u32::try_from(exponent).unwrap_or(0);
        self.base.saturating_mul(doubling).min(self.cap)
    }
}

/// What a worker needs to redeliver a failed delivery and to make the redelivery visible.
#[derive(Debug, Clone)]
pub struct Redelivery {
    pub(crate) schedule: RedeliverySchedule,
    pub(crate) metrics: Arc<Metrics>,
}

impl Redelivery {
    /// The production schedule counting on `metrics`.
    #[must_use]
    pub const fn standard(metrics: Arc<Metrics>) -> Self {
        Self::new(metrics, RedeliverySchedule::STANDARD)
    }

    /// An explicit schedule counting on `metrics`.
    #[must_use]
    pub const fn new(metrics: Arc<Metrics>, schedule: RedeliverySchedule) -> Self {
        Self { schedule, metrics }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RedeliverySchedule;

    /// `min(2 s * 2^(delivered-1), 60 s)`: deliveries one to ten, and a missing count.
    #[test]
    fn redelivery_delay_doubles_from_two_seconds_and_stops_at_sixty() {
        let seconds: Vec<u64> = (1..=10)
            .map(|delivered| RedeliverySchedule::STANDARD.delay(delivered).as_secs())
            .collect();
        assert_eq!(seconds, [2, 4, 8, 16, 32, 60, 60, 60, 60, 60]);
        assert_eq!(RedeliverySchedule::STANDARD.delay(1_000).as_secs(), 60);
        assert_eq!(RedeliverySchedule::STANDARD.delay(i64::MAX).as_secs(), 60);
        assert_eq!(
            RedeliverySchedule::STANDARD.delay(0).as_secs(),
            2,
            "a missing delivery count is the first delivery"
        );
    }

    #[test]
    fn a_faster_schedule_keeps_the_same_shape() {
        let fast = RedeliverySchedule::new(Duration::from_millis(10), Duration::from_millis(50));
        let millis: Vec<u128> = (1..=5)
            .map(|delivered| fast.delay(delivered).as_millis())
            .collect();
        assert_eq!(millis, [10, 20, 40, 50, 50]);
    }
}
