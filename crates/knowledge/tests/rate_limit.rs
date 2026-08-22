//! Fixed-spacing rate limiter tests.

use std::time::{Duration, Instant};

use ratatoskr_knowledge::RateLimiter;

#[tokio::test]
async fn second_call_waits_at_least_one_spacing_interval() -> Result<(), Box<dyn std::error::Error>>
{
    let limiter = RateLimiter::new(Duration::from_millis(80));

    limiter.admit().await;
    let started = Instant::now();
    limiter.admit().await;
    let waited = started.elapsed();

    assert!(waited >= Duration::from_millis(75));
    Ok(())
}

#[tokio::test]
async fn zero_interval_admits_immediately() -> Result<(), Box<dyn std::error::Error>> {
    let limiter = RateLimiter::new(Duration::ZERO);

    let started = Instant::now();
    limiter.admit().await;
    limiter.admit().await;
    limiter.admit().await;

    assert!(started.elapsed() < Duration::from_millis(75));
    Ok(())
}
