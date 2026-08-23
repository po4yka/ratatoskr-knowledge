//! Fixed-spacing rate limiter tests.

use std::time::{Duration, Instant};

use ratatoskr_knowledge::RateLimiter;

#[tokio::test]
async fn second_call_waits_at_least_one_spacing_interval() -> Result<(), Box<dyn std::error::Error>>
{
    let run = async {
        let limiter = RateLimiter::new(Duration::from_millis(80));

        limiter.admit().await;
        let started = Instant::now();
        limiter.admit().await;
        let waited = started.elapsed();
        limiter.admit().await;

        waited
    };
    let waited = tokio::time::timeout(Duration::from_secs(2), run).await?;

    assert!(waited >= Duration::from_millis(75));
    Ok(())
}

#[tokio::test]
async fn zero_interval_admits_immediately() -> Result<(), Box<dyn std::error::Error>> {
    let run = async {
        let limiter = RateLimiter::new(Duration::ZERO);
        let started = Instant::now();
        limiter.admit().await;
        limiter.admit().await;
        limiter.admit().await;
        started.elapsed()
    };
    let elapsed = tokio::time::timeout(Duration::from_secs(2), run).await?;

    assert!(elapsed < Duration::from_millis(75));
    Ok(())
}
