use numaflow_throttling::{RateLimit, RateLimiter, TokenCalcBounds, WithoutDistributedState};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Rate Limiter Timeout Demo");
    println!("=========================");

    // Create a rate limiter with 10 max tokens, 5 initial burst, refilled every second
    let bounds = TokenCalcBounds::new(10, 5, Duration::from_secs(1));
    let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds)?;

    // Demo 1: Immediate success when tokens are available
    println!("\n1. Acquiring 3 tokens (should succeed immediately):");
    let start = Instant::now();
    let tokens = rate_limiter
        .acquire_n(Some(3), Some(Duration::from_millis(500)))
        .await;
    let elapsed = start.elapsed();
    println!("   Got {} tokens in {:?}", tokens, elapsed);

    // Demo 2: Immediate return when no timeout specified and tokens exhausted
    println!("\n2. Acquiring remaining tokens with no timeout:");
    let start = Instant::now();
    let tokens = rate_limiter.acquire_n(None, None).await;
    let elapsed = start.elapsed();
    println!("   Got {} tokens in {:?}", tokens, elapsed);

    // Demo 3: Immediate return when no timeout and no tokens available
    println!("\n3. Trying to acquire 1 token with no timeout (should return 0 immediately):");
    let start = Instant::now();
    let tokens = rate_limiter.acquire_n(Some(1), None).await;
    let elapsed = start.elapsed();
    println!("   Got {} tokens in {:?}", tokens, elapsed);

    // Demo 4: Timeout when tokens not available
    println!("\n4. Trying to acquire 1 token with 300ms timeout (should timeout):");
    let start = Instant::now();
    let tokens = rate_limiter
        .acquire_n(Some(1), Some(Duration::from_millis(300)))
        .await;
    let elapsed = start.elapsed();
    println!("   Got {} tokens in {:?}", tokens, elapsed);

    // Demo 5: Success with timeout when tokens become available
    println!("\n5. Waiting for next epoch and acquiring tokens with timeout:");
    
    // Wait for the next second to pass (tokens will be refilled)
    sleep(Duration::from_millis(1100)).await;
    
    let start = Instant::now();
    let tokens = rate_limiter
        .acquire_n(Some(5), Some(Duration::from_secs(2)))
        .await;
    let elapsed = start.elapsed();
    println!("   Got {} tokens in {:?}", tokens, elapsed);

    println!("\nDemo completed!");
    Ok(())
}
