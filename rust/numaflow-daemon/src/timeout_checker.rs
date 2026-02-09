//! Async timeout helper functions for tests.

use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;

/// Runs a future and returns `()` if it completes within `duration`.
/// Panics on timeout.
pub(crate) async fn should_not_timeout<F, T>(duration: Duration, future: F)
where
    F: Future<Output = T>,
{
    match timeout(duration, future).await {
        Ok(_) => (),
        Err(_) => panic!("Timed out waiting after {:?}", duration),
    }
}

/// Runs a future and returns `()` if it times out within `duration`.
/// Panics if the future unexpectedly completes.
pub(crate) async fn should_timeout<F, T>(duration: Duration, future: F)
where
    F: Future<Output = T>,
{
    if timeout(duration, future).await.is_ok() {
        panic!("Unexpected completion before {:?}", duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn should_not_timeout_success() {
        should_not_timeout(Duration::from_millis(50), async {}).await;
    }

    #[tokio::test]
    #[should_panic(expected = "Timed out waiting after")]
    async fn should_not_timeout_panics_on_timeout() {
        should_not_timeout(Duration::from_millis(10), sleep(Duration::from_millis(50))).await;
    }

    #[tokio::test]
    async fn should_timeout_success() {
        should_timeout(Duration::from_millis(10), sleep(Duration::from_millis(50))).await;
    }

    #[tokio::test]
    #[should_panic(expected = "Unexpected completion before")]
    async fn should_timeout_panics_on_completion() {
        should_timeout(Duration::from_millis(50), async {}).await;
    }
}
