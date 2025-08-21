use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project::pin_project;
use tokio::time::{Instant, Sleep, sleep_until};

use crate::{Condition, Operation};

/// To retry a Future with backoff, we will have to maintain 2 states (which are Futures themselves).
/// These states track whether the [`Operation`] is [`RetryState::Running`] or cooling off via
/// [`RetryState::Sleeping`] as prescribed by the [`crate::strategy`]. The state-machine for retry
/// flips between these two states:
/// ```no_rust
///      (Pending)
///     /
/// (op)            (Ok) -> [Return(Ok)]*
///    \           /
///     (Ready) ---       (Non-retryable) -> [Return(Err)]*
///                \     /
///                 (Err)                            (None) -> [Return(Err)]*
///                      \                          /
///                       (Retryable) ---> (Backoff)          (Pending)
///                                                \        /
///                                                  (Sleep)
///                                                        \
///                                                         (Ready) --> [START(op)]
/// |------------------------------------|-------------------------|-----------------|
///           Operation                          Backoff             Operation(start)
///
/// ```
// We have to pin it because these are futures, and we want to do poll on these.
#[pin_project(project = RetryStateProj)]
enum RetryState<O>
where
    O: Operation,
{
    Running(#[pin] O::Future),
    Sleeping(#[pin] Sleep),
}

/// Retry retries an operation based on the backoff strategy.
#[pin_project]
pub struct Retry<I, O, C>
where
    O: Operation,
{
    #[pin]
    retry_state: RetryState<O>,
    backoff: I,
    operation: O,
    condition: C,
}

impl<I, O, C> Retry<I, O, C>
where
    I: Iterator<Item = Duration>,
    O: Operation,
    C: Condition<O::Error>,
{
    pub fn new<II: IntoIterator<IntoIter = I, Item = I::Item>>(
        backoff: II,
        mut operation: O,
        condition: C,
    ) -> Self {
        Self {
            retry_state: RetryState::Running(operation.run()),
            backoff: backoff.into_iter(),
            condition,
            operation,
        }
    }

    /// cools off before the next retry by doing a sleep on period determined by the backoff [`strategy`]
    fn cool_off(mut self: Pin<&mut Self>, err: O::Error) -> Result<(), O::Error> {
        match self.as_mut().project().backoff.next() {
            // ran out of backoff, return the same error
            None => Err(err),
            Some(duration) => {
                // set the state as sleeping
                let till = sleep_until(Instant::now() + duration);
                self.as_mut()
                    .project()
                    .retry_state
                    .set(RetryState::Sleeping(till));
                Ok(())
            }
        }
    }

    /// reattempts to run the [`Operation`] again.
    fn reattempt(mut self: Pin<&mut Self>) {
        let future = {
            let this = self.as_mut().project();
            this.operation.run()
        };
        self.as_mut()
            .project()
            .retry_state
            .set(RetryState::Running(future));
    }
}

impl<I, O, C> Future for Retry<I, O, C>
where
    I: Iterator<Item = Duration>,
    O: Operation,
    C: Condition<O::Error>,
{
    type Output = Result<O::Item, O::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().project().retry_state.project() {
            RetryStateProj::Running(op) => match op.poll(cx) {
                Poll::Ready(output) => match output {
                    Ok(item) => Poll::Ready(Ok(item)),
                    Err(e) => {
                        // if the error can be retried
                        if self.as_mut().condition.can_retry(&e) {
                            // before we can retry, we can to wait for a cool-off period (aka backoff)
                            match self.as_mut().cool_off(e) {
                                Ok(_) => self.as_mut().poll(cx),
                                Err(e) => Poll::Ready(Err(e)),
                            }
                        } else {
                            Poll::Ready(Err(e))
                        }
                    }
                },
                Poll::Pending => Poll::Pending,
            },
            RetryStateProj::Sleeping(sleep) => match sleep.poll(cx) {
                Poll::Ready(_) => {
                    self.as_mut().reattempt();
                    self.poll(cx)
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::strategy::fixed;

    async fn always_successful() -> Result<u64, ()> {
        Ok(42)
    }

    fn true_cond<E>(_: &E) -> bool {
        true
    }

    fn false_cond<E>(_: &E) -> bool {
        false
    }

    #[tokio::test]
    async fn successful_first_attempt() {
        let interval = fixed::Interval::from_millis(1);
        let fut = Retry::new(interval, always_successful, |_: &()| true);
        let result = fut.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn non_retriable_failure() {
        let interval = fixed::Interval::from_millis(1);

        let fut = Retry::new(
            interval,
            || future::ready(Err::<(), &str>("err")),
            false_cond,
        );
        let result = fut.await;
        assert!(result.is_err());
        assert_eq!(result, Err("err"));
    }

    #[tokio::test]
    async fn retry_till_condition() {
        let interval = fixed::Interval::from_millis(1).take(10);

        let counter = Arc::new(AtomicUsize::new(0));
        let cloned_counter = Arc::clone(&counter);

        let fut = Retry::new(
            interval,
            move || {
                let previous = cloned_counter.fetch_add(1, Ordering::SeqCst);
                future::ready(Err::<(), usize>(previous + 1))
            },
            |e: &usize| *e < 3,
        );

        let result = fut.await;
        assert_eq!(result, Err(3));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_till_exhaustion() {
        let attempts = 5;

        let interval = fixed::Interval::from_millis(1).take(attempts);

        let counter = Arc::new(AtomicUsize::new(0));
        let cloned_counter = Arc::clone(&counter);

        let fut = Retry::new(
            interval,
            move || {
                let previous = cloned_counter.fetch_add(1, Ordering::SeqCst);
                future::ready(Err::<(), usize>(previous + 1))
            },
            true_cond,
        );

        let result = fut.await;
        // + 1 because take(n) are retries and the first run is not a retry
        assert_eq!(result, Err(attempts + 1));
        assert_eq!(counter.load(Ordering::SeqCst), attempts + 1);
    }
}
