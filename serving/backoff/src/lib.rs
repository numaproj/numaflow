//! Retry with backoff for async Rust.
//!
//! Given a Future, we have to run the Future to completion. If the Future returns an error,
//! we have retry the Future with a backoff [`strategy`]. Optionally, there are cases where retry is
//! not useful, and we could break out of retries early; for such cases we have [`Condition`].
//!
//! ```rust
//!
//! use crate::backoff::strategy::fixed;
//! use crate::backoff::retry::Retry;
//!
//! async fn some_work() -> Result<u64, ()> {
//!     Ok(42)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let interval = fixed::Interval::from_millis(1);
//!     let fut = Retry::retry(interval, some_work, |_: &()| true);
//!     let result = fut.await;
//!     assert!(result.is_ok());
//!     assert_eq!(result.unwrap(), 42);
//! }
//! ```

use std::future::Future;

/// strategy has all the different backoff strategies. It is an iterator with Item=Duration.
/// The strategy decides what duration to return. Since it is an iterator, we can stop the
/// iterator using [`take`](https://doc.rust-lang.org/std/iter/struct.Take.html).
pub mod strategy;

/// Conditional retry till we run out of backoff.
pub mod retry;

/// The retry condition depends on the result of [`Condition::can_retry`] function.
/// [`Condition::can_retry`] should return `true` to continue retrying or `false` to stop..
pub trait Condition<E> {
    fn can_retry(&self, error: &E) -> bool;
}

/// we can have an implementation where a fn pointer (Fn) can be passed which
/// can return bool based on the error
impl<E, F> Condition<E> for F
where
    F: Fn(&E) -> bool,
{
    fn can_retry(&self, error: &E) -> bool {
        self(error)
    }
}

/// An `Operation` is anything that returns a Future when executed and that
/// Future can be run to completion.
pub trait Operation {
    type Item;
    type Error;
    /// The [`Future`] returned when the Operation is called.
    type Future: Future<Output = Result<Self::Item, Self::Error>>;

    // a good example https://github.com/tower-rs/tower/blob/master/tower-service/src/lib.rs#L311
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn run(&mut self) -> Self::Future;
}

/// We can implement the [`Operation`] trait for [`FnMut`] that returns a [`Future`] whose output
/// is a [`Result`].
impl<T, E, R, F> Operation for F
where
    R: Future<Output = Result<T, E>>,
    F: FnMut() -> R,
{
    type Item = T;
    type Error = E;
    type Future = R;

    fn run(&mut self) -> Self::Future {
        // run the FnMut
        self()
    }
}

// A NOTE on the [`Operation`] trait!
// I tried without do this Operation trait, but my generic exploded, something like the below:
//
//   struct Retry<I, C, F, R, T, E>
//   where
//       R: Future<Output = Result<T, E>>,
//       F: FnMut() -> R
//
// I tried type alias, but then I ran into language issues https://github.com/rust-lang/rust/issues/21903
// (NB: even to get the compiler to work, I had to do PhantomData.)
//
// An ideal type alias would be as follows:
//   type Operation<T, E, R: Future<Output = Result<T, E>>, F: FnMut() -> R> = F;
// as of today, the above type alias is read by the compiler as follows
//   type Operation<T, E, R, F> = F;
// which is quite pointless.
