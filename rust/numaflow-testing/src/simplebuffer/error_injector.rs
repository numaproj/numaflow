//! Error injection controller for testing.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Controller for injecting errors during testing.
///
/// This allows tests to force specific error conditions like buffer full,
/// ack failures, nack failures, etc.
#[derive(Debug)]
pub struct ErrorInjector {
    /// Force buffer to appear full.
    pub force_buffer_full: AtomicBool,
    /// Fail the next N writes.
    fail_next_writes: AtomicUsize,
    /// Skip the first N writes before starting to fail.
    skip_writes_before_fail: AtomicUsize,
    /// Fail the next N fetches.
    fail_next_fetches: AtomicUsize,
    /// Fail the next N acks.
    fail_next_acks: AtomicUsize,
    /// Fail the next N nacks.
    fail_next_nacks: AtomicUsize,
    /// Fail the next N wip acks.
    fail_next_wip_acks: AtomicUsize,
    /// Fail the next N resolves.
    fail_next_resolves: AtomicUsize,
    /// Artificial write latency in milliseconds.
    write_latency_ms: AtomicU64,
    /// Artificial fetch latency in milliseconds.
    fetch_latency_ms: AtomicU64,
    /// Artificial ack latency in milliseconds.
    ack_latency_ms: AtomicU64,
    /// Artificial resolve latency in milliseconds.
    resolve_latency_ms: AtomicU64,
}

impl Default for ErrorInjector {
    fn default() -> Self {
        Self {
            force_buffer_full: AtomicBool::new(false),
            fail_next_writes: AtomicUsize::new(0),
            skip_writes_before_fail: AtomicUsize::new(0),
            fail_next_fetches: AtomicUsize::new(0),
            fail_next_acks: AtomicUsize::new(0),
            fail_next_nacks: AtomicUsize::new(0),
            fail_next_wip_acks: AtomicUsize::new(0),
            fail_next_resolves: AtomicUsize::new(0),
            write_latency_ms: AtomicU64::new(0),
            fetch_latency_ms: AtomicU64::new(0),
            ack_latency_ms: AtomicU64::new(0),
            resolve_latency_ms: AtomicU64::new(0),
        }
    }
}

impl ErrorInjector {
    /// Create a new error injector with all errors disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the buffer to appear full.
    pub fn set_buffer_full(&self, full: bool) {
        self.force_buffer_full.store(full, Ordering::Relaxed);
    }

    /// Fail the next N write operations.
    pub fn fail_writes(&self, count: usize) {
        self.fail_next_writes.store(count, Ordering::Relaxed);
    }

    /// Skip the first `skip` writes, then fail the next `fail` writes.
    /// This is useful for testing retry scenarios where the initial write
    /// should succeed but subsequent retries should fail.
    pub fn skip_writes_then_fail(&self, skip: usize, fail: usize) {
        self.skip_writes_before_fail.store(skip, Ordering::Relaxed);
        self.fail_next_writes.store(fail, Ordering::Relaxed);
    }

    /// Fail the next N fetch operations.
    pub fn fail_fetches(&self, count: usize) {
        self.fail_next_fetches.store(count, Ordering::Relaxed);
    }

    /// Fail the next N ack operations.
    pub fn fail_acks(&self, count: usize) {
        self.fail_next_acks.store(count, Ordering::Relaxed);
    }

    /// Fail the next N nack operations.
    pub fn fail_nacks(&self, count: usize) {
        self.fail_next_nacks.store(count, Ordering::Relaxed);
    }

    /// Fail the next N wip ack operations.
    pub fn fail_wip_acks(&self, count: usize) {
        self.fail_next_wip_acks.store(count, Ordering::Relaxed);
    }

    /// Fail the next N resolve operations.
    pub fn fail_resolves(&self, count: usize) {
        self.fail_next_resolves.store(count, Ordering::Relaxed);
    }

    /// Set artificial write latency.
    pub fn set_write_latency(&self, ms: u64) {
        self.write_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Set artificial fetch latency.
    pub fn set_fetch_latency(&self, ms: u64) {
        self.fetch_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Set artificial ack latency.
    pub fn set_ack_latency(&self, ms: u64) {
        self.ack_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Set artificial resolve latency.
    pub fn set_resolve_latency(&self, ms: u64) {
        self.resolve_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Check and decrement the write failure counter.
    /// If skip_writes_before_fail is set, decrements that first and returns false.
    pub(crate) fn should_fail_write(&self) -> bool {
        // First check if we need to skip this write
        let skip = self.skip_writes_before_fail
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                if c > 0 { Some(c - 1) } else { None }
            }).is_ok();
        if skip {
            return false;
        }
        // Then check if we should fail
        Self::decrement_counter(&self.fail_next_writes)
    }

    /// Check and decrement the fetch failure counter.
    pub(crate) fn should_fail_fetch(&self) -> bool {
        Self::decrement_counter(&self.fail_next_fetches)
    }

    /// Check and decrement the ack failure counter.
    pub(crate) fn should_fail_ack(&self) -> bool {
        Self::decrement_counter(&self.fail_next_acks)
    }

    /// Check and decrement the nack failure counter.
    pub(crate) fn should_fail_nack(&self) -> bool {
        Self::decrement_counter(&self.fail_next_nacks)
    }

    /// Check and decrement the wip ack failure counter.
    pub(crate) fn should_fail_wip_ack(&self) -> bool {
        Self::decrement_counter(&self.fail_next_wip_acks)
    }

    /// Check and decrement the resolve failure counter.
    pub(crate) fn should_fail_resolve(&self) -> bool {
        Self::decrement_counter(&self.fail_next_resolves)
    }

    /// Helper to decrement a counter and return true if it was > 0.
    fn decrement_counter(counter: &AtomicUsize) -> bool {
        let count = counter.load(Ordering::Relaxed);
        if count > 0 {
            counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    if c > 0 { Some(c - 1) } else { None }
                })
                .ok();
            return true;
        }
        false
    }

    /// Apply write latency if set.
    pub(crate) async fn apply_write_latency(&self) {
        self.apply_latency(&self.write_latency_ms).await;
    }

    /// Apply fetch latency if set.
    pub(crate) async fn apply_fetch_latency(&self) {
        self.apply_latency(&self.fetch_latency_ms).await;
    }

    /// Apply ack latency if set.
    pub(crate) async fn apply_ack_latency(&self) {
        self.apply_latency(&self.ack_latency_ms).await;
    }

    /// Apply resolve latency if set.
    pub(crate) async fn apply_resolve_latency(&self) {
        self.apply_latency(&self.resolve_latency_ms).await;
    }

    /// Helper to apply latency.
    async fn apply_latency(&self, latency_ms: &AtomicU64) {
        let ms = latency_ms.load(Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_injector_init_and_buffer_full() {
        // Default/new starts with everything disabled
        let injector = ErrorInjector::default();
        assert!(!injector.force_buffer_full.load(Ordering::Relaxed));
        assert!(!injector.should_fail_write());
        assert!(!injector.should_fail_fetch());
        assert!(!injector.should_fail_ack());
        assert!(!injector.should_fail_nack());
        assert!(!injector.should_fail_wip_ack());

        // Toggle buffer full
        let injector = ErrorInjector::new();
        injector.set_buffer_full(true);
        assert!(injector.force_buffer_full.load(Ordering::Relaxed));
        injector.set_buffer_full(false);
        assert!(!injector.force_buffer_full.load(Ordering::Relaxed));
    }

    #[test]
    fn test_failure_countdowns() {
        let injector = ErrorInjector::new();

        // Write countdown
        injector.fail_writes(2);
        assert!(injector.should_fail_write());
        assert!(injector.should_fail_write());
        assert!(!injector.should_fail_write());

        // Fetch countdown
        injector.fail_fetches(1);
        assert!(injector.should_fail_fetch());
        assert!(!injector.should_fail_fetch());

        // Ack countdown
        injector.fail_acks(1);
        assert!(injector.should_fail_ack());
        assert!(!injector.should_fail_ack());

        // Nack countdown
        injector.fail_nacks(1);
        assert!(injector.should_fail_nack());
        assert!(!injector.should_fail_nack());

        // WIP ack countdown
        injector.fail_wip_acks(1);
        assert!(injector.should_fail_wip_ack());
        assert!(!injector.should_fail_wip_ack());

        // Reset by setting to 0
        injector.fail_writes(5);
        assert!(injector.should_fail_write());
        injector.fail_writes(0);
        assert!(!injector.should_fail_write());
    }

    #[test]
    fn test_independent_failure_counters() {
        let injector = ErrorInjector::new();

        injector.fail_writes(1);
        injector.fail_acks(2);

        // Consuming write failures doesn't affect ack failures
        assert!(injector.should_fail_write());
        assert!(!injector.should_fail_write());
        assert!(injector.should_fail_ack());
        assert!(injector.should_fail_ack());
        assert!(!injector.should_fail_ack());
    }

    #[test]
    fn test_latency_settings() {
        let injector = ErrorInjector::new();

        injector.set_write_latency(100);
        injector.set_fetch_latency(200);
        injector.set_ack_latency(300);

        assert_eq!(injector.write_latency_ms.load(Ordering::Relaxed), 100);
        assert_eq!(injector.fetch_latency_ms.load(Ordering::Relaxed), 200);
        assert_eq!(injector.ack_latency_ms.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_decrement_counter() {
        // From zero - no change
        let counter = AtomicUsize::new(0);
        assert!(!ErrorInjector::decrement_counter(&counter));
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        // From positive - decrements
        let counter = AtomicUsize::new(5);
        assert!(ErrorInjector::decrement_counter(&counter));
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn test_apply_latencies() {
        let injector = ErrorInjector::new();

        // Zero latency is nearly instant
        injector.set_write_latency(0);
        let start = std::time::Instant::now();
        injector.apply_write_latency().await;
        assert!(start.elapsed().as_millis() < 50);

        // Non-zero write latency
        injector.set_write_latency(50);
        let start = std::time::Instant::now();
        injector.apply_write_latency().await;
        assert!(start.elapsed().as_millis() >= 50);

        // Fetch latency
        injector.set_fetch_latency(50);
        let start = std::time::Instant::now();
        injector.apply_fetch_latency().await;
        assert!(start.elapsed().as_millis() >= 50);

        // Ack latency
        injector.set_ack_latency(50);
        let start = std::time::Instant::now();
        injector.apply_ack_latency().await;
        assert!(start.elapsed().as_millis() >= 50);
    }
}
