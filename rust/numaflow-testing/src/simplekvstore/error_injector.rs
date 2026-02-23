//! Error injection controller for KV store testing.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::broadcast;

/// Controller for injecting errors during KV store testing.
///
/// This allows tests to force specific error conditions like operation failures,
/// watch stream termination, latency injection, etc.
#[derive(Debug)]
pub struct KVErrorInjector {
    /// Fail the next N keys operations.
    fail_next_keys: AtomicUsize,
    /// Fail the next N get operations.
    fail_next_gets: AtomicUsize,
    /// Fail the next N put operations.
    fail_next_puts: AtomicUsize,
    /// Fail the next N delete operations.
    fail_next_deletes: AtomicUsize,
    /// Fail the next N watch operations (when creating the watch).
    fail_next_watches: AtomicUsize,
    /// Close watch streams after N items (0 = disabled).
    close_watch_after_items: AtomicUsize,
    /// Flag to immediately close all active watch streams.
    close_all_watches: AtomicBool,
    /// Artificial keys latency in milliseconds.
    keys_latency_ms: AtomicU64,
    /// Artificial get latency in milliseconds.
    get_latency_ms: AtomicU64,
    /// Artificial put latency in milliseconds.
    put_latency_ms: AtomicU64,
    /// Artificial delete latency in milliseconds.
    delete_latency_ms: AtomicU64,
    /// Sender to broadcast watch close signals.
    watch_close_sender: broadcast::Sender<()>,
}

impl Default for KVErrorInjector {
    fn default() -> Self {
        let (watch_close_sender, _) = broadcast::channel(16);
        Self {
            fail_next_keys: AtomicUsize::new(0),
            fail_next_gets: AtomicUsize::new(0),
            fail_next_puts: AtomicUsize::new(0),
            fail_next_deletes: AtomicUsize::new(0),
            fail_next_watches: AtomicUsize::new(0),
            close_watch_after_items: AtomicUsize::new(0),
            close_all_watches: AtomicBool::new(false),
            keys_latency_ms: AtomicU64::new(0),
            get_latency_ms: AtomicU64::new(0),
            put_latency_ms: AtomicU64::new(0),
            delete_latency_ms: AtomicU64::new(0),
            watch_close_sender,
        }
    }
}

impl KVErrorInjector {
    /// Create a new error injector with all errors disabled.
    pub fn new() -> Self {
        Self::default()
    }

    // === Failure injection setters ===

    /// Fail the next N keys operations.
    pub fn fail_keys(&self, count: usize) {
        self.fail_next_keys.store(count, Ordering::Relaxed);
    }

    /// Fail the next N get operations.
    pub fn fail_gets(&self, count: usize) {
        self.fail_next_gets.store(count, Ordering::Relaxed);
    }

    /// Fail the next N put operations.
    pub fn fail_puts(&self, count: usize) {
        self.fail_next_puts.store(count, Ordering::Relaxed);
    }

    /// Fail the next N delete operations.
    pub fn fail_deletes(&self, count: usize) {
        self.fail_next_deletes.store(count, Ordering::Relaxed);
    }

    /// Fail the next N watch creation operations.
    pub fn fail_watches(&self, count: usize) {
        self.fail_next_watches.store(count, Ordering::Relaxed);
    }

    /// Close watch streams after emitting N items (0 = disabled).
    /// This simulates stream interruption scenarios.
    pub fn close_watch_after(&self, items: usize) {
        self.close_watch_after_items.store(items, Ordering::Relaxed);
    }

    /// Immediately close all active watch streams.
    /// This forces watchers to be recreated.
    pub fn close_all_watch_streams(&self) {
        self.close_all_watches.store(true, Ordering::SeqCst);
        // Send close signal to all watchers
        let _ = self.watch_close_sender.send(());
    }

    /// Reset the close all watches flag.
    pub fn reset_close_all_watches(&self) {
        self.close_all_watches.store(false, Ordering::SeqCst);
    }

    /// Get a receiver for watch close signals.
    pub fn watch_close_receiver(&self) -> broadcast::Receiver<()> {
        self.watch_close_sender.subscribe()
    }

    // === Latency injection setters ===

    /// Set artificial keys operation latency.
    pub fn set_keys_latency(&self, ms: u64) {
        self.keys_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Set artificial get operation latency.
    pub fn set_get_latency(&self, ms: u64) {
        self.get_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Set artificial put operation latency.
    pub fn set_put_latency(&self, ms: u64) {
        self.put_latency_ms.store(ms, Ordering::Relaxed);
    }

    /// Set artificial delete operation latency.
    pub fn set_delete_latency(&self, ms: u64) {
        self.delete_latency_ms.store(ms, Ordering::Relaxed);
    }

    // === Internal check methods ===

    /// Check and decrement the keys failure counter.
    pub(crate) fn should_fail_keys(&self) -> bool {
        Self::decrement_counter(&self.fail_next_keys)
    }

    /// Check and decrement the get failure counter.
    pub(crate) fn should_fail_get(&self) -> bool {
        Self::decrement_counter(&self.fail_next_gets)
    }

    /// Check and decrement the put failure counter.
    pub(crate) fn should_fail_put(&self) -> bool {
        Self::decrement_counter(&self.fail_next_puts)
    }

    /// Check and decrement the delete failure counter.
    pub(crate) fn should_fail_delete(&self) -> bool {
        Self::decrement_counter(&self.fail_next_deletes)
    }

    /// Check and decrement the watch failure counter.
    pub(crate) fn should_fail_watch(&self) -> bool {
        Self::decrement_counter(&self.fail_next_watches)
    }

    /// Check if watches should be closed immediately.
    pub(crate) fn should_close_all_watches(&self) -> bool {
        self.close_all_watches.load(Ordering::SeqCst)
    }

    /// Get the number of items after which to close watch stream (0 = disabled).
    pub(crate) fn get_close_watch_after(&self) -> usize {
        self.close_watch_after_items.load(Ordering::Relaxed)
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

    // === Latency application methods ===

    /// Apply keys latency if set.
    pub(crate) async fn apply_keys_latency(&self) {
        self.apply_latency(&self.keys_latency_ms).await;
    }

    /// Apply get latency if set.
    pub(crate) async fn apply_get_latency(&self) {
        self.apply_latency(&self.get_latency_ms).await;
    }

    /// Apply put latency if set.
    pub(crate) async fn apply_put_latency(&self) {
        self.apply_latency(&self.put_latency_ms).await;
    }

    /// Apply delete latency if set.
    pub(crate) async fn apply_delete_latency(&self) {
        self.apply_latency(&self.delete_latency_ms).await;
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
    fn test_error_injector_default() {
        let injector = KVErrorInjector::default();
        assert!(!injector.should_fail_keys());
        assert!(!injector.should_fail_get());
        assert!(!injector.should_fail_put());
        assert!(!injector.should_fail_delete());
        assert!(!injector.should_fail_watch());
        assert!(!injector.should_close_all_watches());
        assert_eq!(injector.get_close_watch_after(), 0);
    }

    #[test]
    fn test_failure_countdowns() {
        let injector = KVErrorInjector::new();

        // Keys countdown
        injector.fail_keys(2);
        assert!(injector.should_fail_keys());
        assert!(injector.should_fail_keys());
        assert!(!injector.should_fail_keys());

        // Get countdown
        injector.fail_gets(1);
        assert!(injector.should_fail_get());
        assert!(!injector.should_fail_get());

        // Put countdown
        injector.fail_puts(1);
        assert!(injector.should_fail_put());
        assert!(!injector.should_fail_put());

        // Delete countdown
        injector.fail_deletes(1);
        assert!(injector.should_fail_delete());
        assert!(!injector.should_fail_delete());

        // Watch countdown
        injector.fail_watches(1);
        assert!(injector.should_fail_watch());
        assert!(!injector.should_fail_watch());
    }

    #[test]
    fn test_watch_close_controls() {
        let injector = KVErrorInjector::new();

        // Close after N items
        injector.close_watch_after(5);
        assert_eq!(injector.get_close_watch_after(), 5);

        // Close all watches
        assert!(!injector.should_close_all_watches());
        injector.close_all_watch_streams();
        assert!(injector.should_close_all_watches());
        injector.reset_close_all_watches();
        assert!(!injector.should_close_all_watches());
    }

    #[test]
    fn test_latency_settings() {
        let injector = KVErrorInjector::new();

        injector.set_keys_latency(100);
        injector.set_get_latency(200);
        injector.set_put_latency(300);
        injector.set_delete_latency(400);

        assert_eq!(injector.keys_latency_ms.load(Ordering::Relaxed), 100);
        assert_eq!(injector.get_latency_ms.load(Ordering::Relaxed), 200);
        assert_eq!(injector.put_latency_ms.load(Ordering::Relaxed), 300);
        assert_eq!(injector.delete_latency_ms.load(Ordering::Relaxed), 400);
    }

    #[tokio::test]
    async fn test_apply_latencies() {
        let injector = KVErrorInjector::new();

        // Zero latency is nearly instant
        injector.set_get_latency(0);
        let start = std::time::Instant::now();
        injector.apply_get_latency().await;
        assert!(start.elapsed().as_millis() < 50);

        // Non-zero latency
        injector.set_put_latency(50);
        let start = std::time::Instant::now();
        injector.apply_put_latency().await;
        assert!(start.elapsed().as_millis() >= 50);
    }

    #[test]
    fn test_watch_close_receiver() {
        let injector = KVErrorInjector::new();
        let mut receiver = injector.watch_close_receiver();

        // Should receive signal when close_all_watch_streams is called
        injector.close_all_watch_streams();

        // Use try_recv since we just sent the signal
        assert!(receiver.try_recv().is_ok());
    }
}
