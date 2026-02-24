//! Core KV store state and implementation.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use parking_lot::RwLock;
use tokio::sync::broadcast;

use numaflow_shared::kv::{KVEntry, KVError, KVStore, KVWatchOp, KVWatchStream};

use super::error::SimpleKVStoreError;
use super::error_injector::KVErrorInjector;

/// A history entry for watch replay.
#[derive(Debug, Clone)]
pub struct KVHistoryEntry {
    /// The revision at which this entry was created.
    pub revision: u64,
    /// The key that was modified.
    pub key: String,
    /// The value (empty for delete/purge).
    pub value: Bytes,
    /// The operation type.
    pub operation: KVWatchOp,
}

impl From<&KVHistoryEntry> for KVEntry {
    /// Convert to a KVEntry for the watch stream.
    fn from(value: &KVHistoryEntry) -> Self {
        KVEntry {
            key: value.key.clone(),
            value: value.value.clone(),
            operation: match value.operation {
                KVWatchOp::Put => KVWatchOp::Put,
                KVWatchOp::Delete => KVWatchOp::Delete,
                KVWatchOp::Purge => KVWatchOp::Purge,
            },
        }
    }
}

/// Internal state of the KV store.
#[derive(Debug)]
pub struct KVState {
    /// The actual key-value storage.
    pub(crate) data: HashMap<String, Bytes>,
    /// Current revision number (incremented on each mutation).
    pub(crate) revision: u64,
    /// History of changes for watch replay.
    pub(crate) history: Vec<KVHistoryEntry>,
    /// Maximum history size (0 = unlimited).
    pub(crate) max_history_size: usize,
}

impl Default for KVState {
    fn default() -> Self {
        Self::new(0)
    }
}

impl KVState {
    /// Create new KV state with optional max history size.
    pub fn new(max_history_size: usize) -> Self {
        Self {
            data: HashMap::new(),
            revision: 0,
            history: Vec::new(),
            max_history_size,
        }
    }

    /// Record a mutation in history.
    pub(crate) fn record_history(&mut self, key: String, value: Bytes, operation: KVWatchOp) {
        self.revision += 1;
        let entry = KVHistoryEntry {
            revision: self.revision,
            key,
            value,
            operation,
        };
        self.history.push(entry);

        // Trim history if needed
        if self.max_history_size > 0 && self.history.len() > self.max_history_size {
            let excess = self.history.len() - self.max_history_size;
            self.history.drain(0..excess);
        }
    }

    /// Get history entries from a specific revision (inclusive).
    pub(crate) fn history_from(&self, revision: u64) -> Vec<KVHistoryEntry> {
        self.history
            .iter()
            .filter(|e| e.revision >= revision)
            .cloned()
            .collect()
    }
}

/// Simple in-memory KV store for testing.
///
/// This provides a KV store that implements the `KVStore` trait and allows
/// for error injection to test negative cases.
#[derive(Clone)]
pub struct SimpleKVStore {
    /// Shared KV state.
    state: Arc<RwLock<KVState>>,
    /// Store name.
    name: &'static str,
    /// Error injector for testing.
    error_injector: Arc<KVErrorInjector>,
    /// Broadcast sender for watch updates.
    watch_sender: broadcast::Sender<KVHistoryEntry>,
}

impl std::fmt::Debug for SimpleKVStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleKVStore")
            .field("name", &self.name)
            .field("state", &self.state)
            .finish()
    }
}

impl SimpleKVStore {
    /// Create a new simple KV store.
    ///
    /// # Arguments
    /// * `name` - Name of the store (used as identifier).
    pub fn new(name: &'static str) -> Self {
        Self::with_config(name, 1000)
    }

    /// Create a new simple KV store with custom configuration.
    ///
    /// # Arguments
    /// * `name` - Name of the store.
    /// * `max_history_size` - Maximum history entries to keep (0 = unlimited).
    pub fn with_config(name: &'static str, max_history_size: usize) -> Self {
        let (watch_sender, _) = broadcast::channel(1024);
        Self {
            state: Arc::new(RwLock::new(KVState::new(max_history_size))),
            name,
            error_injector: Arc::new(KVErrorInjector::new()),
            watch_sender,
        }
    }

    /// Get the error injector for this store.
    ///
    /// Use this to inject errors for testing.
    pub fn error_injector(&self) -> &Arc<KVErrorInjector> {
        &self.error_injector
    }

    /// Get the current revision number.
    pub fn revision(&self) -> u64 {
        self.state.read().revision
    }

    /// Get the number of keys in the store.
    pub fn len(&self) -> usize {
        self.state.read().data.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.state.read().data.is_empty()
    }

    /// Clear all data in the store and emit a purge event.
    pub fn purge(&self) {
        let entry = {
            let mut state = self.state.write();
            state.data.clear();
            state.record_history(String::new(), Bytes::new(), KVWatchOp::Purge);

            // Notify watchers
            state.history.last().cloned()
        };

        if let Some(entry) = entry {
            let _ = self.watch_sender.send(entry);
        }
    }

    /// Get all data as a snapshot (for testing).
    pub fn snapshot(&self) -> HashMap<String, Bytes> {
        self.state.read().data.clone()
    }
}

#[async_trait]
impl KVStore for SimpleKVStore {
    async fn keys(&self) -> Result<Vec<String>, KVError> {
        self.error_injector.apply_keys_latency().await;

        if self.error_injector.should_fail_keys() {
            return Err(Box::new(SimpleKVStoreError::Keys(
                "injected failure".to_string(),
            )));
        }

        Ok(self.state.read().data.keys().cloned().collect())
    }

    async fn delete(&self, key: &str) -> Result<(), KVError> {
        self.error_injector.apply_delete_latency().await;

        if self.error_injector.should_fail_delete() {
            return Err(Box::new(SimpleKVStoreError::Delete(
                "injected failure".to_string(),
            )));
        }

        let entry = {
            let mut state = self.state.write();
            state.data.remove(key);
            state.record_history(key.to_string(), Bytes::new(), KVWatchOp::Delete);

            // Notify watchers
            state.history.last().cloned()
        };

        if let Some(entry) = entry {
            let _ = self.watch_sender.send(entry);
        }

        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<(), KVError> {
        self.error_injector.apply_put_latency().await;

        if self.error_injector.should_fail_put() {
            return Err(Box::new(SimpleKVStoreError::Put(
                "injected failure".to_string(),
            )));
        }

        let mut state = self.state.write();
        state.data.insert(key.to_string(), value.clone());
        state.record_history(key.to_string(), value, KVWatchOp::Put);

        // Notify watchers
        let entry = state.history.last().cloned();
        drop(state);
        if let Some(entry) = entry {
            let _ = self.watch_sender.send(entry);
        }

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, KVError> {
        self.error_injector.apply_get_latency().await;

        if self.error_injector.should_fail_get() {
            return Err(Box::new(SimpleKVStoreError::Get(
                "injected failure".to_string(),
            )));
        }

        Ok(self.state.read().data.get(key).cloned())
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn watch(&self, revision: Option<u64>) -> Result<KVWatchStream, KVError> {
        if self.error_injector.should_fail_watch() {
            return Err(Box::new(SimpleKVStoreError::Watch(
                "injected failure".to_string(),
            )));
        }

        // Get history entries to replay and subscribe to new updates
        let history = match revision {
            Some(rev) => self.state.read().history_from(rev),
            None => Vec::new(),
        };

        let receiver = self.watch_sender.subscribe();
        let close_receiver = self.error_injector.watch_close_receiver();
        let close_after = self.error_injector.get_close_watch_after();
        let error_injector = Arc::clone(&self.error_injector);

        let stream = SimpleKVWatchStream::new(
            history,
            receiver,
            close_receiver,
            close_after,
            error_injector,
        );

        Ok(Box::pin(stream))
    }
}

/// Watch stream implementation for SimpleKVStore.
struct SimpleKVWatchStream {
    /// History entries to replay first.
    history: Vec<KVHistoryEntry>,
    /// Index into history for replay.
    history_index: usize,
    /// Receiver for live updates.
    receiver: broadcast::Receiver<KVHistoryEntry>,
    /// Receiver for close signals.
    close_receiver: broadcast::Receiver<()>,
    /// Number of items after which to close (0 = disabled).
    close_after: usize,
    /// Number of items emitted.
    items_emitted: usize,
    /// Error injector reference.
    error_injector: Arc<KVErrorInjector>,
    /// Whether the stream is closed.
    closed: bool,
    /// Pending receive future.
    pending_recv: Option<Pin<Box<dyn std::future::Future<Output = RecvResult> + Send + 'static>>>,
}

/// Result type for receive operation.
type RecvResult = Result<KVHistoryEntry, broadcast::error::RecvError>;

impl SimpleKVWatchStream {
    fn new(
        history: Vec<KVHistoryEntry>,
        receiver: broadcast::Receiver<KVHistoryEntry>,
        close_receiver: broadcast::Receiver<()>,
        close_after: usize,
        error_injector: Arc<KVErrorInjector>,
    ) -> Self {
        Self {
            history,
            history_index: 0,
            receiver,
            close_receiver,
            close_after,
            items_emitted: 0,
            error_injector,
            closed: false,
            pending_recv: None,
        }
    }
}

impl Stream for SimpleKVWatchStream {
    type Item = KVEntry;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }

        // Check for close signal
        if self.error_injector.should_close_all_watches() {
            self.closed = true;
            return Poll::Ready(None);
        }

        // Check close_receiver for close signal (non-blocking)
        match self.close_receiver.try_recv() {
            Ok(()) => {
                self.closed = true;
                return Poll::Ready(None);
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                self.closed = true;
                return Poll::Ready(None);
            }
            Err(broadcast::error::TryRecvError::Empty)
            | Err(broadcast::error::TryRecvError::Lagged(_)) => {
                // No close signal, continue
            }
        }

        // Check if we should close after N items
        if self.close_after > 0 && self.items_emitted >= self.close_after {
            self.closed = true;
            return Poll::Ready(None);
        }

        // First, replay history
        if self.history_index < self.history.len() {
            let entry = (&self.history[self.history_index]).into();
            self.history_index += 1;
            self.items_emitted += 1;
            return Poll::Ready(Some(entry));
        }

        // Then, receive live updates using try_recv
        // We use try_recv because broadcast::Receiver doesn't have poll_recv
        match self.receiver.try_recv() {
            Ok(history_entry) => {
                self.items_emitted += 1;
                Poll::Ready(Some((&history_entry).into()))
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                self.closed = true;
                Poll::Ready(None)
            }
            Err(broadcast::error::TryRecvError::Empty) => {
                // No message available, we need to wait
                // Register waker and return pending
                // Since we can't directly poll the receiver, we'll use a simple
                // polling strategy - wake immediately to poll again
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                // Missed some messages, but continue receiving
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_basic_operations() {
        let store = SimpleKVStore::new("test-store");
        assert_eq!(store.name(), "test-store");
        assert!(store.is_empty());
        assert_eq!(store.revision(), 0);

        // Put
        store.put("key1", Bytes::from("value1")).await.unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.revision(), 1);

        // Get
        let value = store.get("key1").await.unwrap();
        assert_eq!(value, Some(Bytes::from("value1")));

        // Get non-existent
        let value = store.get("key2").await.unwrap();
        assert_eq!(value, None);

        // Keys
        let keys = store.keys().await.unwrap();
        assert_eq!(keys, vec!["key1".to_string()]);

        // Delete
        store.delete("key1").await.unwrap();
        assert!(store.is_empty());
        assert_eq!(store.revision(), 2);
    }

    #[tokio::test]
    async fn test_watch_live_updates() {
        let store = SimpleKVStore::new("test-store");

        // Start watching from now (no history replay)
        let mut watch = store.watch(None).await.unwrap();

        // Put a value
        store.put("key1", Bytes::from("value1")).await.unwrap();

        // Should receive the update
        let entry = watch.next().await.unwrap();
        assert_eq!(entry.key, "key1");
        assert_eq!(entry.value, Bytes::from("value1"));
        assert_eq!(entry.operation, KVWatchOp::Put);
    }

    #[tokio::test]
    async fn test_watch_history_replay() {
        let store = SimpleKVStore::new("test-store");

        // Put some values
        store.put("key1", Bytes::from("value1")).await.unwrap();
        store.put("key2", Bytes::from("value2")).await.unwrap();

        // Watch from revision 1 (should replay both)
        let mut watch = store.watch(Some(1)).await.unwrap();

        let entry = watch.next().await.unwrap();
        assert_eq!(entry.key, "key1");

        let entry = watch.next().await.unwrap();
        assert_eq!(entry.key, "key2");
    }

    #[tokio::test]
    async fn test_error_injection_put() {
        let store = SimpleKVStore::new("test-store");

        store.error_injector().fail_puts(1);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_err());

        // Next put should succeed
        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_injection_get() {
        let store = SimpleKVStore::new("test-store");
        store.put("key1", Bytes::from("value1")).await.unwrap();

        store.error_injector().fail_gets(1);

        let result = store.get("key1").await;
        assert!(result.is_err());

        // Next get should succeed
        let result = store.get("key1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_injection_delete() {
        let store = SimpleKVStore::new("test-store");
        store.put("key1", Bytes::from("value1")).await.unwrap();

        store.error_injector().fail_deletes(1);

        let result = store.delete("key1").await;
        assert!(result.is_err());

        // Next delete should succeed
        let result = store.delete("key1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_injection_keys() {
        let store = SimpleKVStore::new("test-store");
        store.put("key1", Bytes::from("value1")).await.unwrap();

        store.error_injector().fail_keys(1);

        let result = store.keys().await;
        assert!(result.is_err());

        // Next keys should succeed
        let result = store.keys().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_injection_watch_creation() {
        let store = SimpleKVStore::new("test-store");

        store.error_injector().fail_watches(1);

        let result = store.watch(None).await;
        assert!(result.is_err());

        // Next watch should succeed
        let result = store.watch(None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_watch_close_after_items() {
        let store = SimpleKVStore::new("test-store");

        // Configure to close after 2 items
        store.error_injector().close_watch_after(2);

        let mut watch = store.watch(None).await.unwrap();

        // Put values
        store.put("key1", Bytes::from("value1")).await.unwrap();
        store.put("key2", Bytes::from("value2")).await.unwrap();
        store.put("key3", Bytes::from("value3")).await.unwrap();

        // Should receive first 2 items
        assert!(watch.next().await.is_some());
        assert!(watch.next().await.is_some());

        // Stream should be closed
        assert!(watch.next().await.is_none());
    }

    #[tokio::test]
    async fn test_watch_force_close() {
        let store = SimpleKVStore::new("test-store");

        let mut watch = store.watch(None).await.unwrap();

        // Force close all watches
        store.error_injector().close_all_watch_streams();

        // Stream should be closed
        assert!(watch.next().await.is_none());
    }

    #[tokio::test]
    async fn test_purge() {
        let store = SimpleKVStore::new("test-store");

        store.put("key1", Bytes::from("value1")).await.unwrap();
        store.put("key2", Bytes::from("value2")).await.unwrap();

        let mut watch = store.watch(None).await.unwrap();

        store.purge();

        assert!(store.is_empty());

        // Should receive purge event
        let entry = watch.next().await.unwrap();
        assert_eq!(entry.operation, KVWatchOp::Purge);
    }

    #[tokio::test]
    async fn test_snapshot() {
        let store = SimpleKVStore::new("test-store");

        store.put("key1", Bytes::from("value1")).await.unwrap();
        store.put("key2", Bytes::from("value2")).await.unwrap();

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get("key1"), Some(&Bytes::from("value1")));
        assert_eq!(snapshot.get("key2"), Some(&Bytes::from("value2")));
    }

    #[test]
    fn test_kv_state_history_trimming() {
        let mut state = KVState::new(3);

        state.record_history("k1".to_string(), Bytes::from("v1"), KVWatchOp::Put);
        state.record_history("k2".to_string(), Bytes::from("v2"), KVWatchOp::Put);
        state.record_history("k3".to_string(), Bytes::from("v3"), KVWatchOp::Put);
        assert_eq!(state.history.len(), 3);

        // Adding 4th should trim oldest
        state.record_history("k4".to_string(), Bytes::from("v4"), KVWatchOp::Put);
        assert_eq!(state.history.len(), 3);
        assert_eq!(state.history[0].key, "k2");
    }
}
