//! JetStream implementation of the KV store traits.
//!
//! This module provides a JetStream-backed implementation of [`KVStore`].

use super::{KVEntry, KVError, KVStore, KVWatchOp, KVWatchStream};
use crate::isb::jetstream::JetstreamWatcher;
use async_nats::jetstream::kv::{Entry, Store};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Convert a JetStream entry to a KVEntry
impl From<Entry> for KVEntry {
    fn from(value: Entry) -> Self {
        let operation = match value.operation {
            async_nats::jetstream::kv::Operation::Put => KVWatchOp::Put,
            async_nats::jetstream::kv::Operation::Delete => KVWatchOp::Delete,
            async_nats::jetstream::kv::Operation::Purge => KVWatchOp::Purge,
        };

        KVEntry {
            key: value.key,
            value: value.value,
            operation,
        }
    }
}

/// Resilient wrapper stream that uses JetstreamWatcher (with auto-reconnection)
/// and converts JetStream entries to KVEntry.
struct ResilientJetstreamWatchAdapter {
    inner: JetstreamWatcher,
}

impl Stream for ResilientJetstreamWatchAdapter {
    type Item = KVEntry;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // JetstreamWatcher already handles reconnection internally and returns Entry directly
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(entry)) => Poll::Ready(Some(entry.into())),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// JetStream KV Store implementation.
///
/// Wraps an `async_nats::jetstream::kv::Store` and implements [`LocalKVStorer`].
/// Use this as `Arc<dyn KVStorer>` for dynamic dispatch.
pub struct JetstreamKVStore {
    store: Store,
    name: &'static str,
}

impl JetstreamKVStore {
    /// Create a new JetstreamKVStore from an existing JetStream KV Store.
    ///
    /// # Arguments
    /// * `store` - The JetStream KV store handle
    /// * `name` - A name/identifier for this store (typically the bucket name)
    pub fn new(store: Store, name: &'static str) -> Self {
        Self { store, name }
    }
}

#[async_trait]
impl KVStore for JetstreamKVStore {
    async fn keys(&self) -> Result<Vec<String>, KVError> {
        self.store
            .keys()
            .await
            .map_err(|e| Box::new(e) as KVError)?
            .try_collect()
            .await
            .map_err(|e| Box::new(e) as KVError)
    }

    async fn delete(&self, key: &str) -> Result<(), KVError> {
        self.store
            .delete(key)
            .await
            .map_err(|e| Box::new(e) as KVError)?;
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<(), KVError> {
        self.store
            .put(key, value)
            .await
            .map_err(|e| Box::new(e) as KVError)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, KVError> {
        self.store
            .get(key)
            .await
            .map_err(|e| Box::new(e) as KVError)
    }

    fn name(&self) -> &str {
        self.name
    }

    async fn watch(&self, revision: Option<u64>) -> Result<KVWatchStream, KVError> {
        // Use resilient JetstreamWatcher which handles auto-reconnection on failures
        let watcher = JetstreamWatcher::new(self.store.clone(), revision)
            .await
            .map_err(|e| Box::new(e) as KVError)?;

        Ok(Box::pin(ResilientJetstreamWatchAdapter { inner: watcher }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "nats-tests")]
    use async_nats::jetstream;

    /// Helper to create a test KV bucket with a unique name
    #[cfg(feature = "nats-tests")]
    async fn setup_test_kv(bucket_name: &str) -> (jetstream::Context, Store) {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js = jetstream::new(client);

        // Delete if exists to ensure clean state
        let _ = js.delete_key_value(bucket_name).await;

        let store = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: bucket_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        (js, store)
    }

    /// Helper to cleanup test KV bucket
    #[cfg(feature = "nats-tests")]
    async fn cleanup_test_kv(js: &jetstream::Context, bucket_name: &str) {
        let _ = js.delete_key_value(bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_put_and_get() {
        let bucket_name = "test-kv-put-get";
        let (js, store) = setup_test_kv(bucket_name).await;

        let kv_store = JetstreamKVStore::new(store, bucket_name);

        // Test put
        let key = "test-key";
        let value = Bytes::from("test-value");
        kv_store.put(key, value.clone()).await.unwrap();

        // Test get
        let result = kv_store.get(key).await.unwrap();
        assert_eq!(result, Some(value));

        // Test get non-existent key
        let result = kv_store.get("non-existent").await.unwrap();
        assert_eq!(result, None);

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_delete() {
        let bucket_name = "test-kv-delete";
        let (js, store) = setup_test_kv(bucket_name).await;

        let kv_store = JetstreamKVStore::new(store, bucket_name);

        // Put a key
        let key = "delete-me";
        kv_store.put(key, Bytes::from("value")).await.unwrap();

        // Verify it exists
        assert!(kv_store.get(key).await.unwrap().is_some());

        // Delete it
        kv_store.delete(key).await.unwrap();

        // Verify it's gone
        assert!(kv_store.get(key).await.unwrap().is_none());

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_get_all_keys() {
        let bucket_name = "test-kv-all-keys";
        let (js, store) = setup_test_kv(bucket_name).await;

        let kv_store = JetstreamKVStore::new(store, bucket_name);

        // Put multiple keys
        kv_store.put("key1", Bytes::from("value1")).await.unwrap();
        kv_store.put("key2", Bytes::from("value2")).await.unwrap();
        kv_store.put("key3", Bytes::from("value3")).await.unwrap();

        // Get all keys
        let mut keys = kv_store.keys().await.unwrap();
        keys.sort();

        assert_eq!(keys, vec!["key1", "key2", "key3"]);

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_name() {
        let bucket_name = "test-kv-name";
        let (js, store) = setup_test_kv(bucket_name).await;

        let kv_store = JetstreamKVStore::new(store, bucket_name);
        assert_eq!(kv_store.name(), bucket_name);

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_from_context() {
        let bucket_name = "test-kv-from-context";
        let (js, store) = setup_test_kv(bucket_name).await;
        // Create store from context
        let kv_store = JetstreamKVStore::new(store, bucket_name);

        // Verify it works
        kv_store.put("test", Bytes::from("value")).await.unwrap();
        let result = kv_store.get("test").await.unwrap();
        assert_eq!(result, Some(Bytes::from("value")));

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_as_trait_object() {
        let bucket_name = "test-kv-trait-object";
        let (js, store) = setup_test_kv(bucket_name).await;

        // Use as Arc<dyn KVStorer>
        let kv_store = JetstreamKVStore::new(store, bucket_name);

        // Test operations via trait object
        kv_store.put("key", Bytes::from("value")).await.unwrap();
        let result = kv_store.get("key").await.unwrap();
        assert_eq!(result, Some(Bytes::from("value")));

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_watch() {
        let bucket_name = "test-kv-watch";
        let (js, store) = setup_test_kv(bucket_name).await;

        let kv_store = JetstreamKVStore::new(store, bucket_name);

        // Put initial values
        kv_store.put("key1", Bytes::from("value1")).await.unwrap();

        // Start watching from beginning
        let mut watch = kv_store.watch(Some(1)).await.unwrap();

        // Get the first entry (should be the put we just did)
        let entry = watch.next().await.unwrap();
        assert_eq!(entry.key, "key1");
        assert_eq!(entry.value, Bytes::from("value1"));
        assert_eq!(entry.operation, KVWatchOp::Put);

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[test]
    fn test_kv_entry_struct() {
        let entry = KVEntry {
            key: "test-key".to_string(),
            value: Bytes::from("test-value"),
            operation: KVWatchOp::Put,
        };

        assert_eq!(entry.key, "test-key");
        assert_eq!(entry.value, Bytes::from("test-value"));
        assert_eq!(entry.operation, KVWatchOp::Put);
    }
}
