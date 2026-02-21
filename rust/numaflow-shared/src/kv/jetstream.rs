//! JetStream implementation of the KV store traits.
//!
//! This module provides a JetStream-backed implementation of [`KVStorer`].

use super::{KVEntry, KVError, KVResult, KVStorer, KVWatchOp, KVWatchStream};
use async_nats::jetstream::kv::{Entry, Store};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

/// JetStream KV Entry implementation.
///
/// Wraps a JetStream entry and implements the [`KVEntry`] trait.
pub struct JetstreamKVEntry {
    key: String,
    value: Bytes,
    operation: KVWatchOp,
}

impl KVEntry for JetstreamKVEntry {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> Bytes {
        self.value.clone()
    }

    fn operation(&self) -> KVWatchOp {
        self.operation.clone()
    }
}

impl From<Entry> for JetstreamKVEntry {
    fn from(entry: Entry) -> Self {
        let operation = match entry.operation {
            async_nats::jetstream::kv::Operation::Put => KVWatchOp::Put,
            async_nats::jetstream::kv::Operation::Delete => KVWatchOp::Delete,
            async_nats::jetstream::kv::Operation::Purge => KVWatchOp::Purge,
        };

        JetstreamKVEntry {
            key: entry.key,
            value: entry.value,
            operation,
        }
    }
}

/// Wrapper stream that converts JetStream watch entries to trait objects.
///
/// This adapts the JetStream `Watch` stream to produce `Box<dyn KVEntry>` items.
struct JetstreamWatchAdapter {
    inner: async_nats::jetstream::kv::Watch,
}

impl Stream for JetstreamWatchAdapter {
    type Item = Box<dyn KVEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(entry))) => Poll::Ready(Some(
                Box::new(JetstreamKVEntry::from(entry)) as Box<dyn KVEntry>,
            )),
            Poll::Ready(Some(Err(_))) => {
                // On error, wake to retry getting next item
                cx.waker().wake_by_ref();
                Poll::Pending
            }
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
    name: String,
}

impl JetstreamKVStore {
    /// Create a new JetstreamKVStore from an existing JetStream KV Store.
    ///
    /// # Arguments
    /// * `store` - The JetStream KV store handle
    /// * `name` - A name/identifier for this store (typically the bucket name)
    pub fn new(store: Store, name: impl Into<String>) -> Self {
        Self {
            store,
            name: name.into(),
        }
    }

    /// Create a JetstreamKVStore from a JetStream context and bucket name.
    ///
    /// # Arguments
    /// * `js_context` - The JetStream context
    /// * `bucket_name` - The name of the KV bucket to use
    pub async fn from_context(
        js_context: &async_nats::jetstream::Context,
        bucket_name: &str,
    ) -> KVResult<Self> {
        let store = js_context
            .get_key_value(bucket_name)
            .await
            .map_err(|e| Box::new(e) as KVError)?;

        Ok(Self::new(store, bucket_name))
    }
}

#[async_trait]
impl KVStorer for JetstreamKVStore {
    async fn get_all_keys(&self) -> KVResult<Vec<String>> {
        self.store
            .keys()
            .await
            .map_err(|e| Box::new(e) as KVError)?
            .try_collect()
            .await
            .map_err(|e| Box::new(e) as KVError)
    }

    async fn delete_key(&self, key: &str) -> KVResult<()> {
        self.store
            .delete(key)
            .await
            .map_err(|e| Box::new(e) as KVError)?;
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> KVResult<()> {
        self.store
            .put(key, value)
            .await
            .map_err(|e| Box::new(e) as KVError)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> KVResult<Option<Bytes>> {
        self.store
            .get(key)
            .await
            .map_err(|e| Box::new(e) as KVError)
    }

    fn store_name(&self) -> &str {
        &self.name
    }

    async fn watch(&self, revision: Option<u64>) -> KVResult<KVWatchStream> {
        let watch = match revision {
            Some(rev) => self.store.watch_all_from_revision(rev).await,
            None => self.store.watch_all().await,
        }
        .map_err(|e| Box::new(e) as KVError)?;

        Ok(Box::pin(JetstreamWatchAdapter { inner: watch }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "nats-tests")]
    use async_nats::jetstream;

    #[cfg(feature = "nats-tests")]
    use std::sync::Arc;

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
        kv_store.delete_key(key).await.unwrap();

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
        let mut keys = kv_store.get_all_keys().await.unwrap();
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
        assert_eq!(kv_store.store_name(), bucket_name);

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_kv_store_from_context() {
        let bucket_name = "test-kv-from-context";
        let (js, _store) = setup_test_kv(bucket_name).await;

        // Create store from context
        let kv_store = JetstreamKVStore::from_context(&js, bucket_name)
            .await
            .unwrap();

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
        let kv_store: Arc<dyn KVStorer> = Arc::new(JetstreamKVStore::new(store, bucket_name));

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
        assert_eq!(entry.key(), "key1");
        assert_eq!(entry.value(), Bytes::from("value1"));
        assert_eq!(entry.operation(), KVWatchOp::Put);

        cleanup_test_kv(&js, bucket_name).await;
    }

    #[test]
    fn test_kv_entry_implementation() {
        let entry = JetstreamKVEntry {
            key: "test-key".to_string(),
            value: Bytes::from("test-value"),
            operation: KVWatchOp::Put,
        };

        assert_eq!(entry.key(), "test-key");
        assert_eq!(entry.value(), Bytes::from("test-value"));
        assert_eq!(entry.operation(), KVWatchOp::Put);
    }

    #[test]
    fn test_kv_watch_op_clone() {
        let op = KVWatchOp::Put;
        assert_eq!(op.clone(), KVWatchOp::Put);

        let op = KVWatchOp::Delete;
        assert_eq!(op.clone(), KVWatchOp::Delete);

        let op = KVWatchOp::Purge;
        assert_eq!(op.clone(), KVWatchOp::Purge);
    }
}
