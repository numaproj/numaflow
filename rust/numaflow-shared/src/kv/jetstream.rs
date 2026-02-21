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
