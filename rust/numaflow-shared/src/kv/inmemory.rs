//! Simple KV store implementation for testing.
//!
//! This module provides a simple in-memory KV store that implements the `KVStore` trait
//! from `numaflow_shared::kv`. It allows for error injection to test negative cases
//! in watermark functionality.
//!
//! # Features
//! - Full `KVStore` trait implementation
//! - Error injection for all operations (put, get, delete, keys, watch)
//! - Watch stream with history replay support
//! - Configurable watch stream termination for testing watcher recreation
//! - Latency injection for all operations
//!
//! # Example
//! ```ignore
//! use numaflow_shared::kv::inmemory::SimpleKVStore;
//! use numaflow_shared::kv::KVStore;
//! use bytes::Bytes;
//!
//! let store = SimpleKVStore::new("test-bucket");
//!
//! // Basic operations
//! store.put("key1", Bytes::from("value1")).await.unwrap();
//! let value = store.get("key1").await.unwrap();
//!
//! // Error injection
//! store.error_injector().fail_puts(1);  // Next put will fail
//! assert!(store.put("key2", Bytes::from("value2")).await.is_err());
//!
//! // Watch stream termination (for testing watcher recreation)
//! store.error_injector().close_all_watch_streams();
//! ```

/// Error types for the simple KV store.
mod error;
/// Error injection controller.
mod error_injector;
/// Core store implementation.
mod store;

// Re-exports
pub use error::{Result, SimpleKVStoreError};
pub use error_injector::KVErrorInjector;
pub use store::{KVHistoryEntry, KVState, SimpleKVStore};

use super::{KVStore, KVStoreFactory};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory that produces [`SimpleKVStore`] instances, registered by bucket name.
///
/// Returning the same instance per bucket name is load-bearing: watermark
/// publisher/fetcher must share state.
#[derive(Default)]
pub struct InMemoryKVStoreFactory {
    stores: parking_lot::Mutex<HashMap<String, Arc<SimpleKVStore>>>,
}

impl InMemoryKVStoreFactory {
    /// Create an empty factory.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl KVStoreFactory for InMemoryKVStoreFactory {
    async fn create_kv_store(&self, bucket: String) -> crate::error::Result<Arc<dyn KVStore>> {
        let mut stores = self.stores.lock();
        if let Some(existing) = stores.get(&bucket) {
            return Ok(Arc::clone(existing) as Arc<dyn KVStore>);
        }
        let leaked_name: &'static str = Box::leak(bucket.clone().into_boxed_str());
        let store = Arc::new(SimpleKVStore::new(leaked_name));
        stores.insert(bucket, Arc::clone(&store));
        Ok(store as Arc<dyn KVStore>)
    }
}

#[cfg(test)]
mod factory_tests {
    use super::*;
    use bytes::Bytes;

    /// Same bucket name → shared KV instance.
    #[tokio::test]
    async fn test_in_memory_kv_store_factory_shares_instance_for_same_bucket() {
        let factory = InMemoryKVStoreFactory::new();

        let a1 = factory
            .create_kv_store("b".to_string())
            .await
            .expect("create a1");
        let a2 = factory
            .create_kv_store("b".to_string())
            .await
            .expect("create a2");

        assert!(
            Arc::ptr_eq(&a1, &a2),
            "same bucket name must return the same instance"
        );

        a1.put("k", Bytes::from("v")).await.expect("put");
        assert_eq!(
            a2.get("k").await.expect("get via peer"),
            Some(Bytes::from("v")),
            "same bucket name must share state"
        );
    }

    /// Different bucket names → independent instances.
    #[tokio::test]
    async fn test_in_memory_kv_store_factory_different_buckets_are_independent() {
        let factory = InMemoryKVStoreFactory::new();

        let a = factory
            .create_kv_store("a".to_string())
            .await
            .expect("create a");
        let b = factory
            .create_kv_store("b".to_string())
            .await
            .expect("create b");

        assert!(
            !Arc::ptr_eq(&a, &b),
            "different bucket names must return different instances"
        );

        a.put("k", Bytes::from("v")).await.expect("put");
        assert_eq!(
            b.get("k").await.expect("get other"),
            None,
            "different bucket names must be independent"
        );
    }
}
