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
//! use numaflow_testing::simplekvstore::SimpleKVStore;
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

