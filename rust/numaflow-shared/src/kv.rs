//! Generic Key-Value store traits for pluggable storage backends.
//!
//! This module provides a generic KV store abstraction that can be implemented
//! by different storage backends (JetStream, Redis, etc.). The design is intentionally
//! free of watermark-specific concerns to allow reuse for other KV storage needs.
//!
//! The traits use `async_trait` to enable object safety, allowing usage as
//! `Arc<dyn KVStorer>` for dynamic dispatch.

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::error::Error as StdError;
use std::pin::Pin;

pub mod jetstream;

/// Error type for KV operations (boxed for object safety)
pub type KVError = Box<dyn StdError + Send + Sync + 'static>;

/// Operation type for KV watch events
#[derive(Debug, PartialEq, Eq)]
pub enum KVWatchOp {
    /// An element has been put/added into the KV store
    Put,
    /// An element has been deleted
    Delete,
    /// The KV bucket has been purged
    Purge,
}

/// A KV entry returned by watch operations.
///
/// Represents a single entry from a KV watch stream containing the key,
/// value, and the operation that triggered this entry.
#[derive(Debug)]
pub struct KVEntry {
    /// The key that was retrieved
    pub key: String,
    /// The retrieved value
    pub value: Bytes,
    /// The operation that triggered this entry
    pub operation: KVWatchOp,
}

/// Type alias for the watch stream.
pub type KVWatchStream = Pin<Box<dyn Stream<Item = KVEntry> + Send>>;

/// KVStor defines a generic key-value store interface.
/// It provides basic CRUD operations plus a watch capability
/// for observing changes to the store.
///
/// This trait is object-safe and can be used as `Arc<dyn KVStorer>` for dynamic dispatch.
#[async_trait]
pub trait KVStore: Send + Sync {
    /// Get all keys from the KV store.
    ///
    /// Returns a vector of all keys currently in the store.
    async fn keys(&self) -> Result<Vec<String>, KVError>;

    /// Delete a key from the KV store.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    async fn delete(&self, key: &str) -> Result<(), KVError>;

    /// Insert or update a key-value pair.
    ///
    /// # Arguments
    /// * `key` - The key to insert or update
    /// * `value` - The value to store
    async fn put(&self, key: &str, value: Bytes) -> Result<(), KVError>;

    /// Get the value for a given key.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(value))` - If the key exists
    /// * `Ok(None)` - If the key does not exist
    async fn get(&self, key: &str) -> Result<Option<Bytes>, KVError>;

    /// Get the store name/identifier.
    ///
    /// This is typically the bucket or collection name.
    fn name(&self) -> &str;

    /// Watch for changes in the KV store.
    ///
    /// Returns a stream of `KVEntry` changes. The stream will emit entries
    /// for all Put, Delete, and Purge operations on the store.
    ///
    /// # Arguments
    /// * `revision` - If `Some`, watches from that revision (inclusive).
    ///                If `None`, watches only new changes from this point forward.
    async fn watch(&self, revision: Option<u64>) -> Result<KVWatchStream, KVError>;
}
