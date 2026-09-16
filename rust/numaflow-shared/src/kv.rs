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
use std::sync::Arc;

/// In-memory implementation — for local testing only.
pub mod inmemory;
pub mod jetstream;

/// Error type for KV operations (boxed for object safety)
pub type KVError = Box<dyn StdError + Send + Sync + 'static>;

/// Operation type for KV watch events
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The time the data was put in the bucket (epoch milliseconds).
    /// This is provided by the KV store (e.g., JetStream) and can be used
    /// for processor liveness detection.
    pub created: i64,
}

/// Type alias for the watch stream.
pub type KVWatchStream = Pin<Box<dyn Stream<Item = KVEntry> + Send>>;

/// Outcome of a compare-and-set [`KVStore::put_if`].
///
/// Under at-least-once delivery a losing writer is an *expected* outcome, not a
/// failure, so a conflict is modelled as a value rather than a [`KVError`]. Only
/// genuine transport/store failures surface as `Err(KVError)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasResult {
    /// The write committed. Carries the new revision of the key.
    Committed(u64),
    /// The precondition did not hold: for a create (`expected_revision == None`)
    /// the key already existed; for an update (`expected_revision == Some(rev)`)
    /// the key's current revision was not `rev`. Nothing was written. Re-read the
    /// key with [`KVStore::get_with_revision`] and retry against the fresh revision.
    Conflict,
}

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

    /// Get the value for a key together with its current revision.
    ///
    /// The revision is the compare-and-set token consumed by [`Self::put_if`].
    ///
    /// # Returns
    /// * `Ok(Some((value, revision)))` - If the key currently holds a value
    /// * `Ok(None)` - If the key does not exist (or its latest entry is a delete/purge)
    async fn get_with_revision(&self, key: &str) -> Result<Option<(Bytes, u64)>, KVError>;

    /// Conditionally write a value using an optimistic compare-and-set on the key's
    /// revision. This is the primitive that makes concurrent, at-least-once writers
    /// safe: only one writer racing on the same revision commits; the rest observe a
    /// [`CasResult::Conflict`] and retry against a fresh revision.
    ///
    /// # Arguments
    /// * `key` - The key to write
    /// * `value` - The value to store
    /// * `expected_revision` -
    ///   * `None` — *create*: commit only if the key does not yet exist.
    ///   * `Some(rev)` — *update*: commit only if the key's current revision is exactly `rev`
    ///     (as returned by [`Self::get_with_revision`]).
    ///
    /// # Returns
    /// * `Ok(CasResult::Committed(new_revision))` - The write was applied
    /// * `Ok(CasResult::Conflict)` - The precondition did not hold; nothing was written
    /// * `Err(KVError)` - A genuine store/transport failure
    async fn put_if(
        &self,
        key: &str,
        value: Bytes,
        expected_revision: Option<u64>,
    ) -> Result<CasResult, KVError>;

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

/// Creates KV stores for a backend. Implementations own whatever connection or
/// state is needed (e.g. a JetStream `Context`) and are shared as `Arc`.
#[async_trait]
pub trait KVStoreFactory: Send + Sync {
    /// Returns a KV store for `bucket`.
    async fn create_kv_store(&self, bucket: String) -> crate::error::Result<Arc<dyn KVStore>>;
}
