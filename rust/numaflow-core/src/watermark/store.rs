//! Watermark store implementations for pluggable storage backends.
//!
//! The [`WatermarkStore`](super::WatermarkStore) trait is defined in the parent module.
//! This module contains concrete implementations.

#![allow(dead_code)]

use numaflow_shared::kv::KVStorer;
use std::sync::Arc;

// FIXME: remove after integration
pub(crate) mod jetstream;

/// WatermarkStore defines a pair of heartbeat KV store and offset timeline KV store.
///
/// In Numaflow's watermark propagation, we use two KV buckets:
/// - **Heartbeat (HB) bucket**: Tracks processor liveness via heartbeat timestamps
/// - **Offset Timeline (OT) bucket**: Stores watermark-offset pairs for each processor
///
/// This trait abstracts the storage backend, allowing different implementations
/// (JetStream, Redis, in-memory for testing, etc.).
///
/// This trait is object-safe and can be used as `Arc<dyn WatermarkStore>` for dynamic dispatch.
#[allow(dead_code)] // FIXME: remove after integration
pub trait WatermarkStore: Send + Sync {
    /// Returns the heartbeat KV store.
    ///
    /// The heartbeat store is used to track processor liveness. Each processor
    /// publishes its heartbeat timestamp periodically, and watchers use this
    /// to detect dead processors.
    fn heartbeat_store(&self) -> Arc<dyn KVStorer>;

    /// Returns the offset timeline KV store.
    ///
    /// The offset timeline store holds watermark-offset pairs for each processor.
    /// This is used to determine the watermark for a given offset by looking up
    /// the timeline entries from all active processors.
    fn offset_timeline_store(&self) -> Arc<dyn KVStorer>;
}
