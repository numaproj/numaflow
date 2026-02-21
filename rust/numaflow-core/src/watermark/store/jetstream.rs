//! JetStream implementation of the WatermarkStore trait.
//!
//! This module provides a JetStream-backed implementation of [`WatermarkStore`].

use crate::watermark::WatermarkStore;
use async_trait::async_trait;
use numaflow_shared::kv::jetstream::JetstreamKVStore;
use numaflow_shared::kv::{KVResult, KVStorer};
use std::sync::Arc;

/// JetStream implementation of WatermarkStore.
///
/// Wraps two JetStream KV stores (heartbeat and offset timeline) and implements
/// [`LocalWatermarkStore`]. Use this as `Arc<dyn WatermarkStore>` for dynamic dispatch.
pub(crate) struct JetstreamWatermarkStore {
    heartbeat: Arc<dyn KVStorer>,
    offset_timeline: Arc<dyn KVStorer>,
}

impl JetstreamWatermarkStore {
    /// Create a new JetstreamWatermarkStore from existing KV stores.
    ///
    /// # Arguments
    /// * `heartbeat` - The heartbeat KV store
    /// * `offset_timeline` - The offset timeline KV store
    pub(crate) fn new(heartbeat: Arc<dyn KVStorer>, offset_timeline: Arc<dyn KVStorer>) -> Self {
        Self {
            heartbeat,
            offset_timeline,
        }
    }

    /// Create a JetstreamWatermarkStore from a JetStream context and bucket names.
    ///
    /// # Arguments
    /// * `js_context` - The JetStream context
    /// * `hb_bucket` - The name of the heartbeat KV bucket
    /// * `ot_bucket` - The name of the offset timeline KV bucket
    pub(crate) async fn from_context(
        js_context: &async_nats::jetstream::Context,
        hb_bucket: &str,
        ot_bucket: &str,
    ) -> KVResult<Self> {
        let hb_store = JetstreamKVStore::from_context(js_context, hb_bucket).await?;
        let ot_store = JetstreamKVStore::from_context(js_context, ot_bucket).await?;

        Ok(Self {
            heartbeat: Arc::new(hb_store),
            offset_timeline: Arc::new(ot_store),
        })
    }

    /// Create a JetstreamWatermarkStore from existing JetStream KV Store handles.
    ///
    /// # Arguments
    /// * `hb_store` - The heartbeat JetStream KV store handle
    /// * `ot_store` - The offset timeline JetStream KV store handle
    /// * `hb_name` - Name for the heartbeat store
    /// * `ot_name` - Name for the offset timeline store
    pub(crate) fn from_stores(
        hb_store: async_nats::jetstream::kv::Store,
        ot_store: async_nats::jetstream::kv::Store,
        hb_name: impl Into<String>,
        ot_name: impl Into<String>,
    ) -> Self {
        Self {
            heartbeat: Arc::new(JetstreamKVStore::new(hb_store, hb_name)),
            offset_timeline: Arc::new(JetstreamKVStore::new(ot_store, ot_name)),
        }
    }
}

#[async_trait]
impl WatermarkStore for JetstreamWatermarkStore {
    fn heartbeat_store(&self) -> Arc<dyn KVStorer> {
        Arc::clone(&self.heartbeat)
    }

    fn offset_timeline_store(&self) -> Arc<dyn KVStorer> {
        Arc::clone(&self.offset_timeline)
    }

    async fn close(&self) -> KVResult<()> {
        // JetStream stores don't require explicit close
        // The underlying NATS connection is managed separately
        Ok(())
    }
}
