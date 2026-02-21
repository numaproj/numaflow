//! JetStream implementation of the WatermarkStore trait.
//!
//! This module provides a JetStream-backed implementation of [`WatermarkStore`].

use crate::watermark::store::WatermarkStore;
use async_nats::jetstream;
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
        js_context: &jetstream::Context,
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
        hb_store: jetstream::kv::Store,
        ot_store: jetstream::kv::Store,
        hb_name: impl Into<String>,
        ot_name: impl Into<String>,
    ) -> Self {
        Self {
            heartbeat: Arc::new(JetstreamKVStore::new(hb_store, hb_name)),
            offset_timeline: Arc::new(JetstreamKVStore::new(ot_store, ot_name)),
        }
    }
}

impl WatermarkStore for JetstreamWatermarkStore {
    fn heartbeat_store(&self) -> Arc<dyn KVStorer> {
        Arc::clone(&self.heartbeat)
    }

    fn offset_timeline_store(&self) -> Arc<dyn KVStorer> {
        Arc::clone(&self.offset_timeline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::jetstream;
    use bytes::Bytes;

    /// Helper to create test KV buckets
    async fn setup_test_kv_buckets(
        hb_bucket: &str,
        ot_bucket: &str,
    ) -> (
        jetstream::Context,
        jetstream::kv::Store,
        jetstream::kv::Store,
    ) {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js = jetstream::new(client);

        // Delete if exists to ensure clean state
        let _ = js.delete_key_value(hb_bucket).await;
        let _ = js.delete_key_value(ot_bucket).await;

        let hb_store = js
            .create_key_value(jetstream::kv::Config {
                bucket: hb_bucket.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let ot_store = js
            .create_key_value(jetstream::kv::Config {
                bucket: ot_bucket.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        (js, hb_store, ot_store)
    }

    /// Helper to cleanup test KV buckets
    async fn cleanup_test_kv_buckets(js: &jetstream::Context, hb_bucket: &str, ot_bucket: &str) {
        let _ = js.delete_key_value(hb_bucket).await;
        let _ = js.delete_key_value(ot_bucket).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_watermark_store_from_stores() {
        let hb_bucket = "test-wm-hb-stores";
        let ot_bucket = "test-wm-ot-stores";
        let (js, hb_store, ot_store) = setup_test_kv_buckets(hb_bucket, ot_bucket).await;

        let wm_store =
            JetstreamWatermarkStore::from_stores(hb_store, ot_store, hb_bucket, ot_bucket);

        // Verify store names
        assert_eq!(wm_store.heartbeat_store().store_name(), hb_bucket);
        assert_eq!(wm_store.offset_timeline_store().store_name(), ot_bucket);

        cleanup_test_kv_buckets(&js, hb_bucket, ot_bucket).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_watermark_store_from_context() {
        let hb_bucket = "test-wm-hb-context";
        let ot_bucket = "test-wm-ot-context";
        let (js, _hb_store, _ot_store) = setup_test_kv_buckets(hb_bucket, ot_bucket).await;

        let wm_store = JetstreamWatermarkStore::from_context(&js, hb_bucket, ot_bucket)
            .await
            .unwrap();

        // Verify stores work
        wm_store
            .heartbeat_store()
            .put("processor-1", Bytes::from("12345"))
            .await
            .unwrap();
        wm_store
            .offset_timeline_store()
            .put("processor-1", Bytes::from("67890"))
            .await
            .unwrap();

        // Verify we can read back
        let hb_value = wm_store.heartbeat_store().get("processor-1").await.unwrap();
        assert_eq!(hb_value, Some(Bytes::from("12345")));

        let ot_value = wm_store
            .offset_timeline_store()
            .get("processor-1")
            .await
            .unwrap();
        assert_eq!(ot_value, Some(Bytes::from("67890")));

        cleanup_test_kv_buckets(&js, hb_bucket, ot_bucket).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_watermark_store_new() {
        let hb_bucket = "test-wm-hb-new";
        let ot_bucket = "test-wm-ot-new";
        let (js, hb_store, ot_store) = setup_test_kv_buckets(hb_bucket, ot_bucket).await;

        let hb_kv: Arc<dyn KVStorer> = Arc::new(JetstreamKVStore::new(hb_store, hb_bucket));
        let ot_kv: Arc<dyn KVStorer> = Arc::new(JetstreamKVStore::new(ot_store, ot_bucket));

        let wm_store = JetstreamWatermarkStore::new(hb_kv, ot_kv);

        // Verify stores are accessible
        assert_eq!(wm_store.heartbeat_store().store_name(), hb_bucket);
        assert_eq!(wm_store.offset_timeline_store().store_name(), ot_bucket);

        cleanup_test_kv_buckets(&js, hb_bucket, ot_bucket).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_watermark_store_as_trait_object() {
        let hb_bucket = "test-wm-hb-trait";
        let ot_bucket = "test-wm-ot-trait";
        let (js, hb_store, ot_store) = setup_test_kv_buckets(hb_bucket, ot_bucket).await;

        // Use as Arc<dyn WatermarkStore>
        let wm_store: Arc<dyn WatermarkStore> = Arc::new(JetstreamWatermarkStore::from_stores(
            hb_store, ot_store, hb_bucket, ot_bucket,
        ));

        // Test operations via trait object
        wm_store
            .heartbeat_store()
            .put("test", Bytes::from("data"))
            .await
            .unwrap();

        let result = wm_store.heartbeat_store().get("test").await.unwrap();
        assert_eq!(result, Some(Bytes::from("data")));

        cleanup_test_kv_buckets(&js, hb_bucket, ot_bucket).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_watermark_store_independent_stores() {
        let hb_bucket = "test-wm-hb-indep";
        let ot_bucket = "test-wm-ot-indep";
        let (js, hb_store, ot_store) = setup_test_kv_buckets(hb_bucket, ot_bucket).await;

        let wm_store =
            JetstreamWatermarkStore::from_stores(hb_store, ot_store, hb_bucket, ot_bucket);

        // Put same key in both stores with different values
        wm_store
            .heartbeat_store()
            .put("key1", Bytes::from("heartbeat-value"))
            .await
            .unwrap();
        wm_store
            .offset_timeline_store()
            .put("key1", Bytes::from("timeline-value"))
            .await
            .unwrap();

        // Verify they are independent
        let hb_value = wm_store.heartbeat_store().get("key1").await.unwrap();
        let ot_value = wm_store.offset_timeline_store().get("key1").await.unwrap();

        assert_eq!(hb_value, Some(Bytes::from("heartbeat-value")));
        assert_eq!(ot_value, Some(Bytes::from("timeline-value")));

        cleanup_test_kv_buckets(&js, hb_bucket, ot_bucket).await;
    }
}
