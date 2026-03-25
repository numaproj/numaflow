//! Publishes watermark of the messages written to ISB. Each publisher is mapped to a processing entity
//! which could be a pod or a partition. It publishes watermark to the appropriate OT bucket based on
//! stream information provided. It makes sure we always publish monotonically increasing watermark.
//!
//! Processor liveness is tracked via the KV store's entry creation timestamp, eliminating the need
//! for a separate heartbeat store. The publisher ensures that WMBs are published periodically (based
//! on the configured delay) even when the watermark hasn't changed, to maintain processor liveness detection.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use numaflow_shared::kv::KVStore;
use tracing::{info, warn};

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::watermark::wmb::WMB;

/// LastPublishedState tracks the best known watermark and offset for a partition,
/// and when we last published to the KV store.
#[derive(Clone, Debug)]
struct LastPublishedState {
    /// Best (highest) offset seen so far
    offset: i64,
    /// Best (highest) watermark seen so far
    watermark: i64,
    /// When we last published to the KV store
    last_published_time: Instant,
    /// Configured delay between publishes (to reduce KV writes)
    delay: Option<Duration>,
}

impl Default for LastPublishedState {
    fn default() -> Self {
        LastPublishedState {
            offset: -1,
            watermark: -1,
            last_published_time: Instant::now(),
            delay: None,
        }
    }
}

impl LastPublishedState {
    /// Returns true if enough time has passed since last publish (delay crossed).
    /// If no delay is configured, always returns true.
    fn delay_crossed(&self) -> bool {
        match self.delay {
            Some(delay) => self.last_published_time.elapsed() >= delay,
            None => true,
        }
    }

    /// Updates the tracked state with incoming values, keeping the highest values.
    /// Returns (offset, watermark, regressed) where regressed is true if watermark regression was detected.
    fn update(&mut self, offset: i64, watermark: i64) -> (i64, i64, bool) {
        if offset > self.offset {
            // we cannot have a lower watermark for a higher offset(that means watermark has regressed)
            let regressed = watermark != -1 && self.watermark > watermark;
            self.offset = offset;
            self.watermark = self.watermark.max(watermark);
            (self.offset, self.watermark, regressed)
        } else {
            self.watermark = self.watermark.max(watermark);
            (self.offset, self.watermark, false)
        }
    }

    /// Marks that we just published.
    fn mark_published(&mut self) {
        self.last_published_time = Instant::now();
    }
}

/// ISBWatermarkPublisher is the watermark publisher for the outgoing edges.
/// Processor liveness is tracked via the KV store's entry creation timestamp.
pub(crate) struct ISBWatermarkPublisher {
    /// name of the processor(node) that is publishing the watermark.
    processor_name: String,
    /// last published watermark for each vertex and partition.
    last_published_wm: HashMap<&'static str, HashMap<u16, LastPublishedState>>,
    /// map of vertex to its ot bucket (using generic KVStore trait).
    ot_stores: HashMap<&'static str, Arc<dyn KVStore>>,
    /// whether this publisher is for a source vertex (data can be out of order).
    is_source: bool,
}

impl ISBWatermarkPublisher {
    /// Creates a new ISBWatermarkPublisher.
    ///
    /// # Arguments
    /// * `processor_name` - Name of the processor publishing watermarks
    /// * `ot_stores` - Map of vertex name to its OT (offset-timeline) KV store
    /// * `bucket_configs` - Configuration for each bucket (used to initialize partition state)
    /// * `is_source` - If true, watermark regression warnings will be suppressed since
    ///   source data can be out of order.
    pub(crate) fn new(
        processor_name: String,
        ot_stores: HashMap<&'static str, Arc<dyn KVStore>>,
        bucket_configs: &[BucketConfig],
        is_source: bool,
    ) -> Self {
        let mut last_published_wm = HashMap::new();

        // Initialize partition state from bucket configs
        for config in bucket_configs {
            let partition_state = HashMap::from_iter(config.partitions.iter().map(|partition| {
                (
                    *partition,
                    LastPublishedState {
                        delay: config.delay,
                        ..Default::default()
                    },
                )
            }));

            last_published_wm.insert(config.vertex, partition_state);
        }

        info!(processor = ?processor_name, "Created ISBWatermarkPublisher with embedded heartbeat");

        ISBWatermarkPublisher {
            processor_name,
            last_published_wm,
            ot_stores,
            is_source,
        }
    }

    /// publish_watermark publishes the watermark for the given offset and the stream.
    /// Processor liveness is tracked via the KV store's entry creation timestamp.
    pub(crate) async fn publish_watermark(
        &mut self,
        stream: &Stream,
        offset: i64,
        watermark: i64,
        idle: bool,
    ) {
        self.publish_watermark_with_processor_count(stream, offset, watermark, idle, None)
            .await;
    }

    /// publish_watermark_with_processor_count publishes the watermark with an optional processor count.
    /// This is used by source watermark publishers to include the total partition count in the WMB.
    ///
    /// The logic is simple:
    /// 1. Always update state with best (highest) watermark and offset
    /// 2. Only publish when delay is crossed
    /// 3. Publish the best values (guaranteed monotonically increasing)
    pub(crate) async fn publish_watermark_with_processor_count(
        &mut self,
        stream: &Stream,
        offset: i64,
        watermark: i64,
        idle: bool,
        processor_count: Option<u32>,
    ) {
        let last_state = self
            .last_published_wm
            .get_mut(stream.vertex)
            .expect("Invalid vertex, no last published watermark state found")
            .get_mut(&stream.partition)
            .expect("should have partition");

        // Update state with incoming values, get the best (highest) values to publish.
        // This handles out-of-order data - we always track the best watermark and offset.
        let (publish_offset, publish_watermark, regressed) = last_state.update(offset, watermark);

        // Log warning for watermark regression (only for non-source, since source data can be out of order)
        if regressed && !self.is_source {
            warn!(
                incoming_watermark = watermark,
                last_watermark = publish_watermark,
                publish_offset = publish_offset,
                "Watermark regression detected, using last known watermark"
            );
        }

        // Only publish when delay is crossed
        if !last_state.delay_crossed() {
            return;
        }

        let ot_store = self.ot_stores.get(stream.vertex).expect("Invalid vertex");
        let wmb_bytes: BytesMut = WMB {
            idle,
            offset: publish_offset,
            watermark: publish_watermark,
            partition: stream.partition,
            processor_count,
        }
        .try_into()
        .expect("Failed to convert WMB to bytes");

        // OT writes can fail when ISB is not healthy, we can ignore failures
        // since subsequent writes will go through
        ot_store
            .put(&self.processor_name, wmb_bytes.freeze())
            .await
            .map_err(|e| warn!(?e, "Failed to write wmb to ot store (ignoring)"))
            .ok();

        last_state.mark_published();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;

    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::watermark::BucketConfig;
    use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;
    use crate::watermark::wmb::WMB;
    use numaflow_shared::kv::KVStore;
    use numaflow_shared::kv::jetstream::JetstreamKVStore;

    /// Helper to create OT KV stores from bucket configs for testing
    #[cfg(feature = "nats-tests")]
    async fn create_test_ot_stores(
        js_context: &async_nats::jetstream::Context,
        bucket_configs: &[BucketConfig],
    ) -> HashMap<&'static str, Arc<dyn KVStore>> {
        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();

        for config in bucket_configs {
            let ot_bucket = js_context
                .get_key_value(config.ot_bucket)
                .await
                .expect("Failed to get OT bucket");
            ot_stores.insert(
                config.vertex,
                Arc::new(JetstreamKVStore::new(ot_bucket, config.ot_bucket)),
            );
        }

        ot_stores
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_isb_publisher_one_edge() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "isb_publisher_one_edge_OT";

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0, 1],
            ot_bucket: ot_bucket_name,
            delay: None,
        }];

        // create key value stores
        js_context
            .create_key_value(Config {
                bucket: ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let ot_stores = create_test_ot_stores(&js_context, &bucket_configs).await;

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream_partition_0 = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        let stream_partition_1 = Stream {
            name: "v1-1",
            vertex: "v1",
            partition: 1,
        };

        // Publish watermark for partition 0
        publisher
            .publish_watermark(&stream_partition_0, 1, 100, false)
            .await;

        let ot_bucket = js_context
            .get_key_value("isb_publisher_one_edge_OT")
            .await
            .expect("Failed to get ot bucket");

        let wmb = ot_bucket
            .get("processor1")
            .await
            .expect("Failed to get wmb");
        assert!(wmb.is_some());

        let wmb: WMB = wmb.unwrap().try_into().unwrap();
        assert_eq!(wmb.offset, 1);
        assert_eq!(wmb.watermark, 100);

        // Try publishing a smaller watermark for the same partition, it should not be published
        publisher
            .publish_watermark(&stream_partition_0, 0, 50, false)
            .await;

        let wmb = ot_bucket
            .get("processor1")
            .await
            .expect("Failed to get wmb");
        assert!(wmb.is_some());

        let wmb: WMB = wmb.unwrap().try_into().unwrap();
        assert_eq!(wmb.offset, 1);
        assert_eq!(wmb.watermark, 100);

        // Publish a smaller watermark for a different partition, it should be published
        publisher
            .publish_watermark(&stream_partition_1, 0, 50, false)
            .await;

        let wmb = ot_bucket
            .get("processor1")
            .await
            .expect("Failed to get wmb");
        assert!(wmb.is_some());

        let wmb: WMB = wmb.unwrap().try_into().unwrap();
        assert_eq!(wmb.offset, 0);
        assert_eq!(wmb.watermark, 50);

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_isb_publisher_multi_edges() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name_v1 = "isb_publisher_multi_edges_v1_OT";
        let ot_bucket_name_v2 = "isb_publisher_multi_edges_v2_OT";

        let bucket_configs = vec![
            BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: ot_bucket_name_v1,
                delay: None,
            },
            BucketConfig {
                vertex: "v2",
                partitions: vec![0],
                ot_bucket: ot_bucket_name_v2,
                delay: None,
            },
        ];

        // create key value stores for v1
        js_context
            .create_key_value(Config {
                bucket: ot_bucket_name_v1.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // create key value stores for v2
        js_context
            .create_key_value(Config {
                bucket: ot_bucket_name_v2.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let ot_stores = create_test_ot_stores(&js_context, &bucket_configs).await;

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream1 = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        let stream2 = Stream {
            name: "v2-0",
            vertex: "v2",
            partition: 0,
        };

        publisher.publish_watermark(&stream1, 1, 100, false).await;

        publisher.publish_watermark(&stream2, 1, 200, false).await;

        let ot_bucket_v1 = js_context
            .get_key_value(ot_bucket_name_v1)
            .await
            .expect("Failed to get ot bucket for v1");

        let ot_bucket_v2 = js_context
            .get_key_value(ot_bucket_name_v2)
            .await
            .expect("Failed to get ot bucket for v2");

        let wmb_v1 = ot_bucket_v1
            .get("processor1")
            .await
            .expect("Failed to get wmb for v1");
        assert!(wmb_v1.is_some());

        let wmb_v1: WMB = wmb_v1.unwrap().try_into().unwrap();
        assert_eq!(wmb_v1.offset, 1);
        assert_eq!(wmb_v1.watermark, 100);

        let wmb_v2 = ot_bucket_v2
            .get("processor1")
            .await
            .expect("Failed to get wmb for v2");
        assert!(wmb_v2.is_some());

        let wmb_v2: WMB = wmb_v2.unwrap().try_into().unwrap();
        assert_eq!(wmb_v2.offset, 1);
        assert_eq!(wmb_v2.watermark, 200);

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name_v1.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(ot_bucket_name_v2.to_string())
            .await
            .unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_isb_publisher_idle_flag() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "isb_publisher_idle_flag_OT";

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            delay: None,
        }];

        // create key value stores
        js_context
            .create_key_value(Config {
                bucket: ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let ot_stores = create_test_ot_stores(&js_context, &bucket_configs).await;

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        // Publish watermark with idle flag set to true
        publisher.publish_watermark(&stream, 1, 100, true).await;

        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let wmb = ot_bucket
            .get("processor1")
            .await
            .expect("Failed to get wmb");
        assert!(wmb.is_some());

        let wmb: WMB = wmb.unwrap().try_into().unwrap();
        assert_eq!(wmb.offset, 1);
        assert_eq!(wmb.watermark, 100);
        assert!(wmb.idle);

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
    }
}
