//! Publishes watermark of the messages written to ISB. Each publisher is mapped to a processing entity
//! which could be a pod or a partition. It publishes watermark to the appropriate OT bucket based on
//! stream information provided. It makes sure we always publish monotonically increasing watermark.
//!
//! Heartbeat is now embedded in the WMB (hb_time field), eliminating the need for a separate
//! heartbeat store. The publisher ensures that WMBs are published periodically (based on the
//! configured delay) even when the watermark hasn't changed, to maintain processor liveness detection.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::time::{Instant, UNIX_EPOCH};

use bytes::BytesMut;
use numaflow_shared::kv::KVStore;
use tracing::{info, warn};

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::watermark::wmb::WMB;

/// LastPublishedState is the state of the last published watermark and offset
/// for a partition.
#[derive(Clone, Debug)]
struct LastPublishedState {
    offset: i64,
    watermark: i64,
    last_published_time: Instant,
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
    fn should_publish(&self) -> bool {
        if let Some(delay) = self.delay
            && self.last_published_time.elapsed() < delay
        {
            return false;
        }
        true
    }

    /// Check if heartbeat interval has elapsed and we need to publish for liveness.
    /// Uses the configured delay as the heartbeat interval.
    fn needs_heartbeat(&self) -> bool {
        if let Some(delay) = self.delay {
            self.last_published_time.elapsed() >= delay
        } else {
            // If no delay is configured, always allow publishing
            true
        }
    }
}

/// ISBWatermarkPublisher is the watermark publisher for the outgoing edges.
/// Heartbeat is now embedded in WMB (hb_time field), so no separate heartbeat publishing is needed.
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

    /// Returns the current epoch time in milliseconds for hb_time.
    fn current_hb_time() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to get duration since epoch")
            .as_millis() as i64
    }

    /// publish_watermark publishes the watermark for the given offset and the stream.
    /// The WMB now includes hb_time for processor liveness tracking.
    pub(crate) async fn publish_watermark(
        &mut self,
        stream: &Stream,
        offset: i64,
        watermark: i64,
        idle: bool,
    ) {
        let last_state = self
            .last_published_wm
            .get_mut(stream.vertex)
            .expect("Invalid vertex, no last published watermark state found")
            .get_mut(&stream.partition)
            .expect("should have partition");

        // we can avoid publishing the watermark if the offset is smaller than the last published offset
        // since we do unordered writes to ISB, the offsets can be out of order even though the watermark
        // is monotonically increasing.
        // NOTE: in idling case since we reuse the control message offset, we can have the same offset
        // with larger watermark (we should publish it).
        if offset < last_state.offset {
            last_state.watermark = last_state.watermark.max(watermark);
            return;
        }

        // Check if we need to publish for heartbeat even if watermark hasn't changed.
        // This ensures processor liveness is maintained.
        let needs_heartbeat = last_state.needs_heartbeat();

        // If the watermark is same as the last published watermark update the last published offset
        // to the largest offset otherwise the watermark will regress between the offsets.
        //
        // Example of the bug:
        // Supposed publish watermark offset=3605646 watermark=1750758997480 last_published_offset=3605147 last_published_watermark=1750758997480
        // Supposed publish watermark offset=3605637 watermark=1750758998480 last_published_offset=3605147 last_published_watermark=1750758997480
        // Actual published watermark offset=3605637 watermark=1750758998480
        // We should've published watermark for offset 3605646 and skipped publishing for offset 3605637
        // if watermark cannot be computed, still we should publish the last known valid WM for the latest offset
        if (watermark == last_state.watermark || watermark == -1) && !needs_heartbeat {
            last_state.offset = last_state.offset.max(offset);
            return;
        }

        // Skip publishing if watermark regression is detected (unless we need heartbeat).
        // For source publishers, we silently skip without logging since data can be out of order.
        if watermark < last_state.watermark && !needs_heartbeat {
            if !self.is_source {
                warn!(?watermark, ?last_state.watermark, "Watermark regression detected, skipping publish");
            }
            return;
        }

        // Determine the watermark and offset to publish
        let (publish_watermark, publish_offset) = if watermark > last_state.watermark {
            // New watermark is higher, use it
            (watermark, offset)
        } else {
            // Publishing for heartbeat with same or lower watermark, use last known good values
            (last_state.watermark, last_state.offset.max(offset))
        };

        // valid offset and watermark, we can update the state
        last_state.offset = publish_offset;
        if publish_watermark > last_state.watermark {
            last_state.watermark = publish_watermark;
        }

        // Update state but skip publishing if delay hasn't passed (unless we need heartbeat)
        // (users can configure delay to reduce the number of writes to the ot bucket)
        if !last_state.should_publish() && !needs_heartbeat {
            return;
        }

        // Publish the watermark to the OT store with embedded hb_time
        let ot_store = self.ot_stores.get(stream.vertex).expect("Invalid vertex");
        let wmb_bytes: BytesMut = WMB {
            idle,
            offset: publish_offset,
            watermark: publish_watermark,
            partition: stream.partition,
            hb_time: Self::current_hb_time(),
        }
        .try_into()
        .expect("Failed to convert WMB to bytes");

        // ot writes can fail when isb is not healthy, we can ignore failures
        // since subsequent writes will go through
        ot_store
            .put(&self.processor_name, wmb_bytes.freeze())
            .await
            .map_err(|e| warn!(?e, "Failed to write wmb to ot store (ignoring)"))
            .ok();

        // reset the last published time
        last_state.last_published_time = Instant::now();
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

        let mut publisher = ISBWatermarkPublisher::new(
            "processor1".to_string(),
            ot_stores,
            &bucket_configs,
            false,
        );

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
        // Verify hb_time is set
        assert!(wmb.hb_time > 0);

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

        let mut publisher = ISBWatermarkPublisher::new(
            "processor1".to_string(),
            ot_stores,
            &bucket_configs,
            false,
        );

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
        // Verify hb_time is set
        assert!(wmb_v1.hb_time > 0);

        let wmb_v2 = ot_bucket_v2
            .get("processor1")
            .await
            .expect("Failed to get wmb for v2");
        assert!(wmb_v2.is_some());

        let wmb_v2: WMB = wmb_v2.unwrap().try_into().unwrap();
        assert_eq!(wmb_v2.offset, 1);
        assert_eq!(wmb_v2.watermark, 200);
        // Verify hb_time is set
        assert!(wmb_v2.hb_time > 0);

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

        let mut publisher = ISBWatermarkPublisher::new(
            "processor1".to_string(),
            ot_stores,
            &bucket_configs,
            false,
        );

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
        // Verify hb_time is set
        assert!(wmb.hb_time > 0);

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
    }
}
