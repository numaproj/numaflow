//! Publishes watermark of the messages written to ISB. Each publisher is mapped to a processing entity
//! which could be a pod or a partition, it also creates a background task to publish heartbeats for the
//! downstream vertices, to indicate the liveliness of the processor. It publishes watermark to the
//! appropriate OT bucket based on stream information provided. It makes sure we always publish m
//! increasing watermark.
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use std::time::{Instant, UNIX_EPOCH};

use bytes::BytesMut;
use prost::Message;
use tracing::{debug, error, info, warn};

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::error::{Error, Result};
use crate::watermark::wmb::WMB;

/// Interval at which the pod sends heartbeats.
const DEFAULT_POD_HEARTBEAT_INTERVAL: u16 = 5;

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
}

/// ISBWatermarkPublisher is the watermark publisher for the outgoing edges.
pub(crate) struct ISBWatermarkPublisher {
    /// name of the processor(node) that is publishing the watermark.
    processor_name: String,
    /// handle to the heartbeat publishing task.
    hb_handle: tokio::task::JoinHandle<()>,
    /// last published watermark for each vertex and partition.
    last_published_wm: HashMap<&'static str, HashMap<u16, LastPublishedState>>,
    /// map of vertex to its ot bucket.
    ot_buckets: HashMap<&'static str, async_nats::jetstream::kv::Store>,
}

impl Drop for ISBWatermarkPublisher {
    fn drop(&mut self) {
        self.hb_handle.abort();
    }
}

impl ISBWatermarkPublisher {
    /// Creates a new ISBWatermarkPublisher.
    pub(crate) async fn new(
        processor_name: String,
        js_context: async_nats::jetstream::Context,
        bucket_configs: &[BucketConfig],
    ) -> Result<Self> {
        let mut ot_buckets = HashMap::new();
        let mut hb_buckets = Vec::with_capacity(bucket_configs.len());
        let mut last_published_wm = HashMap::new();

        // create ot and hb buckets
        for config in bucket_configs {
            let js_context = js_context.clone();
            let ot_bucket = js_context
                .get_key_value(config.ot_bucket)
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            let hb_bucket = js_context
                .get_key_value(config.hb_bucket)
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            ot_buckets.insert(config.vertex, ot_bucket);
            hb_buckets.push(hb_bucket);

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

        // start publishing heartbeats
        let hb_handle = tokio::spawn(Self::start_heartbeat(processor_name.clone(), hb_buckets));

        Ok(ISBWatermarkPublisher {
            processor_name,
            hb_handle,
            last_published_wm,
            ot_buckets,
        })
    }

    /// start_heartbeat starts publishing heartbeats to the hb buckets
    async fn start_heartbeat(
        processor_name: String,
        hb_buckets: Vec<async_nats::jetstream::kv::Store>,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(DEFAULT_POD_HEARTBEAT_INTERVAL as u64));
        info!(processor = ?processor_name, "Started publishing heartbeat");
        loop {
            interval.tick().await;
            let heartbeat = numaflow_pb::objects::watermark::Heartbeat {
                heartbeat: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to get duration since epoch")
                    .as_secs() as i64,
            };

            let mut bytes = BytesMut::new();
            heartbeat
                .encode(&mut bytes)
                .expect("Failed to encode heartbeat");

            for hb_bucket in hb_buckets.iter() {
                debug!(heartbeat = ?heartbeat.heartbeat, processor = ?processor_name,
                    "Publishing heartbeat",
                );
                hb_bucket
                    .put(processor_name.clone(), bytes.clone().freeze())
                    .await
                    .map_err(|e| error!(?e, "Failed to write heartbeat to hb bucket"))
                    .ok();
            }
        }
    }

    /// publish_watermark publishes the watermark for the given offset and the stream.
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

        // If the watermark is same as the last published watermark update the last published offset
        // to the largest offset otherwise the watermark will regress between the offsets.
        //
        // Example of the bug:
        // Supposed publish watermark offset=3605646 watermark=1750758997480 last_published_offset=3605147 last_published_watermark=1750758997480
        // Supposed publish watermark offset=3605637 watermark=1750758998480 last_published_offset=3605147 last_published_watermark=1750758997480
        // Actual published watermark offset=3605637 watermark=1750758998480
        // We should've published watermark for offset 3605646 and skipped publishing for offset 3605637
        // if watermark cannot be computed, still we should publish the last known valid WM for the latest offset
        if watermark == last_state.watermark || watermark == -1 {
            last_state.offset = last_state.offset.max(offset);
            return;
        }

        if watermark < last_state.watermark {
            warn!(?watermark, ?last_state.watermark, "Watermark regression detected, skipping publish");
            return;
        }

        // valid offset and watermark, we can update the state
        last_state.offset = offset;
        last_state.watermark = watermark;

        // Update state but skip publishing if delay hasn't passed
        // (users can configure delay to reduce the number of writes to the ot bucket)
        if !last_state.should_publish() {
            return;
        }

        // Publish the watermark to the OT bucket
        let ot_bucket = self.ot_buckets.get(stream.vertex).expect("Invalid vertex");
        let wmb_bytes: BytesMut = WMB {
            idle,
            offset,
            watermark,
            partition: stream.partition,
        }
        .try_into()
        .expect("Failed to convert WMB to bytes");

        // ot writes can fail when isb is not healthy, we can ignore failures
        // since subsequent writes will go through
        ot_bucket
            .put(self.processor_name.clone(), wmb_bytes.freeze())
            .await
            .map_err(|e| warn!(?e, "Failed to write wmb to ot bucket (ignoring)"))
            .ok();

        // reset the last published time
        last_state.last_published_time = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;

    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::watermark::BucketConfig;
    use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;
    use crate::watermark::wmb::WMB;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_isb_publisher_one_edge() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "isb_publisher_one_edge_OT";
        let hb_bucket_name = "isb_publisher_one_edge_PROCESSORS";

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0, 1],
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
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
        js_context
            .create_key_value(Config {
                bucket: hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut publisher = ISBWatermarkPublisher::new(
            "processor1".to_string(),
            js_context.clone(),
            &bucket_configs,
        )
        .await
        .expect("Failed to create publisher");

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
            .delete_key_value(hb_bucket_name.to_string())
            .await
            .unwrap();
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
        let hb_bucket_name_v1 = "isb_publisher_multi_edges_v1_PROCESSORS";
        let ot_bucket_name_v2 = "isb_publisher_multi_edges_v2_OT";
        let hb_bucket_name_v2 = "isb_publisher_multi_edges_v2_PROCESSORS";

        let bucket_configs = vec![
            BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: ot_bucket_name_v1,
                hb_bucket: hb_bucket_name_v1,
                delay: None,
            },
            BucketConfig {
                vertex: "v2",
                partitions: vec![0],
                ot_bucket: ot_bucket_name_v2,
                hb_bucket: hb_bucket_name_v2,
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
        js_context
            .create_key_value(Config {
                bucket: hb_bucket_name_v1.to_string(),
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
        js_context
            .create_key_value(Config {
                bucket: hb_bucket_name_v2.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut publisher = ISBWatermarkPublisher::new(
            "processor1".to_string(),
            js_context.clone(),
            &bucket_configs,
        )
        .await
        .expect("Failed to create publisher");

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
            .delete_key_value(hb_bucket_name_v1.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(ot_bucket_name_v1.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(hb_bucket_name_v2.to_string())
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
        let hb_bucket_name = "isb_publisher_idle_flag_PROCESSORS";

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
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
        js_context
            .create_key_value(Config {
                bucket: hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut publisher = ISBWatermarkPublisher::new(
            "processor1".to_string(),
            js_context.clone(),
            &bucket_configs,
        )
        .await
        .expect("Failed to create publisher");

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
            .delete_key_value(hb_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
    }
}
