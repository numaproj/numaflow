use std::collections::HashMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::BytesMut;
use chrono::{DateTime, Utc};
use prost::Message;
use tokio::time::sleep;
use tracing::info;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::error::{Error, Result};
use crate::watermark::wmb::WMB;

const DEFAULT_POD_HEARTBEAT_INTERVAL: u16 = 5;

/// LastPublishedState is the state of the last published watermark and offset
/// for a partition.
#[derive(Clone, Debug)]
struct LastPublishedState {
    offset: i64,
    watermark: i64,
    publish_time: DateTime<Utc>,
}

/// EdgePublisher is the watermark publisher for the outgoing edges.
pub(crate) struct EdgePublisher {
    // name of the processor(node) that is publishing the watermark
    processor_name: String,
    // handle to the heartbeat publishing task
    hb_handle: tokio::task::JoinHandle<()>,
    // last published watermark for each vertex and partition
    last_published_wm: HashMap<&'static str, Vec<LastPublishedState>>,
    // map of vertex to its ot bucket
    ot_buckets: HashMap<&'static str, async_nats::jetstream::kv::Store>,
}

impl Drop for EdgePublisher {
    fn drop(&mut self) {
        self.hb_handle.abort();
    }
}

impl EdgePublisher {
    /// Creates a new EdgePublisher.
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
            last_published_wm.insert(
                config.vertex,
                vec![
                    LastPublishedState {
                        offset: -1,
                        watermark: -1,
                        publish_time: DateTime::from_timestamp_millis(-1).expect("Invalid time"),
                    };
                    config.partitions as usize
                ],
            );
        }

        // start publishing heartbeats
        let hb_handle = tokio::spawn(Self::start_heartbeat(processor_name.clone(), hb_buckets));

        Ok(EdgePublisher {
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
        let duration = tokio::time::Duration::from_secs(DEFAULT_POD_HEARTBEAT_INTERVAL as u64);
        info!("Starting heartbeat publishing");
        loop {
            sleep(duration).await;
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
                info!(
                    "Publishing heartbeat {} for processor {}",
                    heartbeat.heartbeat, processor_name
                );
                hb_bucket
                    .put(processor_name.clone(), bytes.clone().freeze())
                    .await
                    .expect("Failed to publish heartbeat");
            }
        }
    }

    /// publish_watermark publishes the watermark for the given offset and the stream.
    pub(crate) async fn publish_watermark(
        &mut self,
        stream: Stream,
        offset: i64,
        watermark: i64,
    ) -> Result<()> {
        let last_published_wm_state = match self.last_published_wm.get_mut(stream.vertex) {
            Some(wm) => wm,
            None => return Err(Error::Watermark("Invalid vertex".to_string())),
        };

        // we can avoid publishing the watermark if it is smaller than the last published watermark
        // or if the last watermark was published in the last 100ms
        let last_state = &last_published_wm_state[stream.partition as usize];
        if offset <= last_state.offset
            || watermark <= last_state.watermark
            || Utc::now()
                .signed_duration_since(last_state.publish_time)
                .num_milliseconds()
                <= 100
        {
            return Ok(());
        }

        let ot_bucket = self.ot_buckets.get(stream.vertex).ok_or(Error::Watermark(
            "Invalid vertex, no ot bucket found".to_string(),
        ))?;

        let wmb_bytes: BytesMut = WMB {
            idle: false,
            offset,
            watermark,
            partition: stream.partition,
        }
        .try_into()
        .map_err(|e| Error::Watermark(format!("{}", e)))?;
        ot_bucket
            .put(self.processor_name.clone(), wmb_bytes.freeze())
            .await
            .map_err(|e| Error::Watermark(e.to_string()))?;

        // update the last published watermark state
        last_published_wm_state[stream.partition as usize] = LastPublishedState {
            offset,
            watermark,
            publish_time: Utc::now(),
        };

        info!(
            "Published watermark: offset={}, stream={:?}, watermark={}",
            offset, stream, watermark
        );
        Ok(())
    }
}
