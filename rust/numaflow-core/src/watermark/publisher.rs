use std::collections::HashMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::error::{Error, Result};
use crate::watermark::WMB;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use prost::Message;
use tokio::time::sleep;
use tracing::info;

const DEFAULT_POD_HEARTBEAT_INTERVAL: u16 = 5;

#[derive(Clone, Debug)]
struct LastPublishedState {
    offset: i64,
    watermark: i64,
    publish_time: DateTime<Utc>,
}

pub(crate) struct EdgePublisher {
    processor_name: String,
    hb_handle: tokio::task::JoinHandle<()>,
    last_published_wm: HashMap<&'static str, Vec<LastPublishedState>>,
    ot_buckets: HashMap<&'static str, async_nats::jetstream::kv::Store>,
}

impl Drop for EdgePublisher {
    fn drop(&mut self) {
        self.hb_handle.abort();
    }
}

impl EdgePublisher {
    pub(crate) async fn new(
        processor_name: String,
        js_context: async_nats::jetstream::Context,
        bucket_configs: &[BucketConfig],
    ) -> Result<Self> {
        let mut ot_buckets = HashMap::new();
        let mut hb_buckets = Vec::with_capacity(bucket_configs.len());
        let mut last_published_wm = HashMap::new();

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
                        publish_time: DateTime::from_timestamp_millis(-1).unwrap()
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
                    .unwrap()
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

    pub(crate) async fn publish_watermark(
        &mut self,
        stream: Stream,
        offset: i64,
        watermark: i64,
    ) -> Result<()> {
        let last_published_wm = match self.last_published_wm.get_mut(stream.vertex) {
            Some(wm) => wm,
            None => return Ok(()),
        };

        let last_state = &last_published_wm[stream.partition as usize];
        if offset <= last_state.offset
            || watermark <= last_state.watermark
            || Utc::now()
                .signed_duration_since(last_state.publish_time)
                .num_milliseconds()
                <= 100
        {
            return Ok(());
        }

        info!(
            "Publishing watermark for processor {} and partition {} and watermark {}",
            stream.vertex, stream.partition, watermark
        );

        let ot_bucket = self.ot_buckets.get(stream.vertex).unwrap();
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

        last_published_wm[stream.partition as usize] = LastPublishedState {
            offset,
            watermark,
            publish_time: Utc::now(),
        };

        Ok(())
    }
}

pub(crate) struct SourcePublisher {
    js_context: async_nats::jetstream::Context,
    source_config: BucketConfig,
    to_vertex_configs: Vec<BucketConfig>,
    publishers: HashMap<String, EdgePublisher>,
}

impl SourcePublisher {
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        source_config: BucketConfig,
        to_vertex_configs: Vec<BucketConfig>,
    ) -> Result<Self> {
        Ok(SourcePublisher {
            js_context,
            source_config,
            to_vertex_configs,
            publishers: HashMap::new(),
        })
    }

    pub(crate) async fn publish_source_watermark(&mut self, partition: u16, watermark: i64) {
        let processor_name = format!("{}-{}", self.source_config.vertex, partition);
        if !self.publishers.contains_key(&processor_name) {
            let publisher = EdgePublisher::new(
                processor_name.clone(),
                self.js_context.clone(),
                &[self.source_config.clone()],
            )
            .await
            .expect("Failed to create publisher");
            info!(
                "Creating new publisher at source publish for processor {} and partition{}",
                processor_name, partition
            );
            self.publishers.insert(processor_name.clone(), publisher);
        }

        self.publishers
            .get_mut(&processor_name)
            .expect("Publisher not found")
            .publish_watermark(
                Stream {
                    name: "source",
                    vertex: self.source_config.vertex,
                    partition,
                },
                Utc::now().timestamp_micros(), // we don't care about the offsets
                watermark,
            )
            .await
            .expect("Failed to publish watermark");
        info!(
            "Updating last published watermark for partition {} to {}",
            partition, watermark
        );
    }

    pub(crate) async fn publish_edge_watermark(
        &mut self,
        input_partition: u16,
        stream: Stream,
        offset: i64,
        watermark: i64,
    ) {
        let processor_name = format!("{}-{}", stream.vertex, input_partition);
        if !self.publishers.contains_key(&processor_name) {
            info!(
                "Creating new publisher for processor at edge publish {} and input partition {}",
                processor_name, input_partition
            );
            let publisher = EdgePublisher::new(
                processor_name.clone(),
                self.js_context.clone(),
                &self.to_vertex_configs,
            )
            .await
            .expect("Failed to create publisher");
            self.publishers.insert(processor_name.clone(), publisher);
        }

        self.publishers
            .get_mut(&processor_name)
            .expect("Publisher not found")
            .publish_watermark(stream, offset, watermark)
            .await
            .expect("Failed to publish watermark");
    }
}
