use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::ToVertexConfig;
use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::{Error, Result};
use crate::watermark::{Watermark, WMB};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::HashMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tracing::info;

const DEFAULT_POD_HEARTBEAT_INTERVAL: u16 = 5;

pub(crate) struct Publisher {
    processor_name: String,
    hb_handle: tokio::task::JoinHandle<()>,
    last_published_wm: HashMap<String, Vec<(i64, i64, DateTime<Utc>)>>,
    ot_buckets: HashMap<String, async_nats::jetstream::kv::Store>,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.hb_handle.abort();
    }
}

impl Publisher {
    pub(crate) async fn new(
        processor_name: String,
        js_context: async_nats::jetstream::Context,
        to_vertex_configs: Vec<ToVertexConfig>,
    ) -> Result<Self> {
        let mut ot_buckets = HashMap::new();
        let mut hb_buckets = Vec::with_capacity(to_vertex_configs.len());
        let mut last_published_wm = HashMap::new();

        for to_vertex_config in to_vertex_configs {
            let js_context = js_context.clone();
            let ot_bucket = js_context
                .get_key_value(to_vertex_config.watermark_config.clone().unwrap().ot_bucket)
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            let hb_bucket = js_context
                .get_key_value(to_vertex_config.watermark_config.clone().unwrap().hb_bucket)
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            ot_buckets.insert(to_vertex_config.name.clone(), ot_bucket);
            hb_buckets.push(hb_bucket);
            last_published_wm.insert(
                to_vertex_config.name.clone(),
                vec![(-1, -1, Utc::now()); to_vertex_config.writer_config.partitions as usize],
            );
        }

        // start publishing heartbeats
        let hb_handle = tokio::spawn(Self::start_heartbeat(processor_name.clone(), hb_buckets));

        Ok(Publisher {
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
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            DEFAULT_POD_HEARTBEAT_INTERVAL as u64,
        ));

        loop {
            interval.tick().await;
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
        info!(
            "Received watermark for stream {}, partition {}, offset {}, watermark {}",
            stream.vertex, stream.partition, offset, watermark
        );

        info!(
            "Current state of last_published_wm: {:?}",
            self.last_published_wm
        );
        // only publish if watermark is greater than last published watermark for this partition and vertex and
        // if the last published watermark is older than 100 milliseconds
        if let Some(last_published_wm) = self.last_published_wm.get_mut(stream.vertex) {
            if offset > last_published_wm[stream.partition as usize].0
                && watermark > last_published_wm[stream.partition as usize].1
                && Utc::now()
                    .signed_duration_since(last_published_wm[stream.partition as usize].2)
                    .num_milliseconds()
                    > 100
            {
                info!(
                    "Publishing watermark for stream {}, partition {}, offset {}, watermark {}",
                    stream.vertex, stream.partition, offset, watermark
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
                last_published_wm[stream.partition as usize] = (offset, watermark, Utc::now());
            }
        }
        Ok(())
    }
}

pub(crate) struct SourcePublisher {
    to_vertex_configs: Vec<ToVertexConfig>,
    publishers: HashMap<u16, Publisher>,
}

impl SourcePublisher {
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        to_vertex_configs: Vec<ToVertexConfig>,
    ) -> Result<Self> {
        Ok(SourcePublisher {
            to_vertex_configs,
            publishers: HashMap::new(),
        })
    }

    // partition will be considered as a processor here, so we need to create
    // a new publisher for each partition, and we need to keep track of the
    // publishers in a hashmap
    pub(crate) async fn publish_source_watermark(&mut self, partition: u16, watermark: Watermark) {
        // check if the publisher for this partition exists
        // if let Some(publisher) = self.publishers.get_mut(&partition) {
        //     publisher
        //         .publish_watermark(partition, watermark)
        //         .await
        //         .expect("Failed to publish watermark");
        // } else {
        //     // create a new publisher for this partition
        //     let publisher = Publisher::new(partition, self.to_vertex_configs.clone()).await;
        //     self.publishers.insert(partition, publisher);
        // }
    }
}
