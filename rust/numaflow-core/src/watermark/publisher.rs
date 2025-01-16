use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::ToVertexConfig;
use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::{Error, Result};
use crate::watermark::WMB;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tracing::info;

const DEFAULT_POD_HEARTBEAT_INTERVAL: u16 = 5;

pub(crate) struct Publisher {
    pub(crate) processor_name: String,
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
        js_context: async_nats::jetstream::Context,
        to_vertex_configs: Vec<ToVertexConfig>,
    ) -> Result<Self> {
        let processor_name = format!("{}-{}", get_vertex_name(), get_vertex_replica());

        let mut ot_buckets = HashMap::new();
        let mut hb_buckets = Vec::with_capacity(to_vertex_configs.len());
        let mut last_published_wm = HashMap::new();

        for to_vertex_config in to_vertex_configs {
            let js_context = js_context.clone();
            let ot_bucket = js_context
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: to_vertex_config.watermark_config.clone().unwrap().ot_bucket,
                    ..Default::default()
                })
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            let hb_bucket = js_context
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: to_vertex_config.watermark_config.clone().unwrap().hb_bucket,
                    ..Default::default()
                })
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
        let hb_handle = tokio::spawn(Self::start_heartbeat(processor_name, hb_buckets));

        Ok(Publisher {
            processor_name: String::new(), // Set the processor name as needed
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
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32;
            for hb_bucket in hb_buckets.iter() {
                hb_bucket
                    .put(
                        processor_name.clone(),
                        bytes::Bytes::from(current_time.to_be_bytes().to_vec()),
                    )
                    .await
                    .unwrap();
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
            "Publishing watermark {} for offset {} and stream {}",
            watermark, offset, stream
        );
        // only publish if watermark is greater than last published watermark for this partition and vertex and
        // if the last published watermark is older than 100 milliseconds
        if let Some(last_published_wm) = self.last_published_wm.get_mut(stream.vertex) {
            if watermark > last_published_wm[stream.partition as usize].0
                && offset > last_published_wm[stream.partition as usize].1
                && Utc::now()
                    .signed_duration_since(last_published_wm[stream.partition as usize].2)
                    .num_milliseconds()
                    > 100
            {
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
