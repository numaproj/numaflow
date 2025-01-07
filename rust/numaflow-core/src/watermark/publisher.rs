use crate::config::pipeline::ToVertexConfig;
use crate::error::{Error, Result};
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const DEFAULT_POD_HEARTBEAT_INTERVAL: u16 = 5;

pub(crate) struct Publisher {
    pub(crate) processor_name: String,
    hb_handle: tokio::task::JoinHandle<()>,
    last_published_wm: i64,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.hb_handle.abort();
    }
}

impl Publisher {
    pub(crate) async fn new(
        processor_name: String,
        to_vertex_configs: Vec<ToVertexConfig>,
        js_context: async_nats::jetstream::Context,
    ) -> Result<Self> {
        let mut ot_buckets = Vec::with_capacity(to_vertex_configs.len());
        let mut hb_buckets = Vec::with_capacity(to_vertex_configs.len());

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

            ot_buckets.push(ot_bucket);
            hb_buckets.push(hb_bucket);
        }

        // start publishing heartbeats
        let hb_handle = tokio::spawn(Self::start_heartbeat(processor_name, hb_buckets));

        Ok(Publisher {
            processor_name: String::new(), // Set the processor name as needed
            hb_handle,
            last_published_wm: -1,
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
        &self,
        partition: u16,
        watermark: i64,
        to_vertex_name: String,
    ) -> Result<()> {
        if watermark <= self.last_published_wm {
            return Ok(());
        }

        Ok(())
    }
}
