//! Publishes the watermark for source, since watermark originates at source, we publish and fetch to determine
//! the watermark across the source partitions. Since we write the messages to the ISB, we will also publish
//! the watermark to the ISB. Unlike other vertices we don't use pod as the processing entity for publishing
//! watermark we use the partition(watermark originates here).
use std::collections::HashMap;

use chrono::Utc;
use tracing::info;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::error;
use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;

/// SourcePublisher is the watermark publisher for the source vertex.
pub(crate) struct SourceWatermarkPublisher {
    js_context: async_nats::jetstream::Context,
    source_config: BucketConfig,
    to_vertex_configs: Vec<BucketConfig>,
    publishers: HashMap<String, ISBWatermarkPublisher>,
}

impl SourceWatermarkPublisher {
    /// Creates a new [SourceWatermarkPublisher].
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        source_config: BucketConfig,
        to_vertex_configs: Vec<BucketConfig>,
    ) -> error::Result<Self> {
        Ok(SourceWatermarkPublisher {
            js_context,
            source_config,
            to_vertex_configs,
            publishers: HashMap::new(),
        })
    }

    /// Publishes the source watermark for the input partition. It internally uses edge publisher
    /// with processor set to the input partition and source OT.
    pub(crate) async fn publish_source_watermark(&mut self, partition: u16, watermark: i64) {
        // for source, we do partition-based watermark publishing rather than pod-based, hence
        // the processing entity is the partition itself. We create a publisher for each partition
        // and publish the watermark to it.
        let processor_name = format!("{}-{}", self.source_config.vertex, partition);
        // create a publisher if not exists
        if !self.publishers.contains_key(&processor_name) {
            let publisher = ISBWatermarkPublisher::new(
                processor_name.clone(),
                self.js_context.clone(),
                &[self.source_config.clone()],
            )
            .await
            .expect("Failed to create publisher");
            info!(processor = ?processor_name, partittion = ?partition,
                "Creating new publisher for source"
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
    }

    /// Publishes the ISB watermark for the input partition. It internally uses ISB publisher with
    /// processor set to the input partition and ISB OTs.
    pub(crate) async fn publish_isb_watermark(
        &mut self,
        input_partition: u16,
        stream: Stream,
        offset: i64,
        watermark: i64,
    ) {
        let processor_name = format!(
            "{}-{}-{}",
            self.source_config.vertex, stream.vertex, input_partition
        );
        // In source, since we do partition-based watermark publishing rather than pod-based, we
        // create a publisher for each partition and publish the watermark to it.
        if !self.publishers.contains_key(&processor_name) {
            info!(processor = ?processor_name, partition = ?input_partition,
                "Creating new publisher for ISB"
            );
            let publisher = ISBWatermarkPublisher::new(
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

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;

    use crate::config::pipeline::isb::Stream;
    use crate::watermark::source::source_wm_publisher::{BucketConfig, SourceWatermarkPublisher};
    use crate::watermark::wmb::WMB;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_source_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "source_watermark_OT";
        let hb_bucket_name = "source_watermark_PROCESSORS";

        let source_config = BucketConfig {
            vertex: "source_vertex",
            partitions: 2,
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
        };

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

        let mut source_publisher =
            SourceWatermarkPublisher::new(js_context.clone(), source_config.clone(), vec![])
                .await
                .expect("Failed to create source publisher");

        // Publish source watermark for partition 0
        source_publisher.publish_source_watermark(0, 100).await;

        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let wmb = ot_bucket
            .get("source_vertex-0")
            .await
            .expect("Failed to get wmb");
        assert!(wmb.is_some());

        let wmb: WMB = wmb.unwrap().try_into().unwrap();
        assert_eq!(wmb.watermark, 100);

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
    async fn test_publish_edge_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let source_ot_bucket_name = "source_edge_watermark_source_OT";
        let source_hb_bucket_name = "source_edge_watermark_source_PROCESSORS";
        let edge_ot_bucket_name = "source_edge_watermark_edge_OT";
        let edge_hb_bucket_name = "source_edge_watermark_edge_PROCESSORS";

        let source_config = BucketConfig {
            vertex: "source_vertex",
            partitions: 2,
            ot_bucket: source_ot_bucket_name,
            hb_bucket: source_hb_bucket_name,
        };

        let edge_config = BucketConfig {
            vertex: "edge_vertex",
            partitions: 2,
            ot_bucket: edge_ot_bucket_name,
            hb_bucket: edge_hb_bucket_name,
        };

        // create key value stores for source
        js_context
            .create_key_value(Config {
                bucket: source_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        js_context
            .create_key_value(Config {
                bucket: source_hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // create key value stores for edge
        js_context
            .create_key_value(Config {
                bucket: edge_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        js_context
            .create_key_value(Config {
                bucket: edge_hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut source_publisher = SourceWatermarkPublisher::new(
            js_context.clone(),
            source_config.clone(),
            vec![edge_config.clone()],
        )
        .await
        .expect("Failed to create source publisher");

        let stream = Stream {
            name: "edge_stream",
            vertex: "edge_vertex",
            partition: 0,
        };

        // Publish edge watermark for partition 0
        source_publisher
            .publish_isb_watermark(0, stream.clone(), 1, 200)
            .await;

        let ot_bucket = js_context
            .get_key_value(edge_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let wmb = ot_bucket
            .get("source_vertex-edge_vertex-0")
            .await
            .expect("Failed to get wmb");
        assert!(wmb.is_some());

        let wmb: WMB = wmb.unwrap().try_into().unwrap();
        assert_eq!(wmb.offset, 1);
        assert_eq!(wmb.watermark, 200);

        // delete the stores
        js_context
            .delete_key_value(source_hb_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(source_ot_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(edge_hb_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(edge_ot_bucket_name.to_string())
            .await
            .unwrap();
    }
}
