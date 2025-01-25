use std::collections::HashMap;

use chrono::Utc;
use tracing::info;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::BucketConfig;
use crate::error;
use crate::watermark::edge::edge_publisher::EdgePublisher;

/// SourcePublisher is the watermark publisher for the source vertex.
pub(crate) struct SourcePublisher {
    js_context: async_nats::jetstream::Context,
    source_config: BucketConfig,
    to_vertex_configs: Vec<BucketConfig>,
    publishers: HashMap<String, EdgePublisher>,
}

impl SourcePublisher {
    /// Creates a new SourcePublisher.
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        source_config: BucketConfig,
        to_vertex_configs: Vec<BucketConfig>,
    ) -> error::Result<Self> {
        Ok(SourcePublisher {
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
    }

    /// Publishes the edge watermark for the input partition. It internally uses edge publisher with
    /// processor set to the input partition and edge OTs.
    pub(crate) async fn publish_edge_watermark(
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
