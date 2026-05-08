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
#[cfg(feature = "nats-tests")]
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

#[cfg(test)]
mod simple_kv_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::watermark::BucketConfig;
    use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;
    use crate::watermark::wmb::WMB;
    use numaflow_shared::kv::KVStore;
    use numaflow_testing::simplekvstore::SimpleKVStore;

    /// A single publish step: stream, offset, watermark, idle flag, and optional processor count.
    struct PublishStep {
        stream: Stream,
        offset: i64,
        watermark: i64,
        idle: bool,
        processor_count: Option<u32>,
    }

    /// Expected WMB state after a sequence of publishes.
    struct ExpectedWMB {
        /// Which vertex store to check.
        vertex: &'static str,
        offset: i64,
        watermark: i64,
        idle: bool,
        partition: u16,
        processor_count: Option<u32>,
    }

    /// Describes a complete publish-then-verify scenario.
    struct PublisherScenario {
        name: &'static str,
        processor_name: &'static str,
        bucket_configs: Vec<BucketConfig>,
        is_source: bool,
        steps: Vec<PublishStep>,
        expected: Vec<ExpectedWMB>,
    }

    /// Runs a PublisherScenario: creates stores and publisher, executes all publish steps,
    /// then verifies the expected WMB in each vertex store.
    async fn run_scenario(scenario: PublisherScenario) {
        // Build OT stores from SimpleKVStore, one per unique vertex.
        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();
        for config in &scenario.bucket_configs {
            if !ot_stores.contains_key(config.vertex) {
                let store = SimpleKVStore::new(config.ot_bucket);
                ot_stores.insert(config.vertex, Arc::new(store.clone()));
            }
        }

        let mut publisher = ISBWatermarkPublisher::new(
            scenario.processor_name.to_string(),
            ot_stores.clone(),
            &scenario.bucket_configs,
            scenario.is_source,
        );

        for step in &scenario.steps {
            publisher
                .publish_watermark_with_processor_count(
                    &step.stream,
                    step.offset,
                    step.watermark,
                    step.idle,
                    step.processor_count,
                )
                .await;
        }

        for expected in &scenario.expected {
            let store = ot_stores.get(expected.vertex).unwrap_or_else(|| {
                panic!(
                    "{}: store for vertex '{}' not found",
                    scenario.name, expected.vertex
                )
            });
            let value = store
                .get(scenario.processor_name)
                .await
                .unwrap_or_else(|e| panic!("{}: get failed: {e}", scenario.name));

            let wmb: WMB = value
                .unwrap_or_else(|| {
                    panic!(
                        "{}: expected WMB in store for '{}'",
                        scenario.name, expected.vertex
                    )
                })
                .try_into()
                .unwrap_or_else(|_| panic!("{}: failed to decode WMB", scenario.name));

            assert_eq!(
                wmb.offset, expected.offset,
                "{}: offset mismatch",
                scenario.name
            );
            assert_eq!(
                wmb.watermark, expected.watermark,
                "{}: watermark mismatch",
                scenario.name
            );
            assert_eq!(wmb.idle, expected.idle, "{}: idle mismatch", scenario.name);
            assert_eq!(
                wmb.partition, expected.partition,
                "{}: partition mismatch",
                scenario.name
            );
            assert_eq!(
                wmb.processor_count, expected.processor_count,
                "{}: processor_count mismatch",
                scenario.name
            );
        }
    }

    // -----------------------------------------------------------------------
    // Basic publish scenarios
    // -----------------------------------------------------------------------

    /// Publish a single watermark to one edge, one partition.
    #[tokio::test]
    async fn test_single_edge_single_partition_publish() {
        run_scenario(PublisherScenario {
            name: "single_edge_single_partition",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "single_edge_single_partition_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![PublishStep {
                stream: Stream {
                    name: "v1-0",
                    vertex: "v1",
                    partition: 0,
                },
                offset: 1,
                watermark: 100,
                idle: false,
                processor_count: None,
            }],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 1,
                watermark: 100,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Publish increasing watermarks, verify only the latest persists.
    #[tokio::test]
    async fn test_single_edge_increasing_watermarks() {
        run_scenario(PublisherScenario {
            name: "increasing_watermarks",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "increasing_wm_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 1,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 2,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 3,
                    watermark: 300,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 3,
                watermark: 300,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Publish to two partitions on the same edge. Each partition tracks state independently,
    /// but they share the same KV key (processor_name), so the last write wins in the store.
    #[tokio::test]
    async fn test_single_edge_two_partitions_independent_state() {
        run_scenario(PublisherScenario {
            name: "two_partitions_independent",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0, 1],
                ot_bucket: "two_part_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 500,
                    idle: false,
                    processor_count: None,
                },
                // Partition 1 can have a lower watermark - it's independent.
                PublishStep {
                    stream: Stream {
                        name: "v1-1",
                        vertex: "v1",
                        partition: 1,
                    },
                    offset: 3,
                    watermark: 50,
                    idle: false,
                    processor_count: None,
                },
            ],
            // The last write to the store wins (partition 1's publish was last).
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 3,
                watermark: 50,
                idle: false,
                partition: 1,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Publish to two separate edges, verify each edge's store has the correct WMB.
    #[tokio::test]
    async fn test_multi_edge_publish() {
        run_scenario(PublisherScenario {
            name: "multi_edge",
            processor_name: "processor1",
            bucket_configs: vec![
                BucketConfig {
                    vertex: "v1",
                    partitions: vec![0],
                    ot_bucket: "multi_edge_v1_ot",
                    delay: None,
                },
                BucketConfig {
                    vertex: "v2",
                    partitions: vec![0],
                    ot_bucket: "multi_edge_v2_ot",
                    delay: None,
                },
            ],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 1,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v2-0",
                        vertex: "v2",
                        partition: 0,
                    },
                    offset: 1,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![
                ExpectedWMB {
                    vertex: "v1",
                    offset: 1,
                    watermark: 100,
                    idle: false,
                    partition: 0,
                    processor_count: None,
                },
                ExpectedWMB {
                    vertex: "v2",
                    offset: 1,
                    watermark: 200,
                    idle: false,
                    partition: 0,
                    processor_count: None,
                },
            ],
        })
        .await;
    }

    /// Verify the idle flag is correctly propagated to the WMB.
    #[tokio::test]
    async fn test_idle_flag_propagated() {
        run_scenario(PublisherScenario {
            name: "idle_flag",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "idle_flag_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![PublishStep {
                stream: Stream {
                    name: "v1-0",
                    vertex: "v1",
                    partition: 0,
                },
                offset: 1,
                watermark: 100,
                idle: true,
                processor_count: None,
            }],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 1,
                watermark: 100,
                idle: true,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    // -----------------------------------------------------------------------
    // Monotonicity and regression scenarios
    // -----------------------------------------------------------------------

    /// Higher offset with lower watermark: the publisher should keep the higher watermark
    /// (regression correction). The published offset advances but watermark does not regress.
    #[tokio::test]
    async fn test_watermark_regression_keeps_higher_watermark() {
        run_scenario(PublisherScenario {
            name: "regression_keeps_higher_wm",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "regression_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
                // Offset advances but watermark regresses — publisher should keep wm=200.
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 20,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 20,
                watermark: 200,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Lower offset with higher watermark: offset should NOT regress, but watermark updates.
    #[tokio::test]
    async fn test_lower_offset_updates_watermark_only() {
        run_scenario(PublisherScenario {
            name: "lower_offset_higher_wm",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "lower_off_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
                // Lower offset, higher watermark — offset stays at 10, watermark updates to 200.
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 5,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 10,
                watermark: 200,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Lower offset with lower watermark: nothing changes (both are below tracked state).
    #[tokio::test]
    async fn test_lower_offset_lower_watermark_no_change() {
        run_scenario(PublisherScenario {
            name: "lower_both",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "lower_both_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
                // Both lower — state unchanged
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 5,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 10,
                watermark: 200,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Same offset, higher watermark: watermark should update, offset stays.
    #[tokio::test]
    async fn test_same_offset_higher_watermark() {
        run_scenario(PublisherScenario {
            name: "same_off_higher_wm",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "same_off_higher_wm_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 10,
                watermark: 200,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    /// Watermark of -1 (initial sentinel) should not cause regression detection.
    /// The code checks `watermark != -1 && self.watermark > watermark`.
    #[tokio::test]
    async fn test_minus_one_watermark_not_treated_as_regression() {
        run_scenario(PublisherScenario {
            name: "minus_one_wm",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "minus_one_wm_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 1,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
                // Offset advances, watermark is -1 (sentinel). Should keep wm=100.
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 2,
                    watermark: -1,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 2,
                watermark: 100,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    // -----------------------------------------------------------------------
    // Processor count scenarios
    // -----------------------------------------------------------------------

    /// Verify processor_count is propagated to the WMB.
    #[tokio::test]
    async fn test_processor_count_propagated() {
        run_scenario(PublisherScenario {
            name: "processor_count",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "proc_count_ot",
                delay: None,
            }],
            is_source: true,
            steps: vec![PublishStep {
                stream: Stream {
                    name: "v1-0",
                    vertex: "v1",
                    partition: 0,
                },
                offset: 1,
                watermark: 100,
                idle: false,
                processor_count: Some(3),
            }],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 1,
                watermark: 100,
                idle: false,
                partition: 0,
                processor_count: Some(3),
            }],
        })
        .await;
    }

    /// Verify processor_count=None results in None in the WMB.
    #[tokio::test]
    async fn test_processor_count_none() {
        run_scenario(PublisherScenario {
            name: "processor_count_none",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "proc_count_none_ot",
                delay: None,
            }],
            is_source: true,
            steps: vec![PublishStep {
                stream: Stream {
                    name: "v1-0",
                    vertex: "v1",
                    partition: 0,
                },
                offset: 1,
                watermark: 100,
                idle: false,
                processor_count: None,
            }],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 1,
                watermark: 100,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    // -----------------------------------------------------------------------
    // Source publisher (is_source=true) behavior
    // -----------------------------------------------------------------------

    /// Source publishers suppress regression warnings but still keep the higher watermark.
    #[tokio::test]
    async fn test_source_publisher_regression_still_corrected() {
        run_scenario(PublisherScenario {
            name: "source_regression",
            processor_name: "source-0",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "source_regression_ot",
                delay: None,
            }],
            is_source: true,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 10,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 20,
                    watermark: 50,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 20,
                watermark: 200,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    // -----------------------------------------------------------------------
    // Multi-edge multi-partition stress scenario
    // -----------------------------------------------------------------------

    /// Publish across 2 edges x 2 partitions, verify each edge has its correct final WMB.
    #[tokio::test]
    async fn test_multi_edge_multi_partition() {
        run_scenario(PublisherScenario {
            name: "multi_edge_multi_partition",
            processor_name: "processor1",
            bucket_configs: vec![
                BucketConfig {
                    vertex: "v1",
                    partitions: vec![0, 1],
                    ot_bucket: "memp_v1_ot",
                    delay: None,
                },
                BucketConfig {
                    vertex: "v2",
                    partitions: vec![0, 1],
                    ot_bucket: "memp_v2_ot",
                    delay: None,
                },
            ],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 1,
                    watermark: 100,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v1-1",
                        vertex: "v1",
                        partition: 1,
                    },
                    offset: 2,
                    watermark: 150,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v2-0",
                        vertex: "v2",
                        partition: 0,
                    },
                    offset: 3,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v2-1",
                        vertex: "v2",
                        partition: 1,
                    },
                    offset: 4,
                    watermark: 250,
                    idle: true,
                    processor_count: None,
                },
            ],
            // Last write per vertex store wins.
            expected: vec![
                ExpectedWMB {
                    vertex: "v1",
                    offset: 2,
                    watermark: 150,
                    idle: false,
                    partition: 1,
                    processor_count: None,
                },
                ExpectedWMB {
                    vertex: "v2",
                    offset: 4,
                    watermark: 250,
                    idle: true,
                    partition: 1,
                    processor_count: None,
                },
            ],
        })
        .await;
    }

    /// Transition from idle to active: publish idle, then non-idle on the same partition.
    #[tokio::test]
    async fn test_idle_to_active_transition() {
        run_scenario(PublisherScenario {
            name: "idle_to_active",
            processor_name: "processor1",
            bucket_configs: vec![BucketConfig {
                vertex: "v1",
                partitions: vec![0],
                ot_bucket: "idle_to_active_ot",
                delay: None,
            }],
            is_source: false,
            steps: vec![
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 1,
                    watermark: 100,
                    idle: true,
                    processor_count: None,
                },
                PublishStep {
                    stream: Stream {
                        name: "v1-0",
                        vertex: "v1",
                        partition: 0,
                    },
                    offset: 2,
                    watermark: 200,
                    idle: false,
                    processor_count: None,
                },
            ],
            expected: vec![ExpectedWMB {
                vertex: "v1",
                offset: 2,
                watermark: 200,
                idle: false,
                partition: 0,
                processor_count: None,
            }],
        })
        .await;
    }

    // -----------------------------------------------------------------------
    // KV store failure resilience
    // -----------------------------------------------------------------------

    /// When the KV store fails, the publisher should not panic and should continue
    /// functioning on subsequent calls (failures are ignored).
    #[tokio::test]
    async fn test_kv_store_failure_is_resilient() {
        let store = SimpleKVStore::new("kv_failure_ot");
        store.error_injector().fail_puts(1); // first put will fail

        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();
        ot_stores.insert("v1", Arc::new(store.clone()));

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: "kv_failure_ot",
            delay: None,
        }];

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        // First publish fails silently.
        publisher.publish_watermark(&stream, 1, 100, false).await;
        let wmb = store.get("processor1").await.unwrap();
        assert!(
            wmb.is_none(),
            "first publish should fail silently due to injected error"
        );

        // Second publish succeeds (error injector decremented to 0).
        publisher.publish_watermark(&stream, 2, 200, false).await;
        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .expect("second publish should succeed")
            .try_into()
            .unwrap();
        assert_eq!(wmb.offset, 2);
        assert_eq!(wmb.watermark, 200);
    }

    // -----------------------------------------------------------------------
    // Delay-based throttling scenarios
    // -----------------------------------------------------------------------

    /// With a long delay configured, publishes within the window are suppressed.
    #[tokio::test]
    async fn test_delay_suppresses_publish_within_window() {
        let store = SimpleKVStore::new("delay_suppress_ot");
        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();
        ot_stores.insert("v1", Arc::new(store.clone()));

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: "delay_suppress_ot",
            delay: Some(Duration::from_secs(60)),
        }];

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        // Default last_published_time = Instant::now(), so elapsed ≈ 0 < 60s.
        // Publish is suppressed.
        publisher.publish_watermark(&stream, 1, 100, false).await;

        let wmb = store.get("processor1").await.unwrap();
        assert!(
            wmb.is_none(),
            "publish should be suppressed within delay window"
        );
    }

    /// After the delay has elapsed, the publish should go through.
    #[tokio::test]
    async fn test_delay_allows_publish_after_elapsed() {
        let store = SimpleKVStore::new("delay_elapsed_ot");
        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();
        ot_stores.insert("v1", Arc::new(store.clone()));

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: "delay_elapsed_ot",
            delay: Some(Duration::from_millis(10)),
        }];

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        // Wait for delay to elapse, then publish.
        tokio::time::sleep(Duration::from_millis(20)).await;

        publisher.publish_watermark(&stream, 1, 100, false).await;

        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .expect("WMB should exist after delay elapsed")
            .try_into()
            .unwrap();
        assert_eq!(wmb.watermark, 100);
        assert_eq!(wmb.offset, 1);
    }

    /// With no delay (None), every publish goes through.
    #[tokio::test]
    async fn test_no_delay_always_publishes() {
        let store = SimpleKVStore::new("no_delay_ot");
        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();
        ot_stores.insert("v1", Arc::new(store.clone()));

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: "no_delay_ot",
            delay: None,
        }];

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        publisher.publish_watermark(&stream, 1, 100, false).await;
        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(wmb.watermark, 100);

        publisher.publish_watermark(&stream, 2, 200, false).await;
        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(wmb.watermark, 200);

        publisher.publish_watermark(&stream, 3, 300, false).await;
        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(wmb.watermark, 300);
    }

    /// Delay accumulates state: publishes within the window update tracked state, and
    /// when the delay elapses, the best values are published.
    #[tokio::test]
    async fn test_delay_publishes_best_accumulated_state() {
        let store = SimpleKVStore::new("delay_accum_ot");
        let mut ot_stores: HashMap<&'static str, Arc<dyn KVStore>> = HashMap::new();
        ot_stores.insert("v1", Arc::new(store.clone()));

        let bucket_configs = vec![BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: "delay_accum_ot",
            delay: Some(Duration::from_millis(50)),
        }];

        let mut publisher =
            ISBWatermarkPublisher::new("processor1".to_string(), ot_stores, &bucket_configs, false);

        let stream = Stream {
            name: "v1-0",
            vertex: "v1",
            partition: 0,
        };

        // Wait for initial delay to pass so first publish goes through.
        tokio::time::sleep(Duration::from_millis(60)).await;

        // First publish goes through.
        publisher.publish_watermark(&stream, 1, 100, false).await;
        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(wmb.offset, 1);
        assert_eq!(wmb.watermark, 100);

        // These are within the delay window — suppressed, but state accumulates.
        publisher.publish_watermark(&stream, 5, 300, false).await;
        publisher.publish_watermark(&stream, 10, 500, false).await;

        // Store should still have the first publish.
        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(wmb.offset, 1, "store should still have first publish");
        assert_eq!(wmb.watermark, 100);

        // Wait for delay to elapse, then publish again — should get best accumulated state.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // publish again with lower offset and watermark to trigger accumulated state to be reflected
        // in the store
        publisher.publish_watermark(&stream, 1, 100, false).await;

        let wmb: WMB = store
            .get("processor1")
            .await
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(wmb.offset, 10);
        assert_eq!(wmb.watermark, 500);
    }

    #[tokio::test]
    async fn test_last_published_state_delay_crossed() {
        // No delay configured: always crossed.
        let state = LastPublishedState::default();
        assert!(state.delay_crossed());

        // With delay, not yet elapsed.
        let state = LastPublishedState {
            delay: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        assert!(!state.delay_crossed());
    }
}
