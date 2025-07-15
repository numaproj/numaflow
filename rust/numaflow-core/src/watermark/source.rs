//! Exposes methods to fetch and publish the watermark for the messages read from [Source], also
//! exposes methods to publish the watermark for the messages written to [ISB]. Since the watermark
//! starts at source we publish and fetch inside the source to get the minimum event time across the
//! partitions. Since we write the messages to ISB, we publish the watermark for the messages written
//! to ISB. Hence, source publisher internally uses ISB publisher to publish the watermarks. Since source
//! is not streaming in nature we don't have to track the inflight offsets and their watermarks.
//!
//!
//! ##### Watermark flow
//!
//! ```text
//! (Read) ---> (Publish WM For Source) ---> (Fetch WM For Source) ---> (Write to ISB) ---> (Publish WM to ISB)
//! ```
//!
//! [Source]: https://numaflow.numaproj.io/user-guide/sources/overview/
//! [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use std::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::SourceWatermarkConfig;
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::{Error, Result};
use crate::message::{Message, Offset};
use crate::watermark::idle::isb::ISBIdleDetector;
use crate::watermark::idle::source::SourceIdleDetector;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::source::source_wm_fetcher::SourceWatermarkFetcher;
use crate::watermark::source::source_wm_publisher::SourceWatermarkPublisher;
use crate::watermark::wmb::Watermark;

/// fetcher for fetching the source watermark
pub(crate) mod source_wm_fetcher;

/// publisher for publishing the source watermark
pub(crate) mod source_wm_publisher;

/// Messages that can be sent to the SourceWatermarkActor
#[allow(clippy::enum_variant_names)]
enum SourceActorMessage {
    PublishSourceWatermark {
        partition: u16,
        event_time: i64,
        is_idle: bool,
    },
    PublishISBWatermark {
        input_partition: u16,
        stream: Stream,
        offset: i64,
        watermark: i64,
        is_idle: bool,
    },
    FetchSourceWatermark {
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
    FetchHeadWatermark {
        partition_idx: u16,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
}

/// SourceWatermarkActor comprises SourcePublisher and SourceFetcher.
/// Only responsible for the actual fetch and publish operations.
struct SourceWatermarkActor {
    publisher: SourceWatermarkPublisher,
    fetcher: SourceWatermarkFetcher,
}

impl SourceWatermarkActor {
    /// Creates a new SourceWatermarkActor.
    fn new(publisher: SourceWatermarkPublisher, fetcher: SourceWatermarkFetcher) -> Self {
        Self { publisher, fetcher }
    }

    /// Runs the SourceWatermarkActor
    async fn run(mut self, mut receiver: Receiver<SourceActorMessage>) {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = self.handle_message(message).await {
                error!("error handling message: {:?}", e);
            }
        }
    }

    /// Handles the SourceActorMessage.
    async fn handle_message(&mut self, message: SourceActorMessage) -> Result<()> {
        match message {
            // publish the watermark for the messages read from the source to the source bucket
            SourceActorMessage::PublishSourceWatermark {
                partition,
                event_time,
                is_idle,
            } => {
                self.publisher
                    .publish_source_watermark(partition, event_time, is_idle)
                    .await;
            }

            // publish the watermark for the messages written to ISB
            SourceActorMessage::PublishISBWatermark {
                input_partition,
                stream,
                offset,
                watermark,
                is_idle,
            } => {
                self.publisher
                    .publish_isb_watermark(input_partition, &stream, offset, watermark, is_idle)
                    .await;
            }

            // fetch the source watermark
            SourceActorMessage::FetchSourceWatermark { oneshot_tx } => {
                let watermark = self.fetcher.fetch_source_watermark();
                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))?;
            }

            // fetch the head watermark
            SourceActorMessage::FetchHeadWatermark {
                partition_idx,
                oneshot_tx,
            } => {
                let watermark = self.fetcher.fetch_head_watermark(partition_idx);
                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))?;
            }
        }

        Ok(())
    }
}

/// SourceWatermarkHandle is the handle for the SourceWatermarkActor.
/// Exposes methods to publish the source watermark and edge watermark.
/// Contains all the computation logic.
#[derive(Clone)]
pub(crate) struct SourceWatermarkHandle {
    sender: tokio::sync::mpsc::Sender<SourceActorMessage>,
    isb_idle_manager: ISBIdleDetector,
    source_idle_manager: Option<SourceIdleDetector>,
    active_input_partitions: Arc<Mutex<HashMap<u16, bool>>>,
}

impl SourceWatermarkHandle {
    /// Creates a new SourceWatermarkHandle.
    pub(crate) async fn new(
        idle_timeout: Duration,
        js_context: async_nats::jetstream::Context,
        to_vertex_configs: &[ToVertexConfig],
        config: &SourceWatermarkConfig,
        cln_token: CancellationToken,
    ) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let processor_manager = ProcessorManager::new(
            js_context.clone(),
            &config.source_bucket_config,
            VertexType::Source,
            *crate::config::get_vertex_replica(),
        )
        .await?;

        let fetcher = SourceWatermarkFetcher::new(processor_manager);
        let publisher = SourceWatermarkPublisher::new(
            js_context.clone(),
            config.max_delay,
            config.source_bucket_config.clone(),
            config.to_vertex_bucket_config.clone(),
        )
        .await
        .map_err(|e| Error::Watermark(e.to_string()))?;

        let source_idle_manager = config
            .idle_config
            .as_ref()
            .map(|idle_config| SourceIdleDetector::new(idle_config.clone()));

        let isb_idle_manager =
            ISBIdleDetector::new(idle_timeout, to_vertex_configs, js_context.clone()).await;

        let actor = SourceWatermarkActor::new(publisher, fetcher);
        tokio::spawn(async move { actor.run(receiver).await });

        let source_watermark_handle = Self {
            sender,
            isb_idle_manager,
            source_idle_manager,
            active_input_partitions: Arc::new(Mutex::new(HashMap::new())),
        };

        // start a task to keep publishing idle watermarks every 100ms
        tokio::spawn({
            let mut source_watermark_handle = source_watermark_handle.clone();
            let mut interval_ticker = tokio::time::interval(idle_timeout);
            async move {
                loop {
                    tokio::select! {
                        _ = interval_ticker.tick() => {
                            source_watermark_handle.publish_isb_idle_watermark().await;
                        }
                        _ = cln_token.cancelled() => {
                            break;
                        }
                    }
                }
            }
        });

        Ok(source_watermark_handle)
    }

    /// Generates and Publishes the source watermark for the given messages.
    pub(crate) async fn generate_and_publish_source_watermark(&mut self, messages: &[Message]) {
        // we need to build a hash-map of the lowest event time for each partition
        let partition_to_lowest_event_time =
            messages.iter().fold(HashMap::new(), |mut acc, message| {
                let partition_id = match &message.offset {
                    Offset::Int(offset) => offset.partition_idx,
                    Offset::String(offset) => offset.partition_idx,
                };

                let event_time = message.event_time.timestamp_millis();

                let lowest_event_time = acc.entry(partition_id).or_insert(event_time);
                if event_time < *lowest_event_time {
                    *lowest_event_time = event_time;
                }
                acc
            });

        if partition_to_lowest_event_time.is_empty() {
            return;
        }

        // Publish the watermark for each partition
        for (partition, event_time) in partition_to_lowest_event_time {
            self.sender
                .send(SourceActorMessage::PublishSourceWatermark {
                    partition,
                    event_time,
                    is_idle: false,
                })
                .await
                .unwrap_or_else(|e| error!("failed to send message: {:?}", e));

            // cache the active input partitions, we need it for publishing isb idle watermark
            let mut active_input_partitions = self
                .active_input_partitions
                .lock()
                .expect("failed to acquire lock");
            active_input_partitions.insert(partition, true);
        }

        // Reset the source idle manager
        if let Some(source_idle_manager) = &mut self.source_idle_manager {
            source_idle_manager.reset();
        }
    }

    /// Publishes the watermark for the given input partition on to the ISB of the next vertex.
    pub(crate) async fn publish_source_isb_watermark(
        &mut self,
        stream: Stream,
        offset: Offset,
        input_partition: u16,
    ) {
        let Offset::Int(offset) = offset else {
            error!(?offset, "Invalid offset type, cannot publish watermark");
            return;
        };

        // Fetch the source watermark
        let watermark = self.fetch_source_watermark().await;

        // Send the publish watermark message to the actor
        self.sender
            .send(SourceActorMessage::PublishISBWatermark {
                input_partition,
                stream: stream.clone(),
                offset: offset.offset,
                watermark: watermark.timestamp_millis(),
                is_idle: false,
            })
            .await
            .expect("failed to send message");

        // Mark the vertex and partition as active since we published the watermark
        self.isb_idle_manager.reset_idle(&stream).await;
    }

    /// Fetches the source watermark.
    pub(crate) async fn fetch_source_watermark(&mut self) -> Watermark {
        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(SourceActorMessage::FetchSourceWatermark { oneshot_tx })
            .await
        {
            error!(?e, "Failed to send message");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        }

        match oneshot_rx.await {
            Ok(watermark) => watermark.unwrap_or_else(|e| {
                error!(?e, "Failed to fetch watermark");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }),
            Err(e) => {
                error!(?e, "Failed to receive response");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }
        }
    }

    /// Fetches the head watermark using the source watermark fetcher. This returns the minimum
    /// of the head watermarks across all active processors for the specified partition.
    pub(crate) async fn fetch_head_watermark(&mut self, partition_idx: u16) -> Watermark {
        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(SourceActorMessage::FetchHeadWatermark {
                partition_idx,
                oneshot_tx,
            })
            .await
        {
            error!(?e, "Failed to send message");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        }

        match oneshot_rx.await {
            Ok(watermark) => watermark.unwrap_or_else(|e| {
                error!(?e, "Failed to fetch head watermark");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }),
            Err(e) => {
                error!(?e, "Failed to receive response");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }
        }
    }

    pub(crate) async fn publish_source_idle_watermark(&mut self, partitions: Vec<u16>) {
        // First check if source idle manager exists and if source is idling
        let is_source_idling = if let Some(source_idle_manager) = &self.source_idle_manager {
            source_idle_manager.is_source_idling()
        } else {
            return;
        };

        if !is_source_idling {
            return;
        }

        // Fetch the source watermark first
        let compute_wm = self.fetch_source_watermark().await;

        // Now get the idle watermark
        let idle_wm = if let Some(source_idle_manager) = &mut self.source_idle_manager {
            source_idle_manager.update_and_fetch_idle_wm(compute_wm.timestamp_millis())
        } else {
            return;
        };

        // publish the idle watermark for the given partitions
        for partition in partitions.iter() {
            self.sender
                .send(SourceActorMessage::PublishSourceWatermark {
                    partition: *partition,
                    event_time: idle_wm,
                    is_idle: true,
                })
                .await
                .unwrap_or_else(|e| error!("failed to send message: {:?}", e));
        }

        // since isb will also be idling since we are not reading any data
        // we need to propagate idle watermarks to ISB
        let compute_wm = self.fetch_source_watermark().await;
        if compute_wm.timestamp_millis() == -1 {
            return;
        }

        // all the isb partitions will be idling because the source is idling, fetch the idle offset
        // for each vertex and partition and publish the idle watermark
        let vertex_streams = self.isb_idle_manager.fetch_all_streams().await;
        for stream in vertex_streams.iter() {
            let offset = self
                .isb_idle_manager
                .fetch_idle_offset(stream)
                .await
                .unwrap_or(-1);
            for idle_partition in partitions.iter() {
                self.sender
                    .send(SourceActorMessage::PublishISBWatermark {
                        input_partition: *idle_partition,
                        stream: stream.clone(),
                        offset,
                        watermark: compute_wm.timestamp_millis(),
                        is_idle: true,
                    })
                    .await
                    .unwrap_or_else(|e| error!("failed to send message: {:?}", e));
            }

            // mark the vertex and partition as idle, since we published the idle watermark
            self.isb_idle_manager
                .update_idle_metadata(stream, offset)
                .await;
        }
    }

    pub(crate) async fn publish_isb_idle_watermark(&mut self) {
        // if source is idling, we can avoid publishing the idle watermark since we publish
        // the idle watermark for all the downstream partitions in the source idling control flow
        if let Some(source_idle_manager) = &self.source_idle_manager
            && source_idle_manager.is_source_idling()
        {
            return;
        }

        // fetch the source watermark, identify the idle partitions and publish the idle watermark
        let compute_wm = self.fetch_source_watermark().await;
        if compute_wm.timestamp_millis() == -1 {
            return;
        }

        // we should only publish to active input partitions, because we consider input-partitions as
        // the processing entity while publishing watermark inside source
        let idle_streams = self.isb_idle_manager.fetch_idle_streams().await;
        for stream in idle_streams.iter() {
            let offset = self
                .isb_idle_manager
                .fetch_idle_offset(stream)
                .await
                .unwrap_or(-1);
            let active_input_partitions = {
                let active_input_partitions = self
                    .active_input_partitions
                    .lock()
                    .expect("failed to acquire lock");
                active_input_partitions.keys().cloned().collect::<Vec<_>>()
            };
            for partition in active_input_partitions {
                self.sender
                    .send(SourceActorMessage::PublishISBWatermark {
                        input_partition: partition,
                        stream: stream.clone(),
                        offset,
                        watermark: compute_wm.timestamp_millis(),
                        is_idle: true,
                    })
                    .await
                    .unwrap_or_else(|e| error!("failed to send message: {:?}", e));
            }
            self.isb_idle_manager
                .update_idle_metadata(stream, offset)
                .await;
        }
        // clear the cache since we published the idle watermarks
        let mut active_input_partitions = self
            .active_input_partitions
            .lock()
            .expect("failed to acquire lock");
        active_input_partitions.clear();
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use async_nats::jetstream::stream;
    use bytes::BytesMut;
    use chrono::{DateTime, Utc};
    use numaflow_pb::objects::watermark::Heartbeat;
    use prost::Message as _;
    use tokio::time::sleep;

    use super::*;
    use crate::config::pipeline::VertexType;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::config::pipeline::watermark::{BucketConfig, IdleConfig};
    use crate::message::{IntOffset, Message};
    use crate::watermark::wmb::WMB;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_source_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_publish_source_watermark_OT";
        let hb_bucket_name = "test_publish_source_watermark_PROCESSORS";

        let source_config = SourceWatermarkConfig {
            max_delay: Default::default(),
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: 1, // partitions is always one for source
                ot_bucket: ot_bucket_name,
                hb_bucket: hb_bucket_name,
                delay: None,
            },
            to_vertex_bucket_config: vec![],
            idle_config: None,
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

        let mut handle = SourceWatermarkHandle::new(
            Duration::from_millis(100),
            js_context.clone(),
            Default::default(),
            &source_config,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create source watermark handle");

        let messages = vec![
            Message {
                offset: Offset::Int(IntOffset {
                    offset: 1,
                    partition_idx: 0,
                }),
                event_time: DateTime::from_timestamp_millis(60000).unwrap(),
                ..Default::default()
            },
            Message {
                offset: Offset::Int(IntOffset {
                    offset: 2,
                    partition_idx: 0,
                }),
                event_time: DateTime::from_timestamp_millis(70000).unwrap(),
                ..Default::default()
            },
        ];

        handle
            .generate_and_publish_source_watermark(&messages)
            .await;

        // try getting the value for the processor from the ot bucket to make sure
        // the watermark is getting published(min event time in the batch), wait until
        // one second if it's not there, fail the test
        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let mut wmb_found = false;
        for _ in 0..10 {
            let wmb = ot_bucket
                .get("source-source_vertex-0")
                .await
                .expect("Failed to get wmb");
            if wmb.is_some() {
                let wmb: WMB = wmb.unwrap().try_into().unwrap();
                assert_eq!(wmb.watermark, 60000);
                wmb_found = true;
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        if !wmb_found {
            panic!("Failed to get watermark");
        }

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
    async fn test_publish_source_edge_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let source_ot_bucket_name = "test_publish_source_edge_watermark_source_OT";
        let source_hb_bucket_name = "test_publish_source_edge_watermark_source_PROCESSORS";
        let edge_ot_bucket_name = "test_publish_source_edge_watermark_edge_OT";
        let edge_hb_bucket_name = "test_publish_source_edge_watermark_edge_PROCESSORS";

        // delete the stores
        let _ = js_context
            .delete_key_value(source_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(source_hb_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(edge_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(edge_hb_bucket_name.to_string())
            .await;

        let source_config = SourceWatermarkConfig {
            max_delay: Default::default(),
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: 2,
                ot_bucket: source_ot_bucket_name,
                hb_bucket: source_hb_bucket_name,
                delay: None,
            },
            to_vertex_bucket_config: vec![BucketConfig {
                vertex: "edge_vertex",
                partitions: 2,
                ot_bucket: edge_ot_bucket_name,
                hb_bucket: edge_hb_bucket_name,
                delay: None,
            }],
            idle_config: None,
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

        let mut handle = SourceWatermarkHandle::new(
            Duration::from_millis(100),
            js_context.clone(),
            &[ToVertexConfig {
                name: "edge_vertex",
                writer_config: BufferWriterConfig {
                    streams: vec![Stream {
                        name: "edge_stream",
                        vertex: "edge_vertex",
                        partition: 0,
                    }],
                    ..Default::default()
                },
                conditions: None,
                partitions: 1,
                to_vertex_type: VertexType::MapUDF,
            }],
            &source_config,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create source watermark handle");

        let ot_bucket = js_context
            .get_key_value(edge_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let stream = Stream {
            name: "edge_stream",
            vertex: "edge_vertex",
            partition: 0,
        };

        let mut wmb_found = false;
        for i in 1..11 {
            // publish source watermarks before publishing edge watermarks
            let messages = vec![
                Message {
                    offset: Offset::Int(IntOffset {
                        offset: 1,
                        partition_idx: 0,
                    }),
                    event_time: DateTime::from_timestamp_millis(10000 * i).unwrap(),
                    ..Default::default()
                },
                Message {
                    offset: Offset::Int(IntOffset {
                        offset: 2,
                        partition_idx: 0,
                    }),
                    event_time: DateTime::from_timestamp_millis(20000 * i).unwrap(),
                    ..Default::default()
                },
            ];

            handle
                .generate_and_publish_source_watermark(&messages)
                .await;

            let offset = Offset::Int(IntOffset {
                offset: i,
                partition_idx: 0,
            });
            handle
                .publish_source_isb_watermark(stream.clone(), offset, 0)
                .await;
        }

        // check if the watermark is published
        for _ in 0..10 {
            let wmb = ot_bucket
                .get("source_vertex-0")
                .await
                .expect("Failed to get wmb");
            if wmb.is_some() {
                let wmb: WMB = wmb.unwrap().try_into().unwrap();
                assert_ne!(wmb.watermark, -1);
                wmb_found = true;
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        if !wmb_found {
            panic!("Failed to get watermark");
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_invoke_publish_source_idle_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_invoke_publish_source_idle_watermark_OT";
        let hb_bucket_name = "test_invoke_publish_source_idle_watermark_PROCESSORS";
        let to_vertex_ot_bucket_name = "test_invoke_publish_source_idle_watermark_TO_VERTEX_OT";
        let to_vertex_hb_bucket_name =
            "test_invoke_publish_source_idle_watermark_TO_VERTEX_PROCESSORS";

        let to_vertex_configs = vec![ToVertexConfig {
            name: "edge_vertex",
            writer_config: BufferWriterConfig {
                streams: vec![Stream {
                    name: "edge_stream",
                    vertex: "edge_vertex",
                    partition: 0,
                }],
                ..Default::default()
            },
            conditions: None,
            partitions: 1,
            to_vertex_type: VertexType::MapUDF,
        }];

        // create to vertex stream since we will be writing ctrl message to it
        js_context
            .get_or_create_stream(stream::Config {
                name: "edge_stream".to_string(),
                subjects: vec!["edge_stream".to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_bucket_config = BucketConfig {
            vertex: "v1",
            partitions: 1,
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
            delay: None,
        };

        let to_vertex_bucket_config = BucketConfig {
            vertex: "edge_vertex",
            partitions: 1,
            ot_bucket: to_vertex_ot_bucket_name,
            hb_bucket: to_vertex_hb_bucket_name,
            delay: None,
        };

        // delete stores if the exist
        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_vertex_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_vertex_hb_bucket_name.to_string())
            .await;

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
        js_context
            .create_key_value(Config {
                bucket: to_vertex_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        js_context
            .create_key_value(Config {
                bucket: to_vertex_hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_idle_config = IdleConfig {
            threshold: Duration::from_millis(10),
            step_interval: Duration::from_millis(5),
            increment_by: Duration::from_millis(1),
        };

        let mut handle = SourceWatermarkHandle::new(
            Duration::from_millis(10),
            js_context.clone(),
            &to_vertex_configs,
            &SourceWatermarkConfig {
                max_delay: Default::default(),
                source_bucket_config,
                to_vertex_bucket_config: vec![to_vertex_bucket_config],
                idle_config: Some(source_idle_config),
            },
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create SourceWatermarkHandle");

        // get ot and hb buckets for source and publish some wmb and heartbeats
        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");
        let hb_bucket = js_context
            .get_key_value(hb_bucket_name)
            .await
            .expect("Failed to get hb bucket");

        for i in 1..11 {
            let wmb: BytesMut = WMB {
                watermark: 1000 * i,
                offset: i,
                idle: false,
                partition: 0,
            }
            .try_into()
            .unwrap();
            ot_bucket
                .put("source-v1-0", wmb.freeze())
                .await
                .expect("Failed to put wmb");

            let heartbeat = Heartbeat {
                heartbeat: Utc::now().timestamp_millis(),
            };
            let mut bytes = BytesMut::new();
            heartbeat
                .encode(&mut bytes)
                .expect("Failed to encode heartbeat");

            hb_bucket
                .put("source-v1-0", bytes.freeze())
                .await
                .expect("Failed to put hb");
            sleep(Duration::from_millis(3)).await;
        }

        // sleep so that the idle condition is met
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Invoke publish_source_idle_watermark
        handle.publish_source_idle_watermark(vec![0]).await;

        // Check if the idle watermark is published
        let ot_bucket = js_context
            .get_key_value(to_vertex_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let mut wmb_found = false;
        for _ in 0..10 {
            if let Some(wmb) = ot_bucket.get("v1-0").await.expect("Failed to get wmb") {
                let wmb: WMB = wmb.try_into().unwrap();
                // idle watermark should be published
                if wmb.idle {
                    wmb_found = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(wmb_found, "Idle watermark not found");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_source_isb_idle_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_publish_source_isb_idle_watermark_OT";
        let hb_bucket_name = "test_publish_source_isb_idle_watermark_PROCESSORS";
        let to_vertex_ot_bucket_name = "test_publish_source_isb_idle_watermark_TO_VERTEX_OT";
        let to_vertex_hb_bucket_name =
            "test_publish_source_isb_idle_watermark_TO_VERTEX_PROCESSORS";

        let to_vertex_configs = vec![ToVertexConfig {
            name: "edge_vertex",
            writer_config: BufferWriterConfig {
                streams: vec![Stream {
                    name: "edge_stream",
                    vertex: "edge_vertex",
                    partition: 0,
                }],
                ..Default::default()
            },
            conditions: None,
            partitions: 1,
            to_vertex_type: VertexType::MapUDF,
        }];

        // create to vertex stream since we will be writing ctrl message to it
        js_context
            .get_or_create_stream(stream::Config {
                name: "edge_stream".to_string(),
                subjects: vec!["edge_stream".to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_bucket_config = BucketConfig {
            vertex: "v1",
            partitions: 1,
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
            delay: None,
        };

        let to_vertex_bucket_config = BucketConfig {
            vertex: "edge_vertex",
            partitions: 1,
            ot_bucket: to_vertex_ot_bucket_name,
            hb_bucket: to_vertex_hb_bucket_name,
            delay: None,
        };

        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_vertex_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_vertex_hb_bucket_name.to_string())
            .await;

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
        js_context
            .create_key_value(Config {
                bucket: to_vertex_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        js_context
            .create_key_value(Config {
                bucket: to_vertex_hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_idle_config = IdleConfig {
            threshold: Duration::from_millis(2000), // set higher value so that the source won't be idling
            step_interval: Duration::from_millis(5),
            increment_by: Duration::from_millis(1),
        };

        let mut handle = SourceWatermarkHandle::new(
            Duration::from_millis(5),
            js_context.clone(),
            &to_vertex_configs,
            &SourceWatermarkConfig {
                max_delay: Default::default(),
                source_bucket_config,
                to_vertex_bucket_config: vec![to_vertex_bucket_config],
                idle_config: Some(source_idle_config),
            },
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create SourceWatermarkHandle");

        let messages = vec![Message {
            offset: Offset::Int(IntOffset {
                offset: 1,
                partition_idx: 0,
            }),
            event_time: DateTime::from_timestamp_millis(100).unwrap(),
            ..Default::default()
        }];

        // generate some watermarks to make partition active
        handle
            .generate_and_publish_source_watermark(&messages)
            .await;

        // get ot and hb buckets for source and publish some wmb and heartbeats
        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");
        let hb_bucket = js_context
            .get_key_value(hb_bucket_name)
            .await
            .expect("Failed to get hb bucket");

        for i in 1..10 {
            let wmb: BytesMut = WMB {
                watermark: 1000 * i,
                offset: i,
                idle: false,
                partition: 0,
            }
            .try_into()
            .unwrap();
            ot_bucket
                .put("source-v1-0", wmb.freeze())
                .await
                .expect("Failed to put wmb");

            let heartbeat = Heartbeat {
                heartbeat: Utc::now().timestamp_millis(),
            };
            let mut bytes = BytesMut::new();
            heartbeat
                .encode(&mut bytes)
                .expect("Failed to encode heartbeat");

            hb_bucket
                .put("source-v1-0", bytes.freeze())
                .await
                .expect("Failed to put hb");
            sleep(Duration::from_millis(3)).await;
        }

        // Check if the idle watermark is published
        let ot_bucket = js_context
            .get_key_value(to_vertex_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let mut wmb_found = false;
        for _ in 0..10 {
            if let Some(wmb) = ot_bucket.get("v1-0").await.expect("Failed to get wmb") {
                let wmb: WMB = wmb.try_into().expect("Failed to convert to WMB");
                // idle watermark should be published
                if wmb.idle {
                    wmb_found = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(wmb_found, "Idle watermark not found");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_head_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_fetch_head_watermark_source_OT";
        let hb_bucket_name = "test_fetch_head_watermark_source_PROCESSORS";

        let source_config = SourceWatermarkConfig {
            max_delay: Default::default(),
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: 1,
                ot_bucket: ot_bucket_name,
                hb_bucket: hb_bucket_name,
                delay: None,
            },
            to_vertex_bucket_config: vec![],
            idle_config: None,
        };

        // delete the stores first
        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;

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

        // Publish some WMB entries to the source OT bucket to simulate source processors
        let ot_bucket = js_context.get_key_value(ot_bucket_name).await.unwrap();

        // Create WMB entries that will be read by the ProcessorManager
        let wmb1 = WMB {
            watermark: 60000,
            offset: 1,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 70000,
            offset: 2,
            idle: false,
            partition: 0,
        };

        // Publish WMB entries to the OT bucket with a processor name
        let processor_name = "source-processor-0";
        let wmb1_bytes: bytes::BytesMut = wmb1.try_into().unwrap();
        let wmb2_bytes: bytes::BytesMut = wmb2.try_into().unwrap();
        ot_bucket
            .put(processor_name, wmb1_bytes.freeze())
            .await
            .unwrap();
        ot_bucket
            .put(processor_name, wmb2_bytes.freeze())
            .await
            .unwrap();

        // Also publish a heartbeat to the HB bucket to mark the processor as active
        let hb_bucket = js_context.get_key_value(hb_bucket_name).await.unwrap();
        let current_time = chrono::Utc::now().timestamp_millis();
        hb_bucket
            .put(processor_name, current_time.to_string().into())
            .await
            .unwrap();

        let mut handle = SourceWatermarkHandle::new(
            Duration::from_millis(100),
            js_context.clone(),
            Default::default(),
            &source_config,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create source watermark handle");

        // Poll for head watermark with timeout using tokio::time::timeout
        let timeout_duration = Duration::from_millis(200);
        let poll_interval = Duration::from_millis(10);

        let head_watermark = tokio::time::timeout(timeout_duration, async {
            loop {
                let watermark = handle.fetch_head_watermark(0).await;

                // Break if we got a valid watermark (not -1)
                if watermark.timestamp_millis() != -1 {
                    return watermark;
                }

                // Wait before next poll
                tokio::time::sleep(poll_interval).await;
            }
        })
        .await
        .expect("Timeout: head watermark still -1 after 200ms");

        // The head watermark should be a valid timestamp (not -1)
        assert_ne!(head_watermark.timestamp_millis(), -1);

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
