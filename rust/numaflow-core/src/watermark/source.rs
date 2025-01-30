use std::collections::HashMap;

use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::SourceWatermarkConfig;
use crate::error::{Error, Result};
use crate::message::{IntOffset, Message, Offset};
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::source::source_wm_fetcher::SourceWatermarkFetcher;
use crate::watermark::source::source_wm_publisher::SourceWatermarkPublisher;

/// fetcher for fetching the source watermark
pub(crate) mod source_wm_fetcher;

/// publisher for publishing the source watermark
pub(crate) mod source_wm_publisher;

/// Messages that can be sent to the SourceWatermarkActor
enum SourceActorMessage {
    PublishSourceWatermark {
        map: HashMap<u16, i64>,
    },
    PublishEdgeWatermark {
        offset: IntOffset,
        stream: Stream,
        input_partition: u16,
    },
}

/// SourceWatermarkActor comprises SourcePublisher and SourceFetcher.
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

    /// Handles the SourceActorMessage
    async fn handle_message(&mut self, message: SourceActorMessage) -> Result<()> {
        match message {
            SourceActorMessage::PublishSourceWatermark { map } => {
                for (partition, event_time) in map {
                    self.publisher
                        .publish_source_watermark(partition, event_time)
                        .await;
                }
            }
            SourceActorMessage::PublishEdgeWatermark {
                offset,
                stream,
                input_partition,
            } => {
                let watermark = self.fetcher.fetch_source_watermark().await?;
                self.publisher
                    .publish_edge_watermark(
                        input_partition,
                        stream,
                        offset.offset,
                        watermark.timestamp_millis(),
                    )
                    .await;
            }
        }

        Ok(())
    }
}

/// SourceWatermarkHandle is the handle for the SourceWatermarkActor.
/// Exposes methods to publish the source watermark and edge watermark.
#[derive(Clone)]
pub(crate) struct SourceWatermarkHandle {
    sender: tokio::sync::mpsc::Sender<SourceActorMessage>,
}

impl SourceWatermarkHandle {
    /// Creates a new SourceWatermarkHandle.
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        config: &SourceWatermarkConfig,
    ) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let processor_manager =
            ProcessorManager::new(js_context.clone(), &config.source_bucket_config).await?;

        let fetcher = SourceWatermarkFetcher::new(processor_manager)
            .await
            .map_err(|e| Error::Watermark(e.to_string()))?;

        let publisher = SourceWatermarkPublisher::new(
            js_context.clone(),
            config.source_bucket_config.clone(),
            config.to_vertex_bucket_config.clone(),
        )
        .await
        .map_err(|e| Error::Watermark(e.to_string()))?;

        let actor = SourceWatermarkActor::new(publisher, fetcher);
        tokio::spawn(async move { actor.run(receiver).await });
        Ok(Self { sender })
    }

    /// Publishes the source watermark for the given messages.
    pub(crate) async fn publish_source_watermark(&self, messages: &[Message]) -> Result<()> {
        // we need to find the lowest event time for each partition
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

        self.sender
            .send(SourceActorMessage::PublishSourceWatermark {
                map: partition_to_lowest_event_time,
            })
            .await
            .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
        Ok(())
    }

    /// Publishes the edge watermark for the given input partition.
    pub(crate) async fn publish_source_edge_watermark(
        &self,
        stream: Stream,
        offset: Offset,
        input_partition: u16,
    ) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(SourceActorMessage::PublishEdgeWatermark {
                    offset,
                    stream,
                    input_partition,
                })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use chrono::DateTime;

    use super::*;
    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::watermark::BucketConfig;
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
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: 1, // partitions is always one for source
                ot_bucket: ot_bucket_name,
                hb_bucket: hb_bucket_name,
            },
            to_vertex_bucket_config: vec![],
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

        let handle = SourceWatermarkHandle::new(js_context.clone(), &source_config)
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
            .publish_source_watermark(&messages)
            .await
            .expect("Failed to publish source watermark");

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
                .get("source_vertex-0")
                .await
                .expect("Failed to get wmb");
            if wmb.is_some() {
                let wmb: WMB = wmb.unwrap().try_into().unwrap();
                assert_eq!(wmb.watermark, 60000);
                wmb_found = true;
                break;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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

        let source_config = SourceWatermarkConfig {
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: 2,
                ot_bucket: source_ot_bucket_name,
                hb_bucket: source_hb_bucket_name,
            },
            to_vertex_bucket_config: vec![BucketConfig {
                vertex: "edge_vertex",
                partitions: 2,
                ot_bucket: edge_ot_bucket_name,
                hb_bucket: edge_hb_bucket_name,
            }],
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

        let handle = SourceWatermarkHandle::new(js_context.clone(), &source_config)
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
                .publish_source_watermark(&messages)
                .await
                .expect("Failed to publish source watermark");

            let offset = Offset::Int(IntOffset {
                offset: i,
                partition_idx: 0,
            });
            handle
                .publish_source_edge_watermark(stream.clone(), offset, 0)
                .await
                .expect("Failed to publish edge watermark");

            // check if the watermark is published
            let wmb = ot_bucket
                .get("source_vertex-edge_vertex-0")
                .await
                .expect("Failed to get wmb");
            if wmb.is_some() {
                let wmb: WMB = wmb.unwrap().try_into().unwrap();
                assert_ne!(wmb.watermark, -1);
                wmb_found = true;
                break;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        if !wmb_found {
            panic!("Failed to get watermark");
        }

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
