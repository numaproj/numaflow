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
