use std::time::Duration;

use async_nats::jetstream::{
    consumer::PullConsumer, AckKind, Context, Message as JetstreamMessage,
};
use log::info;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::config::pipeline::isb::BufferReaderConfig;
use crate::config::pipeline::PipelineConfig;
use crate::error::Error;
use crate::message::{IntOffset, Message, Offset, ReadAck, ReadMessage};
use crate::metrics::{forward_pipeline_metrics, pipeline_forward_read_metric_labels};
use crate::Result;

// The JetstreamReader is a handle to the background actor that continuously fetches messages from Jetstream.
// It can be used to cancel the background task and stop reading from Jetstream.
// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel is only closed when all the senders are dropped.
// Storing the Sender end of channel in this struct would make it difficult to close the channel with `cancel` method.
#[derive(Clone)]
pub(crate) struct JetstreamReader {
    pipeline_config: PipelineConfig,
    partition_idx: u16,
    config: BufferReaderConfig,
    consumer: PullConsumer,
}

impl JetstreamReader {
    pub(crate) async fn new(
        pipeline_config: PipelineConfig,
        stream_name: String,
        partition_idx: u16,
        js_ctx: Context,
        config: BufferReaderConfig,
    ) -> Result<Self> {
        let mut config = config;

        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&stream_name, &stream_name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer for stream {}", e)))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer info {}", e)))?;

        // Calculate inProgressTickSeconds based on the ack_wait_seconds.
        let ack_wait_seconds = consumer_info.config.ack_wait.as_secs();
        let wip_ack_interval = Duration::from_secs(std::cmp::max(
            config.wip_ack_interval.as_secs(),
            ack_wait_seconds * 2 / 3,
        ));
        config.wip_ack_interval = wip_ack_interval;

        Ok(Self {
            pipeline_config,
            partition_idx,
            config: config.clone(),
            consumer,
        })
    }

    // When we encounter an error, we log the error and return from the function. This drops the sender end of the channel.
    // The closing of the channel should propagate to the receiver end and the receiver should exit gracefully.
    // Within the loop, we only consider cancellationToken cancellation during the permit reservation and fetching messages,
    // since rest of the operations should finish immediately.
    pub(crate) async fn start(
        &self,
        cancel_token: CancellationToken,
    ) -> Result<(Receiver<ReadMessage>, JoinHandle<Result<()>>)> {
        let (messages_tx, messages_rx) = mpsc::channel(2 * self.config.batch_size);

        let partition: &str = self
            .pipeline_config
            .from_vertex_config
            .first()
            .unwrap()
            .reader_config
            .streams
            .first()
            .unwrap()
            .0
            .as_ref();

        let labels = pipeline_forward_read_metric_labels(
            self.pipeline_config.pipeline_name.as_ref(),
            partition,
            self.pipeline_config.vertex_name.as_ref(),
            "Sink",
            self.pipeline_config.replica,
        );

        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let this = self.clone();
            async move {
                let chunk_stream = this
                    .consumer
                    .messages()
                    .await
                    .unwrap()
                    .chunks_timeout(this.config.batch_size, this.config.read_timeout);

                tokio::pin!(chunk_stream);

                let mut read_time = Instant::now();
                let mut count = 0;

                let mut chunk_time = Instant::now();
                let mut total_count = 0;
                while let Some(messages) = chunk_stream.next().await {
                    let labels = labels.clone();
                    let this = this.clone();
                    for message in messages {
                        let jetstream_message = match message {
                            Ok(message) => message,
                            Err(e) => {
                                error!(?e, "Failed to fetch messages from the Jetstream");
                                continue;
                            }
                        };

                        let msg_info = match jetstream_message.info() {
                            Ok(info) => info,
                            Err(e) => {
                                error!(?e, "Failed to get message info from Jetstream");
                                continue;
                            }
                        };

                        let mut message: Message =
                            match jetstream_message.payload.clone().try_into() {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(
                                        ?e,
                                        "Failed to parse message payload received from Jetstream"
                                    );
                                    continue;
                                }
                            };

                        message.offset = Some(Offset::Int(IntOffset::new(
                            msg_info.stream_sequence,
                            this.partition_idx,
                        )));

                        let (ack_tx, ack_rx) = oneshot::channel();

                        tokio::spawn(Self::start_work_in_progress(
                            jetstream_message,
                            ack_rx,
                            this.config.wip_ack_interval,
                        ));

                        let read_message = ReadMessage {
                            message,
                            ack: ack_tx,
                        };

                        if messages_tx.send(read_message).await.is_err() {
                            error!("Failed to send message to the channel");
                            return Ok(());
                        }

                        forward_pipeline_metrics()
                            .forwarder
                            .data_read
                            .get_or_create(&labels)
                            .inc();
                        count += 1;
                        total_count += 1;
                    }
                    if cancel_token.is_cancelled() {
                        warn!("Cancellation token is cancelled. Exiting JetstreamReader");
                        break;
                    }

                    info!(
                        "Read {} messages from Jetstream in {}ms",
                        count,
                        read_time.elapsed().as_millis()
                    );
                    read_time = Instant::now();
                    count = 0;

                    if chunk_time.elapsed().as_millis() >= 1000 {
                        info!(
                            "Total {} messages read from Jetstream in {}ms",
                            total_count,
                            chunk_time.elapsed().as_millis()
                        );
                        chunk_time = Instant::now();
                        total_count = 0;
                    }
                }
                Ok(())
            }
        });
        Ok((messages_rx, handle))
    }

    // Intended to be run as background task which will continuously send InProgress acks to Jetstream.
    // We will continuously retry if there is an error in acknowledging the message as work-in-progress.
    // If the sender end of the ack_rx channel was dropped before sending a final Ack or Nak (due to some unhandled/unknown failure), we will send a Nak to Jetstream.
    async fn start_work_in_progress(
        msg: JetstreamMessage,
        mut ack_rx: oneshot::Receiver<ReadAck>,
        tick: Duration,
    ) {
        let mut interval = time::interval_at(Instant::now() + tick, tick);

        loop {
            let wip = async {
                interval.tick().await;
                let ack_result = msg.ack_with(AckKind::Progress).await;
                if let Err(e) = ack_result {
                    // We expect that the ack in the next iteration will be successful.
                    // If its some unrecoverable Jetstream error, the fetching messages in the JestreamReader implementation should also fail and cause the system to shut down.
                    error!(?e, "Failed to send InProgress Ack to Jetstream for message");
                }
            };

            let ack = tokio::select! {
                ack = &mut ack_rx => ack,
                _ = wip => continue,
            };

            let ack = ack.unwrap_or_else(|e| {
                error!(?e, "Received error while waiting for Ack oneshot channel");
                ReadAck::Nak
            });

            match ack {
                ReadAck::Ack => {
                    let ack_result = msg.ack().await;
                    if let Err(e) = ack_result {
                        error!(?e, "Failed to send Ack to Jetstream for message");
                    }
                    return;
                }
                ReadAck::Nak => {
                    let ack_result = msg.ack_with(AckKind::Nak(None)).await;
                    if let Err(e) = ack_result {
                        error!(?e, "Failed to send Nak to Jetstream for message");
                    }
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;
    use std::collections::HashMap;

    use super::*;
    use crate::message::{Message, MessageID, Offset};
    use crate::pipeline::isb::jetstream::writer::JetstreamWriter;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_read() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_cancellation-2";
        context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .unwrap();

        let buf_reader_config = BufferReaderConfig {
            partitions: 0,
            streams: vec![],
            batch_size: 2,
            read_timeout: Duration::from_millis(1000),
            wip_ack_interval: Duration::from_millis(5),
        };
        let js_reader = JetstreamReader::new(
            Default::default(),
            stream_name.to_string(),
            0,
            context.clone(),
            buf_reader_config,
        )
        .await
        .unwrap();
        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) =
            js_reader.start(reader_cancel_token.clone()).await.unwrap();

        let writer_cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            500,
            writer_cancel_token.clone(),
        );

        for i in 0..10 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            };
            let (success_tx, success_rx) = oneshot::channel::<Result<Offset>>();
            let message_bytes: BytesMut = message.try_into().unwrap();
            writer.write(message_bytes.into(), success_tx).await;
            success_rx.await.unwrap().unwrap();
        }
        // Cancel the token to exit the retry loop
        writer_cancel_token.cancel();

        let mut buffer = vec![];
        for _ in 0..10 {
            let Some(val) = js_reader_rx.recv().await else {
                break;
            };
            buffer.push(val);
        }
        assert_eq!(
            buffer.len(),
            10,
            "Expected 10 messages from the Jestream reader"
        );

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap();
        assert!(js_reader_rx.is_closed());

        context.delete_stream(stream_name).await.unwrap();
    }
}
