use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{
    consumer::PullConsumer, AckKind, Context, Message as JetstreamMessage,
};
use prost::Message as ProtoMessage;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, Metadata, Offset, ReadAck};
use crate::metrics::{
    pipeline_forward_metric_labels, pipeline_isb_metric_labels, pipeline_metrics,
};
use crate::shared::grpc::utc_from_timestamp;
use crate::tracker::TrackerHandle;
use crate::watermark::isb::ISBWatermarkHandle;
use crate::Result;

/// The JetstreamReader is a handle to the background actor that continuously fetches messages from Jetstream.
/// It can be used to cancel the background task and stop reading from Jetstream.
/// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel is only closed when all the senders are dropped.
/// Storing the Sender end of channel in this struct would make it difficult to close the channel with `cancel` method.
#[derive(Clone)]
pub(crate) struct JetstreamReader {
    stream: Stream,
    config: BufferReaderConfig,
    consumer: PullConsumer,
    tracker_handle: TrackerHandle,
    batch_size: usize,
    watermark_handle: Option<ISBWatermarkHandle>,
}

/// JSWrappedMessage is a wrapper around the Jetstream message that includes the
/// partition index and the vertex name.
struct JSWrappedMessage {
    partition_idx: u16,
    message: async_nats::jetstream::Message,
    vertex_name: String,
}

impl TryFrom<JSWrappedMessage> for Message {
    type Error = Error;

    fn try_from(value: JSWrappedMessage) -> Result<Self> {
        let msg_info = value.message.info().map_err(|e| {
            Error::ISB(format!(
                "Failed to get message info from Jetstream: {:?}",
                e
            ))
        })?;

        let proto_message =
            numaflow_pb::objects::isb::Message::decode(value.message.payload.clone())
                .map_err(|e| Error::Proto(e.to_string()))?;

        let header = proto_message
            .header
            .ok_or(Error::Proto("Missing header".to_string()))?;
        let body = proto_message
            .body
            .ok_or(Error::Proto("Missing body".to_string()))?;
        let message_info = header
            .message_info
            .ok_or(Error::Proto("Missing message_info".to_string()))?;
        let offset = Offset::Int(IntOffset::new(
            msg_info.stream_sequence as i64,
            value.partition_idx,
        ));

        Ok(Message {
            keys: Arc::from(header.keys.into_boxed_slice()),
            tags: None,
            value: body.payload.into(),
            offset: offset.clone(),
            event_time: utc_from_timestamp(message_info.event_time),
            id: MessageID {
                vertex_name: value.vertex_name.into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: header.headers,
            watermark: None,
            metadata: Some(Metadata {
                previous_vertex: header
                    .id
                    .ok_or(Error::Proto("Missing id".to_string()))?
                    .vertex_name,
            }),
        })
    }
}

impl JetstreamReader {
    pub(crate) async fn new(
        stream: Stream,
        js_ctx: Context,
        config: BufferReaderConfig,
        tracker_handle: TrackerHandle,
        batch_size: usize,
        watermark_handle: Option<ISBWatermarkHandle>,
    ) -> Result<Self> {
        let mut config = config;

        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&stream.name, &stream.name)
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
            stream,
            config: config.clone(),
            consumer,
            tracker_handle,
            batch_size,
            watermark_handle,
        })
    }

    /// streaming_read is a background task that continuously fetches messages from Jetstream and
    /// emits them on a channel. When we encounter an error, we log the error and return from the
    /// function. This drops the sender end of the channel. The closing of the channel should propagate
    /// to the receiver end and the receiver should exit gracefully. Within the loop, we only consider
    /// cancellationToken cancellation during the permit reservation and fetching messages,
    /// since rest of the operations should finish immediately.
    pub(crate) async fn streaming_read(
        &self,
        cancel_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (messages_tx, messages_rx) = mpsc::channel(2 * self.batch_size);

        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let consumer = self.consumer.clone();
            let stream = self.stream.clone();
            let config = self.config.clone();
            let tracker_handle = self.tracker_handle.clone();
            let cancel_token = cancel_token.clone();
            let watermark_handle = self.watermark_handle.clone();

            async move {
                let labels = pipeline_forward_metric_labels("Sink", Some(stream.name));

                let mut message_stream = consumer.messages().await.map_err(|e| {
                    Error::ISB(format!(
                        "Failed to get message stream from Jetstream: {:?}",
                        e
                    ))
                })?;

                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => { // should we drain from the stream when token is cancelled?
                            info!(?stream, "Cancellation token received, stopping the reader.");
                            break;
                        }
                        message = message_stream.next() => {
                            let Some(message) = message else {
                                // stream has been closed because we got none
                                info!(?stream, "Stream has been closed");
                                break;
                            };

                            let jetstream_message = match message {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(?e, ?stream, "Failed to fetch messages from the Jetstream");
                                    continue;
                                }
                            };

                            let js_message = JSWrappedMessage {
                                partition_idx: stream.partition,
                                message: jetstream_message.clone(),
                                vertex_name: get_vertex_name().to_string(),
                            };

                            let mut message: Message = match js_message.try_into() {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(
                                        ?e, ?stream, ?jetstream_message,
                                        "Failed to parse message payload received from Jetstream",
                                    );
                                    continue;
                                }
                            };

                            if let Some(watermark_handle) = watermark_handle.as_ref() {
                                let watermark = watermark_handle.fetch_watermark(message.offset.clone()).await?;
                                message.watermark = Some(watermark);
                            }

                            // Insert the message into the tracker and wait for the ack to be sent back.
                            let (ack_tx, ack_rx) = oneshot::channel();
                            tracker_handle.insert(&message, ack_tx).await?;

                            tokio::spawn(Self::start_work_in_progress(
                                jetstream_message,
                                ack_rx,
                                config.wip_ack_interval,
                            ));

                            let offset = message.offset.clone();
                            if let Err(e) = messages_tx.send(message).await {
                                // nak the read message and return
                                tracker_handle.discard(offset).await?;
                                return Err(Error::ISB(format!(
                                    "Failed to send message to receiver: {:?}",
                                    e
                                )));
                            }

                            pipeline_metrics()
                                .forwarder
                                .read_total
                                .get_or_create(labels)
                                .inc();
                        }
                    }
                }
                Ok(())
            }
        });
        Ok((ReceiverStream::new(messages_rx), handle))
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
        let start = Instant::now();

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
                    pipeline_metrics()
                        .forwarder
                        .ack_time
                        .get_or_create(pipeline_isb_metric_labels())
                        .observe(start.elapsed().as_micros() as f64);

                    pipeline_metrics()
                        .forwarder
                        .ack_total
                        .get_or_create(pipeline_isb_metric_labels())
                        .inc();
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

    pub(crate) async fn pending(&mut self) -> Result<Option<usize>> {
        let x = self.consumer.info().await.map_err(|e| {
            Error::ISB(format!(
                "Failed to get consumer info for stream {}: {}",
                self.stream.name, e
            ))
        })?;
        Ok(Some(x.num_pending as usize + x.num_ack_pending))
    }

    pub(crate) fn name(&self) -> &'static str {
        self.stream.name
    }
}

impl fmt::Display for JetstreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JetstreamReader {{ stream_name: {}, partition_idx: {}, config: {:?} }}",
            self.stream, self.stream.partition, self.config
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;
    use tokio::time::sleep;

    use super::*;
    use crate::message::{Message, MessageID};

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_read() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_jetstream_read", "test", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
        };
        let js_reader = JetstreamReader::new(
            stream.clone(),
            context.clone(),
            buf_reader_config,
            TrackerHandle::new(None, None),
            500,
            None,
        )
        .await
        .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        for i in 0..10 {
            let message = Message {
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                headers: HashMap::new(),
                metadata: None,
            };
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        let mut buffer = vec![];
        for _ in 0..10 {
            let Some(val) = js_reader_rx.next().await else {
                break;
            };
            buffer.push(val);
        }

        assert_eq!(
            buffer.len(),
            10,
            "Expected 10 messages from the jetstream reader"
        );

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_ack() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None, None);

        let js_stream = Stream::new("test-ack", "test", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(js_stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: js_stream.to_string(),
                subjects: vec![js_stream.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(js_stream.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                js_stream.name.to_string(),
            )
            .await
            .unwrap();

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
        };
        let js_reader = JetstreamReader::new(
            js_stream.clone(),
            context.clone(),
            buf_reader_config,
            tracker_handle.clone(),
            1,
            None,
        )
        .await
        .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        let mut offsets = vec![];
        // write 5 messages
        for i in 0..5 {
            let message = Message {
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i + 1, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("{}-0", i + 1).into(),
                    index: i as i32,
                },
                headers: HashMap::new(),
                metadata: None,
            };
            offsets.push(message.offset.clone());
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(js_stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        for _ in 0..5 {
            let Some(_val) = js_reader_rx.next().await else {
                break;
            };
        }

        // after reading messages remove from the tracker so that the messages are acked
        for offset in offsets {
            tracker_handle.delete(offset).await.unwrap();
        }

        // wait until the tracker becomes empty, don't wait more than 1 second
        tokio::time::timeout(Duration::from_secs(1), async {
            while !tracker_handle.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Tracker is not empty after 1 second");

        let mut consumer: PullConsumer = context
            .get_consumer_from_stream(js_stream.name, js_stream.name)
            .await
            .unwrap();

        let consumer_info = consumer.info().await.unwrap();

        assert_eq!(consumer_info.num_pending, 0);
        assert_eq!(consumer_info.num_ack_pending, 0);

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();

        context.delete_stream(js_stream.name).await.unwrap();
    }
}
