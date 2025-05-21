use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{
    AckKind, Context, Message as JetstreamMessage, consumer::PullConsumer,
};
use backoff::retry::Retry;
use backoff::strategy::fixed;
use chrono::Utc;
use prost::Message as ProtoMessage;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracing::{error, info};

use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, MessageType, Metadata, Offset, ReadAck};
use crate::metrics::{
    pipeline_forward_metric_labels, pipeline_isb_metric_labels, pipeline_metrics,
};
use crate::shared::grpc::utc_from_timestamp;
use crate::tracker::TrackerHandle;
use crate::watermark::isb::ISBWatermarkHandle;
use crate::{Result, metrics};

const ACK_RETRY_INTERVAL: u64 = 100;
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;
const MAX_ACK_PENDING: usize = 25000;

/// The JetStreamReader is a handle to the background actor that continuously fetches messages from JetStream.
/// It can be used to cancel the background task and stop reading from JetStream.
///
/// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel
/// is only closed when all the senders are dropped. Storing the Sender end of channel in this struct would make it
/// difficult to close the channel with `cancel` method.
///
/// Error handling and shutdown: The JetStreamReader will stop reading from JetStream when the cancellation token is
/// cancelled(critical error in downstream or SIGTERM), there can't be any other cases where the JetStreamReader
/// should stop reading messages. We just drop the tokio stream so that the downstream components can stop gracefully
/// and before exiting we make sure all the work-in-progress(tasks) are completed.
#[derive(Clone)]
pub(crate) struct JetStreamReader {
    stream: Stream,
    config: BufferReaderConfig,
    consumer: PullConsumer,
    tracker_handle: TrackerHandle,
    batch_size: usize,
    watermark_handle: Option<ISBWatermarkHandle>,
    vertex_type: String,
}

/// JSWrappedMessage is a wrapper around the JetStream message that includes the
/// partition index and the vertex name.
#[derive(Debug)]
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
                "Failed to get message info from JetStream: {:?}",
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
            typ: header.kind.into(),
            keys: Arc::from(header.keys.into_boxed_slice()),
            tags: None,
            value: body.payload.into(),
            offset: offset.clone(),
            event_time: message_info.event_time.map(utc_from_timestamp).unwrap(),
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

impl JetStreamReader {
    pub(crate) async fn new(
        vertex_type: String,
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
            vertex_type,
            stream,
            config: config.clone(),
            consumer,
            tracker_handle,
            batch_size,
            watermark_handle,
        })
    }

    /// streaming_read is a background task that continuously fetches messages from JetStream and
    /// emits them on a channel. When we encounter an error, we log the error and return from the
    /// function. This drops the sender end of the channel. The closing of the channel should propagate
    /// to the receiver end and the receiver should exit gracefully. Within the loop, we only consider
    /// cancellationToken cancellation during the permit reservation and fetching messages,
    /// since rest of the operations should finish immediately.
    pub(crate) async fn streaming_read(
        mut self,
        cancel_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (messages_tx, messages_rx) = mpsc::channel(2 * self.batch_size);

        let handle: JoinHandle<Result<()>> = tokio::spawn({
            async move {
                let mut labels = pipeline_forward_metric_labels(&self.vertex_type).clone();
                let semaphore = Arc::new(Semaphore::new(MAX_ACK_PENDING));
                let mut processed_msgs_count: usize = 0;
                let mut last_logged_at = Instant::now();

                labels.push((
                    metrics::PIPELINE_PARTITION_NAME_LABEL.to_string(),
                    self.stream.name.to_string(),
                ));

                let mut message_stream = self.consumer.messages().await.map_err(|e| {
                    Error::ISB(format!(
                        "Failed to get message stream from Jetstream: {:?}",
                        e
                    ))
                })?;

                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => { // should we drain from the stream when token is cancelled?
                            info!(stream=?self.stream, "Cancellation token received, stopping the reader.");
                            break;
                        }
                        message = message_stream.next() => {
                            let Some(message) = message else {
                                // stream has been closed because we got none
                                info!(stream=?self.stream, "Stream has been closed");
                                break;
                            };

                            let jetstream_message = match message {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(?e, stream=?self.stream, "Failed to fetch messages from the Jetstream");
                                    continue;
                                }
                            };

                            let js_message = JSWrappedMessage {
                                partition_idx: self.stream.partition,
                                message: jetstream_message.clone(),
                                vertex_name: get_vertex_name().to_string(),
                            };

                            let mut message: Message = js_message.try_into().map_err(|e| {
                                Error::ISB(format!("Failed to convert JetStream message to Message: {:?}", e))
                            })?;

                            // we can ignore the wmb messages
                            if let MessageType::WMB = message.typ {
                                // ack the message and continue
                                jetstream_message.ack().await.map_err(|e| {
                                    Error::ISB(format!("Failed to ack the wmb message: {:?}", e))
                                })?;
                                continue;
                            }

                            if let Some(watermark_handle) = self.watermark_handle.as_mut() {
                                let watermark = watermark_handle.fetch_watermark(message.offset.clone()).await;
                                message.watermark = Some(watermark);
                            }

                            // Insert the message into the tracker and wait for the ack to be sent back.
                            let (ack_tx, ack_rx) = oneshot::channel();
                            self.tracker_handle.insert(&message, ack_tx).await?;

                            // Reserve a permit before sending the message to the channel.
                            let permit = Arc::clone(&semaphore).acquire_owned().await.expect("Failed to acquire semaphore permit");
                            tokio::spawn(Self::start_work_in_progress(
                                message.offset.clone(),
                                jetstream_message,
                                ack_rx,
                                self.config.wip_ack_interval,
                                permit,
                                cancel_token.clone(),
                            ));

                            messages_tx.send(message).await.expect("Failed to send message to channel");
                            pipeline_metrics()
                                .forwarder
                                .read_total
                                .get_or_create(&labels)
                                .inc();

                            processed_msgs_count += 1;
                            if last_logged_at.elapsed().as_secs() >= 1 {
                                info!(
                                    "Processed {} messages in {:?}",
                                    processed_msgs_count,
                                    Utc::now()
                                );
                                processed_msgs_count = 0;
                                last_logged_at = Instant::now();
                            }
                        }
                    }
                }
                info!(stream=?self.stream, "Jetstream reader stopped, waiting for ack tasks to complete");
                // wait for all the permits to be released before returning
                let _permit = Arc::clone(&semaphore)
                    .acquire_many_owned(MAX_ACK_PENDING as u32)
                    .await
                    .expect("Failed to acquire semaphore permit");
                info!(stream=?self.stream, "All inflight messages are successfully acked/nacked");
                Ok(())
            }
        });
        Ok((ReceiverStream::new(messages_rx), handle))
    }

    // Intended to be run as background task which will continuously send InProgress acks to Jetstream.
    // We will continuously retry if there is an error in acknowledging the message as work-in-progress.
    // If the sender end of the ack_rx channel was dropped before sending a final Ack or Nak (due to some unhandled/unknown failure), we will send a Nak to Jetstream.
    async fn start_work_in_progress(
        offset: Offset,
        msg: JetstreamMessage,
        mut ack_rx: oneshot::Receiver<ReadAck>,
        tick: Duration,
        _permit: OwnedSemaphorePermit, // permit to release after acking the offsets.
        cancel_token: CancellationToken,
    ) {
        let start = Instant::now();
        let mut interval = time::interval_at(start + tick, tick);

        loop {
            let wip = async {
                interval.tick().await;
                let ack_result = msg.ack_with(AckKind::Progress).await;
                if let Err(e) = ack_result {
                    error!(
                        ?e,
                        ?offset,
                        "Failed to send InProgress Ack to Jetstream for message"
                    );
                }
            };

            let ack = tokio::select! {
                ack = &mut ack_rx => ack,
                _ = wip => continue,
            };

            let ack = ack.unwrap_or_else(|e| {
                error!(
                    ?e,
                    ?offset,
                    "Received error while waiting for Ack oneshot channel"
                );
                ReadAck::Nak
            });

            match ack {
                ReadAck::Ack => {
                    Self::invoke_ack_with_retry(&msg, AckKind::Ack, &cancel_token, offset.clone())
                        .await;

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
                    Self::invoke_ack_with_retry(
                        &msg,
                        AckKind::Nak(None),
                        &cancel_token,
                        offset.clone(),
                    )
                    .await;

                    warn!(?offset, "Sent Nak to Jetstream for message");
                    return;
                }
            }
        }
    }

    // invokes the ack with infinite retries until the cancellation token is cancelled.
    async fn invoke_ack_with_retry(
        msg: &JetstreamMessage,
        ack_kind: AckKind,
        cancel_token: &CancellationToken,
        offset: Offset,
    ) {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let _ = Retry::retry(
            interval,
            async || {
                let result = match msg.ack_with(ack_kind).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!(?e, "Failed to send {:?} to Jetstream for message", ack_kind);
                        Err(Error::Connection(format!(
                            "Failed to send {:?}: {:?}",
                            ack_kind, e
                        )))
                    }
                };
                if result.is_err() && cancel_token.is_cancelled() {
                    error!(
                        ?result,
                        ?offset,
                        "Cancellation token received, stopping the {:?} retry loop",
                        ack_kind
                    );
                    return Ok(());
                }
                result
            },
            |_: &Error| true,
        )
        .await;
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

impl fmt::Display for JetStreamReader {
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
        let tracker = TrackerHandle::new(None, None);
        let js_reader = JetStreamReader::new(
            "Map".to_string(),
            stream.clone(),
            context.clone(),
            buf_reader_config,
            tracker.clone(),
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

        let mut offsets = vec![];
        for i in 0..10 {
            let offset = Offset::Int(IntOffset::new(i + 1, 0));
            offsets.push(offset.clone());
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset,
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

        for offset in offsets {
            tracker.discard(offset).await.unwrap();
        }
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
        let js_reader = JetStreamReader::new(
            "Map".to_string(),
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
                typ: Default::default(),
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

    #[tokio::test]
    async fn test_child_tasks_not_aborted() {
        use tokio::task;
        use tokio::time::{Duration, sleep};

        // Parent task
        let parent_task = task::spawn(async {
            // Spawn a child task
            task::spawn(async {
                for _ in 1..=5 {
                    sleep(Duration::from_secs(1)).await;
                }
            });

            // Parent task logic
            sleep(Duration::from_secs(2)).await;
        });

        drop(parent_task);

        // Give some time to observe the child task behavior
        sleep(Duration::from_secs(8)).await;
    }
}
