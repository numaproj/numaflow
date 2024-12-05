use std::fmt;
use std::time::Duration;

use async_nats::jetstream::{
    consumer::PullConsumer, AckKind, Context, Message as JetstreamMessage,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::pipeline::isb::BufferReaderConfig;
use crate::config::pipeline::PipelineConfig;
use crate::error::Error;
use crate::message::{IntOffset, Message, Offset, ReadAck};
use crate::metrics::{
    pipeline_forward_metric_labels, pipeline_isb_metric_labels, pipeline_metrics,
};
use crate::tracker::TrackerHandle;
use crate::Result;

/// The JetstreamReader is a handle to the background actor that continuously fetches messages from Jetstream.
/// It can be used to cancel the background task and stop reading from Jetstream.
/// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel is only closed when all the senders are dropped.
/// Storing the Sender end of channel in this struct would make it difficult to close the channel with `cancel` method.
#[derive(Clone)]
pub(crate) struct JetstreamReader {
    stream_name: &'static str,
    partition_idx: u16,
    config: BufferReaderConfig,
    consumer: PullConsumer,
    tracker_handle: TrackerHandle,
}

impl JetstreamReader {
    pub(crate) async fn new(
        stream_name: &'static str,
        partition_idx: u16,
        js_ctx: Context,
        config: BufferReaderConfig,
        tracker_handle: TrackerHandle,
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
            stream_name,
            partition_idx,
            config: config.clone(),
            consumer,
            tracker_handle,
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
        pipeline_config: &PipelineConfig,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (messages_tx, messages_rx) = mpsc::channel(2 * pipeline_config.batch_size);

        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let consumer = self.consumer.clone();
            let partition_idx = self.partition_idx;
            let config = self.config.clone();
            let tracker_handle = self.tracker_handle.clone();
            let cancel_token = cancel_token.clone();

            let stream_name = self.stream_name;
            async move {
                let labels = pipeline_forward_metric_labels("Sink", Some(stream_name));

                let mut message_stream = consumer.messages().await.map_err(|e| {
                    Error::ISB(format!(
                        "Failed to get message stream from Jetstream: {:?}",
                        e
                    ))
                })?;

                let mut start_time = Instant::now();
                let mut total_messages = 0;
                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => { // should we drain from the stream when token is cancelled?
                            info!(?stream_name, "Cancellation token received, stopping the reader.");
                            break;
                        }
                        message = message_stream.next() => {
                            let Some(message) = message else {
                                // stream has been closed because we got none
                                info!(?stream_name, "Stream has been closed");
                                break;
                            };

                            let jetstream_message = match message {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(?e, ?stream_name, "Failed to fetch messages from the Jetstream");
                                    continue;
                                }
                            };
                            let msg_info = match jetstream_message.info() {
                                Ok(info) => info,
                                Err(e) => {
                                    error!(?e, ?stream_name, "Failed to get message info from Jetstream");
                                    continue;
                                }
                            };

                            let mut message: Message = match jetstream_message.payload.clone().try_into() {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(
                                        ?e, ?stream_name, ?jetstream_message,
                                        "Failed to parse message payload received from Jetstream",
                                    );
                                    continue;
                                }
                            };

                            message.offset = Some(Offset::Int(IntOffset::new(
                                msg_info.stream_sequence,
                                partition_idx,
                            )));

                            // Insert the message into the tracker and wait for the ack to be sent back.
                            let (ack_tx, ack_rx) = oneshot::channel();
                            tracker_handle.insert(message.id.offset.clone(), ack_tx).await?;

                            tokio::spawn(Self::start_work_in_progress(
                                jetstream_message,
                                ack_rx,
                                config.wip_ack_interval,
                            ));

                            messages_tx.send(message).await.map_err(|e| {
                                Error::ISB(format!("Error while sending message to channel: {:?}", e))
                            })?;

                            pipeline_metrics()
                                .forwarder
                                .read_total
                                .get_or_create(labels)
                                .inc();

                            if start_time.elapsed() >= Duration::from_millis(1000) {
                                info!(
                                    "Total messages read from Jetstream in {:?} seconds: {}",
                                    start_time.elapsed(),
                                    total_messages
                                );
                                start_time = Instant::now();
                                total_messages = 0;
                            }
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
}

impl fmt::Display for JetstreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JetstreamReader {{ stream_name: {}, partition_idx: {}, config: {:?} }}",
            self.stream_name, self.partition_idx, self.config
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;

    use super::*;
    use crate::message::{Message, MessageID};
    use crate::pipeline::isb::jetstream::writer::JetstreamWriter;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_read() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_jetstream_read";
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
            wip_ack_interval: Duration::from_millis(5),
        };
        let js_reader = JetstreamReader::new(
            stream_name,
            0,
            context.clone(),
            buf_reader_config,
            TrackerHandle::new(),
        )
        .await
        .unwrap();

        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLW91dCIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJjcmVhdGlvblRpbWVzdGFtcCI6bnVsbH0sInNwZWMiOnsibmFtZSI6Im91dCIsInNpbmsiOnsiYmxhY2tob2xlIjp7fSwicmV0cnlTdHJhdGVneSI6eyJvbkZhaWx1cmUiOiJyZXRyeSJ9fSwibGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJmcm9tRWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoib3V0IiwiY29uZGl0aW9ucyI6bnVsbCwiZnJvbVZlcnRleFR5cGUiOiJTb3VyY2UiLCJmcm9tVmVydGV4UGFydGl0aW9uQ291bnQiOjEsImZyb21WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJTaW5rIiwidG9WZXJ0ZXhQYXJ0aXRpb25Db3VudCI6MSwidG9WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9fV0sIndhdGVybWFyayI6eyJtYXhEZWxheSI6IjBzIn19LCJzdGF0dXMiOnsicGhhc2UiOiIiLCJyZXBsaWNhcyI6MCwiZGVzaXJlZFJlcGxpY2FzIjowLCJsYXN0U2NhbGVkQXQiOm51bGx9fQ==".to_string();

        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config = PipelineConfig::load(pipeline_cfg_base64, env_vars).unwrap();
        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone(), &pipeline_config)
            .await
            .unwrap();

        let writer_cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            vec![(stream_name.to_string(), 0)],
            Default::default(),
            context.clone(),
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
            let message_bytes: BytesMut = message.try_into().unwrap();
            writer
                .write((stream_name.to_string(), 0), message_bytes.into())
                .await
                .await
                .unwrap();
        }

        // Cancel the token to exit the retry loop
        writer_cancel_token.cancel();

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
            "Expected 10 messages from the Jestream reader"
        );

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();

        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_ack() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_ack";
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
            wip_ack_interval: Duration::from_millis(5),
        };
        let js_reader = JetstreamReader::new(
            stream_name,
            0,
            context.clone(),
            buf_reader_config,
            TrackerHandle::new(),
        )
        .await
        .unwrap();

        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLW91dCIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJjcmVhdGlvblRpbWVzdGFtcCI6bnVsbH0sInNwZWMiOnsibmFtZSI6Im91dCIsInNpbmsiOnsiYmxhY2tob2xlIjp7fSwicmV0cnlTdHJhdGVneSI6eyJvbkZhaWx1cmUiOiJyZXRyeSJ9fSwibGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJmcm9tRWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoib3V0IiwiY29uZGl0aW9ucyI6bnVsbCwiZnJvbVZlcnRleFR5cGUiOiJTb3VyY2UiLCJmcm9tVmVydGV4UGFydGl0aW9uQ291bnQiOjEsImZyb21WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJTaW5rIiwidG9WZXJ0ZXhQYXJ0aXRpb25Db3VudCI6MSwidG9WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9fV0sIndhdGVybWFyayI6eyJtYXhEZWxheSI6IjBzIn19LCJzdGF0dXMiOnsicGhhc2UiOiIiLCJyZXBsaWNhcyI6MCwiZGVzaXJlZFJlcGxpY2FzIjowLCJsYXN0U2NhbGVkQXQiOm51bGx9fQ==".to_string();

        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config = PipelineConfig::load(pipeline_cfg_base64, env_vars).unwrap();
        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone(), &pipeline_config)
            .await
            .unwrap();

        let writer_cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            vec![(stream_name.to_string(), 0)],
            Default::default(),
            context.clone(),
            writer_cancel_token.clone(),
        );

        // write 5 messages
        for i in 0..5 {
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
            let message_bytes: BytesMut = message.try_into().unwrap();
            writer
                .write((stream_name.to_string(), 0), message_bytes.into())
                .await
                .await
                .unwrap();
        }
        // Cancel the token to exit the retry loop
        writer_cancel_token.cancel();

        for _ in 0..5 {
            let Some(_val) = js_reader_rx.next().await else {
                break;
            };
        }

        let mut consumer: PullConsumer = context
            .get_consumer_from_stream(stream_name, stream_name)
            .await
            .unwrap();

        let consumer_info = consumer.info().await.unwrap();

        assert_eq!(consumer_info.num_pending, 0);
        assert_eq!(consumer_info.num_ack_pending, 0);

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();

        context.delete_stream(stream_name).await.unwrap();
    }
}
