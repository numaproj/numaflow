use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::stream::RetentionPolicy::Limits;
use bytes::BytesMut;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::config::pipeline::ToVertexConfig;
use crate::config::pipeline::isb::{BufferFullStrategy, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, Offset};
use crate::metrics::{pipeline_isb_metric_labels, pipeline_metrics};
use crate::shared::forward;
use crate::tracker::TrackerHandle;
use crate::watermark::WatermarkHandle;

const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 1;

/// Writes to JetStream ISB. Exposes both write and blocking methods to write messages.
/// It accepts a cancellation token to stop infinite retries during shutdown.
/// JetstreamWriter is one to many mapping of streams to write messages to. It also
/// maintains the buffer usage metrics for each stream.
///
/// Error handling and shutdown: Unlike udf components we will not have non retryable
/// errors here all the failures are infinitely retried until the message is successfully
/// written. The cancellation token is used to short-circuit the retries during shutdown.
/// We will always drain the stream from which we are reading messages.
#[derive(Clone)]
pub(crate) struct JetstreamWriter {
    config: Arc<Vec<ToVertexConfig>>,
    js_ctx: Context,
    /// HashMap of streams (a vertex can write to any immediate downstream) and a bool to represent
    /// whether the corresponding stream is full.
    is_full: HashMap<&'static str, Arc<AtomicBool>>,
    tracker_handle: TrackerHandle,
    sem: Arc<Semaphore>,
    watermark_handle: Option<WatermarkHandle>,
}

impl JetstreamWriter {
    /// Creates a JetStream Writer and a background task to make sure the Write futures (PAFs) are
    /// successful. Batch Size determines the maximum pending futures.
    pub(crate) fn new(
        config: Vec<ToVertexConfig>,
        js_ctx: Context,
        paf_concurrency: usize,
        tracker_handle: TrackerHandle,
        cancel_token: CancellationToken,
        watermark_handle: Option<WatermarkHandle>,
    ) -> Self {
        let to_vertex_streams = config
            .iter()
            .flat_map(|c| c.writer_config.streams.clone())
            .collect::<Vec<Stream>>();

        let is_full = to_vertex_streams
            .iter()
            .map(|stream| (stream.name, Arc::new(AtomicBool::new(false))))
            .collect::<HashMap<_, _>>();

        let this = Self {
            config: Arc::new(config),
            js_ctx,
            is_full,
            tracker_handle,
            sem: Arc::new(Semaphore::new(paf_concurrency)),
            watermark_handle,
        };

        // spawn a task for checking whether buffer is_full
        tokio::task::spawn({
            let mut this = this.clone();
            async move {
                this.check_stream_status(cancel_token).await;
            }
        });

        this
    }

    /// Checks the buffer usage metrics (soft and solid usage) for each stream in the streams vector.
    /// If the usage is greater than the bufferUsageLimit, it sets the is_full flag to true.
    async fn check_stream_status(&mut self, cln_token: CancellationToken) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(DEFAULT_REFRESH_INTERVAL_SECS));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for config in &*self.config {
                        for stream in &config.writer_config.streams {
                            let stream = stream.name;
                            match Self::fetch_buffer_usage(self.js_ctx.clone(), stream, config.writer_config.max_length).await {
                                Ok((soft_usage, solid_usage)) => {
                                    if solid_usage >= config.writer_config.usage_limit && soft_usage >= config.writer_config.usage_limit {
                                        if let Some(is_full) = self.is_full.get(stream) {
                                            is_full.store(true, Ordering::Relaxed);
                                        }
                                    } else if let Some(is_full) = self.is_full.get(stream) {
                                        is_full.store(false, Ordering::Relaxed);
                                    }
                                }
                                Err(e) => {
                                    error!(?e, "Failed to fetch buffer usage for stream {}, updating isFull to true", stream);
                                    if let Some(is_full) = self.is_full.get(stream) {
                                        is_full.store(true, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                }
                _ = cln_token.cancelled() => {
                    return;
                }
            }
        }
    }

    /// Fetches the buffer usage metrics (soft and solid usage) for the given stream.
    ///
    /// Soft Usage:
    /// Formula: (NumPending + NumAckPending) / maxLength
    /// - NumPending: The number of pending messages.
    /// - NumAckPending: The number of messages that are in processing state(yet to be acked).
    /// - maxLength: The maximum length of the buffer.
    ///
    /// Solid Usage:
    /// Formula:
    /// - If the stream's retention policy is LimitsPolicy: solidUsage = softUsage
    /// - Otherwise: solidUsage = State.Msgs / maxLength
    /// - State.Msgs: The total number of messages in the stream.
    /// - maxLength: The maximum length of the buffer.
    async fn fetch_buffer_usage(
        js_ctx: Context,
        stream_name: &str,
        max_length: usize,
    ) -> Result<(f64, f64)> {
        let mut stream = js_ctx
            .get_stream(stream_name)
            .await
            .map_err(|_| Error::ISB("Failed to get stream".to_string()))?;

        let stream_info = stream
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the stream info {:?}", e)))?;

        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(stream_name, stream_name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer {:?}", e)))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer info {:?}", e)))?;

        let soft_usage = (consumer_info.num_pending as f64 + consumer_info.num_ack_pending as f64)
            / max_length as f64;
        let solid_usage = if stream_info.config.retention == Limits {
            soft_usage
        } else {
            stream_info.state.messages as f64 / max_length as f64
        };

        Ok((soft_usage, solid_usage))
    }

    /// Starts reading messages from the stream and writes them to Jetstream ISB.
    pub(crate) async fn streaming_write(
        self,
        messages_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            info!("Starting streaming Jetstream writer");
            let mut messages_stream = messages_stream;
            let mut hash = DefaultHasher::new();

            let mut processed_msgs_count: usize = 0;
            let mut last_logged_at = Instant::now();

            while let Some(message) = messages_stream.next().await {
                // if message needs to be dropped, ack and continue
                // TODO: add metric for dropped count
                if message.dropped() {
                    // delete the entry from tracker
                    self.tracker_handle
                        .delete(message.offset)
                        .await
                        .expect("Failed to delete offset from tracker");
                    continue;
                }

                // List of PAFs(one message can be written to multiple streams)
                let mut pafs = vec![];
                for vertex in &*self.config {
                    // check whether we need to write to this downstream vertex
                    if !forward::should_forward(message.tags.clone(), vertex.conditions.clone()) {
                        continue;
                    }

                    // check to which partition the message should be written
                    let partition = forward::determine_partition(
                        String::from_utf8_lossy(&message.id.offset).to_string(),
                        vertex.partitions,
                        &mut hash,
                    );

                    // write the message to the corresponding stream
                    let stream = vertex
                        .writer_config
                        .streams
                        .get(partition as usize)
                        .expect("stream should be present")
                        .clone();

                    if let Some(paf) = self
                        .write(
                            stream.clone(),
                            message.clone(),
                            vertex.writer_config.buffer_full_strategy.clone(),
                            cln_token.clone(),
                        )
                        .await
                    {
                        pafs.push((stream, paf));
                    }
                }

                pipeline_metrics()
                    .forwarder
                    .write_total
                    .get_or_create(pipeline_isb_metric_labels())
                    .inc();

                if pafs.is_empty() {
                    continue;
                }

                self.resolve_pafs(pafs, message, cln_token.clone())
                    .await
                    .inspect_err(|e| {
                        error!(?e, "Failed to resolve PAFs");
                        cln_token.cancel();
                    })?;

                processed_msgs_count += 1;
                if last_logged_at.elapsed().as_secs() >= 1 {
                    info!(
                        "Processed {} messages in {:?}",
                        processed_msgs_count,
                        std::time::Instant::now()
                    );
                    processed_msgs_count = 0;
                    last_logged_at = Instant::now();
                }
            }
            info!("Streaming jetstream writer finished");
            Ok(())
        });
        Ok(handle)
    }

    /// Writes the message to the JetStream ISB and returns a future which can be
    /// awaited to get the PublishAck. It will do infinite retries until the message
    /// gets published successfully. If it returns an error it means it is fatal error
    pub(super) async fn write(
        &self,
        stream: Stream,
        message: Message,
        on_full: BufferFullStrategy,
        cln_token: CancellationToken,
    ) -> Option<PublishAckFuture> {
        let mut log_counter = 500u16;
        let offset = message.offset.clone();
        let payload: BytesMut = message
            .try_into()
            .expect("message serialization should not fail");

        // loop till we get a PAF, there could be other reasons why PAFs cannot be created.
        let paf = loop {
            // let's write only if the buffer is not full for the stream
            match self
                .is_full
                .get(stream.name)
                .map(|is_full| is_full.load(Ordering::Relaxed))
            {
                Some(true) => {
                    // FIXME: add metrics
                    if log_counter >= 500 {
                        warn!(stream=?stream, "stream is full (throttled logging)");
                        log_counter = 0;
                    }
                    log_counter += 1;
                    match on_full {
                        BufferFullStrategy::DiscardLatest => {
                            // delete the entry from tracker
                            self.tracker_handle
                                .delete(offset.clone())
                                .await
                                .expect("Failed to delete offset from tracker");
                            return None;
                        }
                        BufferFullStrategy::RetryUntilSuccess => {}
                    }
                }
                Some(false) => match self
                    .js_ctx
                    .publish(stream.name, payload.clone().freeze())
                    .await
                {
                    Ok(paf) => break paf,
                    Err(e) => error!(?e, "publishing failed, retrying"),
                },
                None => error!("Stream {} not found in is_full map", stream),
            }

            // short-circuit out in failure mode if shutdown has been initiated
            if cln_token.is_cancelled() {
                error!("Shutdown signal received, exiting write loop");
                return None;
            }

            // sleep to avoid busy looping
            sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
        };
        Some(paf)
    }

    /// resolve_pafs resolves the PAFs for the given result. It will try to resolve the PAFs
    /// asynchronously, if it fails it will do a blocking write to resolve the PAFs.
    /// At any point in time, we will only have X PAF resolvers running, this will help us create a
    /// natural backpressure.
    pub(super) async fn resolve_pafs(
        &self,
        pafs: Vec<(Stream, PublishAckFuture)>,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let start_time = Instant::now();
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .map_err(|_e| Error::ISB("Failed to acquire semaphore permit".to_string()))?;

        let this = self.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let mut offsets = Vec::new();

            // resolve the pafs
            for (stream, paf) in pafs {
                let ack = match paf.await {
                    Ok(ack) => Ok(ack),
                    Err(e) => {
                        error!(
                            ?e, stream = ?stream,
                            "Failed to resolve the future trying blocking write",
                        );
                        this.blocking_write(stream.clone(), message.clone(), cln_token.clone())
                            .await
                    }
                };

                match ack {
                    Ok(ack) => {
                        if ack.duplicate {
                            warn!(
                                stream = ?stream,
                                ack = ?ack,
                                "Duplicate message detected"
                            );
                        }
                        offsets.push((
                            stream.clone(),
                            Offset::Int(IntOffset::new(ack.sequence as i64, stream.partition)),
                        ));
                    }
                    Err(e) => {
                        error!(?e, stream = ?stream, "Blocking write failed");
                        // Since we failed to write to the stream, we need to send a NAK to the reader
                        this.tracker_handle
                            .discard(message.offset.clone())
                            .await
                            .expect("Failed to discard offset from the tracker");
                        return;
                    }
                }
            }

            // now the pafs have resolved, lets use the offsets to send watermark
            for (stream, offset) in offsets {
                if let Some(watermark_handle) = this.watermark_handle.as_ref() {
                    JetstreamWriter::publish_watermark(watermark_handle, stream, offset, &message)
                        .await;
                }
            }

            this.tracker_handle
                .delete(message.offset.clone())
                .await
                .expect("Failed to delete offset from tracker");

            pipeline_metrics()
                .isb
                .paf_resolution_time
                .get_or_create(pipeline_isb_metric_labels())
                .observe(start_time.elapsed().as_micros() as f64);
        });

        Ok(())
    }
    /// Writes the message to the JetStream ISB and returns the PublishAck. It will do
    /// infinite retries until the message gets published successfully. If it returns
    /// an error it means it is fatal non-retryable error.
    pub(crate) async fn blocking_write(
        &self,
        stream: Stream,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<PublishAck> {
        let start_time = Instant::now();
        let payload: BytesMut = message
            .try_into()
            .expect("message serialization should not fail");

        loop {
            match self
                .js_ctx
                .publish(stream.name, payload.clone().freeze())
                .await
            {
                Ok(paf) => match paf.await {
                    Ok(ack) => {
                        if ack.duplicate {
                            warn!(?ack, "Duplicate message detected, ignoring");
                        }
                        debug!(
                            elapsed_ms = start_time.elapsed().as_millis(),
                            "Blocking write successful in",
                        );
                        return Ok(ack);
                    }
                    Err(e) => {
                        error!(?e, "awaiting publish ack failed, retrying");
                        sleep(Duration::from_millis(10)).await;
                    }
                },
                Err(e) => {
                    error!(?e, "publishing failed, retrying");
                    sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
                }
            }

            if cln_token.is_cancelled() {
                return Err(Error::ISB("Shutdown signal received".to_string()));
            }
        }
    }

    /// publishes the watermark for the given stream and offset
    async fn publish_watermark(
        watermark_handle: &WatermarkHandle,
        stream: Stream,
        offset: Offset,
        message: &Message,
    ) {
        match watermark_handle {
            WatermarkHandle::ISB(handle) => {
                handle.publish_watermark(stream, offset).await;
            }
            WatermarkHandle::Source(handle) => {
                let input_partition = match &message.offset {
                    Offset::Int(offset) => offset.partition_idx,
                    Offset::String(offset) => offset.partition_idx,
                };
                handle
                    .publish_source_isb_watermark(stream, offset, input_partition)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Instant;

    use async_nats::jetstream;
    use async_nats::jetstream::consumer::{Config, Consumer};
    use async_nats::jetstream::{consumer, stream};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_models::models::ForwardConditions;
    use numaflow_models::models::TagConditions;

    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::message::{Message, MessageID, ReadAck};

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_async_write() {
        let tracker_handle = TrackerHandle::new(None, None);
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-async", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            context.clone(),
            100,
            tracker_handle,
            cln_token.clone(),
            None,
        );

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_0".to_string()]),
            tags: None,
            value: "message 0".as_bytes().to_vec().into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_0".to_string().into(),
                index: 0,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        let paf = writer
            .write(
                stream.clone(),
                message,
                BufferFullStrategy::RetryUntilSuccess,
                cln_token.clone(),
            )
            .await;
        assert!(paf.unwrap().await.is_ok());

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_sync_write() {
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-sync", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_0".to_string()]),
            tags: None,
            value: "message 0".as_bytes().to_vec().into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_0".to_string().into(),
                index: 0,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        let writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            context.clone(),
            100,
            TrackerHandle::new(None, None),
            cln_token.clone(),
            None,
        );

        let result = writer
            .blocking_write(stream.clone(), message, cln_token.clone())
            .await;
        assert!(result.is_ok());

        let publish_ack = result.unwrap();
        assert_eq!(publish_ack.stream, stream.name);

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_write_with_cancellation() {
        let tracker_handle = TrackerHandle::new(None, None);
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-cancellation", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
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
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name.to_string(),
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();

        let writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            context.clone(),
            100,
            tracker_handle,
            cancel_token.clone(),
            None,
        );

        let mut result_receivers = Vec::new();
        // Publish 10 messages successfully
        for i in 0..10 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                headers: HashMap::new(),
                watermark: None,
                metadata: None,
            };
            let paf = writer
                .write(
                    stream.clone(),
                    message,
                    BufferFullStrategy::RetryUntilSuccess,
                    cancel_token.clone(),
                )
                .await;
            result_receivers.push(paf);
        }

        // Attempt to publish a message which has a payload size greater than the max_message_size
        // so that it fails and sync write will be attempted and it will be blocked
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_11".to_string()]),
            tags: None,
            value: vec![0; 1025].into(),
            offset: Offset::Int(IntOffset::new(11, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_11".to_string().into(),
                index: 11,
            },
            headers: HashMap::new(),
            metadata: None,
        };
        let paf = writer
            .write(
                stream.clone(),
                message,
                BufferFullStrategy::RetryUntilSuccess,
                cancel_token.clone(),
            )
            .await;
        result_receivers.push(paf);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        // Check the results
        for (i, receiver) in result_receivers.into_iter().enumerate() {
            if i < 10 {
                assert!(
                    receiver.unwrap().await.is_ok(),
                    "Message {} should be published successfully",
                    i
                );
            } else {
                assert!(
                    receiver.unwrap().await.is_err(),
                    "Message 11 should fail with cancellation error"
                );
            }
        }

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_buffer_usage() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_fetch_buffer_usage", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_messages: 1000,
                max_message_size: 1024,
                max_messages_per_subject: 1000,
                retention: Limits, // Set retention policy to Limits for solid usage
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name.to_string(),
            )
            .await;

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name.to_string(),
            )
            .await
            .unwrap();

        let max_length = 100;

        // Publish messages to fill the buffer
        for _ in 0..80 {
            context
                .publish(stream.name, Bytes::from("test message"))
                .await
                .unwrap();
        }

        // Fetch buffer usage
        let (soft_usage, _) =
            JetstreamWriter::fetch_buffer_usage(context.clone(), stream.name, max_length)
                .await
                .unwrap();

        // Verify the buffer usage metrics
        assert_eq!(soft_usage, 0.8);
        assert_eq!(soft_usage, 0.8);

        // Clean up
        context
            .delete_consumer_from_stream(stream.name, stream.name)
            .await
            .unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_check_stream_status() {
        let tracker_handle = TrackerHandle::new(None, None);
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_check_stream_status", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_messages: 1000,
                max_message_size: 1024,
                max_messages_per_subject: 1000,
                retention: Limits, // Set retention policy to Limits for solid usage
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name.to_string(),
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    max_length: 100,
                    ..Default::default()
                },
                conditions: None,
            }],
            context.clone(),
            100,
            tracker_handle,
            cancel_token.clone(),
            None,
        );

        let mut js_writer = writer.clone();
        // Simulate the stream status check
        tokio::spawn(async move {
            js_writer.check_stream_status(cancel_token.clone()).await;
        });

        // Publish messages to fill the buffer, since max_length is 100, we need to publish 80 messages
        for _ in 0..80 {
            context
                .publish(stream.name, Bytes::from("test message"))
                .await
                .unwrap();
        }

        let start_time = Instant::now();
        while !writer
            .is_full
            .get(stream.name)
            .map(|is_full| is_full.load(Ordering::Relaxed))
            .unwrap()
            && start_time.elapsed().as_millis() < 1000
        {
            sleep(Duration::from_millis(5)).await;
        }

        // Verify the is_full flag
        assert!(
            writer
                .is_full
                .get(stream.name)
                .map(|is_full| is_full.load(Ordering::Relaxed))
                .unwrap(),
            "Buffer should be full after publishing messages"
        );

        // Clean up
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write() {
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None, None);

        let stream = Stream::new("test_publish_messages", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.into()],
                max_messages: 1000,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    max_length: 1000,
                    ..Default::default()
                },
                conditions: None,
            }],
            context.clone(),
            100,
            tracker_handle.clone(),
            cln_token.clone(),
            None,
        );

        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(500);
        let mut ack_rxs = vec![];
        // Publish 500 messages
        for i in 0..500 {
            let message = Message {
                typ: Default::default(),
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            ack_rxs.push(ack_rx);
            messages_tx.send(message).await.unwrap();
        }
        drop(messages_tx);

        let receiver_stream = ReceiverStream::new(messages_rx);
        let _handle = writer
            .streaming_write(receiver_stream, cln_token.clone())
            .await
            .unwrap();

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }
        // make sure all messages are acked
        assert!(tracker_handle.is_empty().await.unwrap());
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write_with_cancellation() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None, None);

        let stream = Stream::new("test_publish_cancellation", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.into()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            context.clone(),
            100,
            tracker_handle.clone(),
            cancel_token.clone(),
            None,
        );

        let (tx, rx) = tokio::sync::mpsc::channel(500);
        let mut ack_rxs = vec![];
        // Publish 100 messages successfully
        for i in 0..100 {
            let message = Message {
                typ: Default::default(),
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            ack_rxs.push(ack_rx);
            tx.send(message).await.unwrap();
        }

        let receiver_stream = ReceiverStream::new(rx);
        let _handle = writer
            .streaming_write(receiver_stream, cancel_token.clone())
            .await
            .unwrap();

        // Attempt to publish the 101st message, which should get stuck in the retry loop
        // because the max message size is set to 1024
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_101".to_string()]),
            tags: None,
            value: vec![0; 1025].into(),
            offset: Offset::Int(IntOffset::new(101, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_101".to_string().into(),
                index: 101,
            },
            headers: HashMap::new(),
            metadata: None,
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tracker_handle.insert(&message, ack_tx).await.unwrap();
        ack_rxs.push(ack_rx);
        tx.send(message).await.unwrap();
        drop(tx);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();
        // Check the results
        for (i, receiver) in ack_rxs.into_iter().enumerate() {
            let result = receiver.await.unwrap();
            if i < 100 {
                assert_eq!(result, ReadAck::Ack);
            } else {
                assert_eq!(result, ReadAck::Nak);
            }
        }

        // make sure all messages are acked
        assert!(tracker_handle.is_empty().await.unwrap());
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write_multiple_streams_vertices() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None, None);
        let cln_token = CancellationToken::new();

        let vertex1_streams = vec![
            Stream::new("vertex1-0", "temp", 0),
            Stream::new("vertex1-1", "temp", 0),
        ];
        let vertex2_streams = vec![
            Stream::new("vertex2-0", "temp", 0),
            Stream::new("vertex2-1", "temp", 0),
        ];
        let vertex3_streams = vec![
            Stream::new("vertex3-0", "temp", 0),
            Stream::new("vertex3-1", "temp", 0),
        ];

        let (_, consumers1) = create_streams_and_consumers(&context, &vertex1_streams).await;
        let (_, consumers2) = create_streams_and_consumers(&context, &vertex2_streams).await;
        let (_, consumers3) = create_streams_and_consumers(&context, &vertex3_streams).await;

        let writer = JetstreamWriter::new(
            vec![
                ToVertexConfig {
                    name: "vertex1",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex1_streams.clone(),
                        ..Default::default()
                    },
                    conditions: Some(Box::new(ForwardConditions::new(TagConditions {
                        operator: Some("and".to_string()),
                        values: vec!["tag1".to_string(), "tag2".to_string()],
                    }))),
                },
                ToVertexConfig {
                    name: "vertex2",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex2_streams.clone(),
                        ..Default::default()
                    },
                    conditions: Some(Box::new(ForwardConditions::new(TagConditions {
                        operator: Some("or".to_string()),
                        values: vec!["tag2".to_string()],
                    }))),
                },
                ToVertexConfig {
                    name: "vertex3",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex3_streams.clone(),
                        ..Default::default()
                    },
                    conditions: Some(Box::new(ForwardConditions::new(TagConditions {
                        operator: Some("not".to_string()),
                        values: vec!["tag1".to_string()],
                    }))),
                },
            ],
            context.clone(),
            100,
            tracker_handle.clone(),
            cln_token.clone(),
            None,
        );

        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(500);
        let mut ack_rxs = vec![];
        for i in 0..10 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: Some(Arc::from(vec!["tag1".to_string(), "tag2".to_string()])),
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            ack_rxs.push(ack_rx);
            messages_tx.send(message).await.unwrap();
        }
        drop(messages_tx);

        let receiver_stream = ReceiverStream::new(messages_rx);
        let _handle = writer
            .streaming_write(receiver_stream, cln_token.clone())
            .await
            .unwrap();

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        // since its and operation and both the tags match all 10 messages should be written
        // messages will be distributed based on the message id but the total message count
        // should be 10
        let mut write_count = 0;
        for mut consumer in consumers1 {
            write_count += consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(write_count, 10);

        // since its or operation and one of the tags match all 10 messages should be written
        write_count = 0;
        for mut consumer in consumers2 {
            write_count += consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(write_count, 10);

        // since it's a not operation, and none of the tags match, no messages should be written
        write_count = 0;
        for mut consumer in consumers3 {
            write_count += consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(write_count, 0);

        // make sure all messages are acked
        assert!(tracker_handle.is_empty().await.unwrap());

        for stream_name in vertex1_streams
            .iter()
            .chain(&vertex2_streams)
            .chain(&vertex3_streams)
        {
            context.delete_stream(stream_name.name).await.unwrap();
        }
    }

    async fn create_streams_and_consumers(
        context: &Context,
        stream_names: &[Stream],
    ) -> (Vec<stream::Stream>, Vec<Consumer<Config>>) {
        let mut streams = Vec::new();
        let mut consumers = Vec::new();

        for stream_name in stream_names {
            let _ = context.delete_stream(stream_name.name).await;
            let stream = context
                .get_or_create_stream(stream::Config {
                    name: stream_name.name.to_string(),
                    subjects: vec![stream_name.name.to_string()],
                    ..Default::default()
                })
                .await
                .unwrap();
            streams.push(stream);

            let consumer = context
                .create_consumer_on_stream(
                    Config {
                        name: Some(stream_name.name.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream_name.name,
                )
                .await
                .unwrap();
            consumers.push(consumer);
        }

        (streams, consumers)
    }
}
