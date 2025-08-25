use crate::error::Result;
use crate::message::Message;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::reduce::wal::WalMessage;
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use crate::reduce::wal::segment::compactor::Compactor;
use crate::tracker::TrackerHandle;
use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// WAL for storing the data. If None, we will not persist the data.
#[allow(clippy::upper_case_acronyms)]
pub(crate) struct WAL {
    /// Segment WAL which is only for appending.
    pub(crate) append_only_wal: AppendOnlyWal,
    /// Compactor for compacting the Segment WAL with GC WAL.
    pub(crate) compactor: Compactor,
}

/// PBQBuilder is a builder for PBQ.
pub(crate) struct PBQBuilder<C: crate::typ::NumaflowTypeConfig> {
    isb_reader: JetStreamReader<C>,
    tracker_handle: TrackerHandle,
    wal: Option<WAL>,
}

impl<C: crate::typ::NumaflowTypeConfig> PBQBuilder<C> {
    /// Creates a new PBQBuilder.
    pub(crate) fn new(isb_reader: JetStreamReader<C>, tracker_handle: TrackerHandle) -> Self {
        Self {
            isb_reader,
            tracker_handle,
            wal: None,
        }
    }

    pub(crate) fn wal(mut self, wal: WAL) -> Self {
        self.wal = Some(wal);
        self
    }

    pub(crate) fn build(self) -> PBQ<C> {
        PBQ {
            isb_reader: self.isb_reader,
            wal: self.wal,
            tracker_handle: self.tracker_handle,
        }
    }
}

/// PBQ is a persistent buffer queue.
#[allow(clippy::upper_case_acronyms)]
pub(crate) struct PBQ<C: crate::typ::NumaflowTypeConfig> {
    isb_reader: JetStreamReader<C>,
    wal: Option<WAL>,
    tracker_handle: TrackerHandle,
}

impl<C: crate::typ::NumaflowTypeConfig> PBQ<C> {
    /// Streaming read from PBQ, returns a ReceiverStream and a JoinHandle for monitoring errors.
    pub(crate) async fn streaming_read(
        self,
        cancellation_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (tx, rx) = mpsc::channel(100);
        let Some(wal) = self.wal else {
            // No WAL, just read from ISB
            return self.isb_reader.streaming_read(cancellation_token).await;
        };

        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            // Create a channel for WAL replay
            let (wal_tx, mut wal_rx) = mpsc::channel(500);

            // Clone the tx for use in the replay handler
            let messages_tx = tx.clone();

            // starts the compaction process, for the first time it compacts and replays the
            // unprocessed data then it does the periodic compaction.
            let compaction_handle = wal
                .compactor
                .start_compaction_with_replay(
                    wal_tx,
                    Duration::from_secs(60),
                    cancellation_token.clone(),
                )
                .await?;

            let mut replayed_count = 0;

            // Process replayed messages
            while let Some(msg) = wal_rx.recv().await {
                let msg: WalMessage = msg.try_into().expect("Failed to parse WAL message");
                messages_tx
                    .send(msg.into())
                    .await
                    .expect("Receiver dropped");
                replayed_count += 1;
            }

            info!(
                time_taken_ms = start.elapsed().as_millis(),
                ?replayed_count,
                "Finished replaying from WAL, starting to read from ISB"
            );

            // After replaying the unprocessed data, start reading the new set of messages from ISB
            // and also persist them in WAL.
            Self::read_isb_and_write_wal(
                self.isb_reader,
                wal.append_only_wal,
                self.tracker_handle,
                tx,
                cancellation_token,
            )
            .await?;

            // Wait for compaction task to exit gracefully
            compaction_handle.await.expect("task failed")?;

            info!("PBQ streaming read completed");
            Ok(())
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    /// Reads from ISB and writes to WAL.
    async fn read_isb_and_write_wal(
        isb_reader: JetStreamReader<C>,
        append_only_wal: AppendOnlyWal,
        tracker_handle: TrackerHandle,
        tx: Sender<Message>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let (wal_tx, wal_rx) = mpsc::channel(100);
        let (mut isb_stream, isb_handle) = isb_reader.streaming_read(cancellation_token).await?;
        let (mut offset_stream, wal_handle) = append_only_wal
            .streaming_write(ReceiverStream::new(wal_rx))
            .await?;

        // acknowledge the successfully written wal messages by listening on the offset stream.
        tokio::spawn(async move {
            while let Some(offset) = offset_stream.next().await {
                // no watermark publishing here, since we are just acknowledging the write to WAL
                tracker_handle
                    .delete(offset)
                    .await
                    .expect("Failed to delete offset");
            }
        });

        while let Some(msg) = isb_stream.next().await {
            wal_tx
                .send(SegmentWriteMessage::WriteData {
                    offset: Some(msg.offset.clone()),
                    data: WalMessage {
                        message: msg.clone(),
                    }
                    .clone()
                    .try_into()
                    .expect("Failed to parse message to bytes"),
                })
                .await
                .expect("Receiver dropped");

            tx.send(msg).await.expect("Receiver dropped");
        }

        isb_handle.await.expect("task failed")?;

        // drop the sender to signal the wal eof and wait for the wal task to exit gracefully
        drop(wal_tx);
        wal_handle.await.expect("task failed")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
    use crate::message::{IntOffset, MessageID, Offset};
    use crate::reduce::wal::segment::WalType;
    use crate::reduce::wal::segment::compactor::WindowKind;
    use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;
    use std::sync::Arc;
    use std::time::Duration;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_pbq_read_without_wal() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_pbq_read_without_wal", "test", 0);
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
            ..Default::default()
        };
        let tracker = TrackerHandle::new(None);
        use crate::pipeline::isb::jetstream::reader::ISBReaderComponents;
        let reader_components = ISBReaderComponents {
            vertex_type: "test".to_string(),
            stream: stream.clone(),
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker_handle: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };
        let js_reader: JetStreamReader<crate::typ::WithoutRateLimiter> =
            JetStreamReader::new(reader_components, numaflow_throttling::NoOpRateLimiter)
                .await
                .unwrap();

        let reader_cancel_token = CancellationToken::new();

        let pbq = PBQBuilder::new(js_reader, tracker.clone()).build();

        let (mut pbq_stream, handle) = pbq
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
                value: format!("pbq message {}", i).as_bytes().to_vec().into(),
                offset,
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        let mut buffer = vec![];
        for _ in 0..10 {
            let Some(val) = pbq_stream.next().await else {
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
        context.delete_stream(stream.name).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_pbq_read_with_wal() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        // Create temp directory for WAL
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();

        let stream = Stream::new("test_pbq_read_with_wal", "test", 0);
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
            ..Default::default()
        };
        let tracker = TrackerHandle::new(None);
        use crate::pipeline::isb::jetstream::reader::ISBReaderComponents;
        let reader_components = ISBReaderComponents {
            vertex_type: "test".to_string(),
            stream: stream.clone(),
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker_handle: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };
        let js_reader: JetStreamReader<crate::typ::WithoutRateLimiter> =
            JetStreamReader::new(reader_components, numaflow_throttling::NoOpRateLimiter)
                .await
                .unwrap();

        // Create WAL components
        let append_only_wal = AppendOnlyWal::new(
            crate::reduce::wal::segment::WalType::Data,
            wal_path.clone(),
            10,  // 10MB max file size
            100, // 100ms flush interval
            100, // channel buffer
            300, // max_segment_age_secs
        )
        .await
        .unwrap();

        let compactor = Compactor::new(wal_path.clone(), WindowKind::Aligned, 10, 100, 100, 300)
            .await
            .unwrap();

        let wal = WAL {
            append_only_wal,
            compactor,
        };

        let reader_cancel_token = CancellationToken::new();

        // Build PBQ with WAL
        let pbq = PBQBuilder::new(js_reader, tracker.clone()).wal(wal).build();

        let (mut pbq_stream, handle) = pbq
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        // Publish messages to the stream
        let mut offsets = vec![];
        for i in 0..10 {
            let offset = Offset::Int(IntOffset::new(i + 1, 0));
            offsets.push(offset.clone());
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("pbq message {}", i).as_bytes().to_vec().into(),
                offset,
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        // Read messages from PBQ
        let mut buffer = vec![];
        for _ in 0..10 {
            let Some(val) = pbq_stream.next().await else {
                break;
            };
            buffer.push(val);
        }

        assert_eq!(buffer.len(), 10, "Expected 10 messages from the PBQ");

        reader_cancel_token.cancel();
        handle.await.unwrap().unwrap();

        let append_only_wal =
            AppendOnlyWal::new(WalType::Data, wal_path.clone(), 10, 100, 100, 300)
                .await
                .unwrap();

        let (tx, rx) = mpsc::channel::<SegmentWriteMessage>(10);
        let (_result_rx, writer_handle) = append_only_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        tx.send(SegmentWriteMessage::Rotate { on_size: false })
            .await
            .unwrap();
        drop(tx);
        writer_handle.await.unwrap().unwrap();

        // Now use ReplayWal to verify the messages are persisted
        let replay_wal = ReplayWal::new(WalType::Data, wal_path.clone());
        let (mut replay_stream, replay_handle) = replay_wal.streaming_read().unwrap();

        let mut persisted_messages = vec![];

        while let Some(entry) = replay_stream.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let wal_message: WalMessage = data.try_into().unwrap();
                let message: Message = wal_message.into();
                persisted_messages.push(message);
            }
        }

        replay_handle.await.unwrap().unwrap();

        assert_eq!(
            persisted_messages.len(),
            10,
            "Expected 10 messages to be persisted in WAL"
        );

        // Verify the persisted messages match what we sent
        for (i, msg) in persisted_messages.iter().enumerate() {
            assert_eq!(
                msg.keys.as_ref(),
                &[format!("key_{}", i)],
                "Persisted message keys don't match"
            );
            assert_eq!(
                msg.value.as_ref(),
                format!("pbq message {}", i).as_bytes(),
                "Persisted message value doesn't match"
            );
        }

        for offset in offsets {
            tracker.discard(offset).await.unwrap();
        }
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_pbq_replay_from_wal_then_read_from_isb() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        // Create temp directory for WAL
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();

        let stream = Stream::new("test_pbq_replay_wal", "test", 0);
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

        // First, write some messages directly to WAL
        let append_only_wal =
            AppendOnlyWal::new(WalType::Data, wal_path.clone(), 10, 100, 100, 300)
                .await
                .unwrap();

        let (tx, rx) = mpsc::channel::<SegmentWriteMessage>(100);
        let (_result_rx, writer_handle) = append_only_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        // Write 5 messages to WAL directly
        for i in 0..5 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("wal_key_{}", i)]),
                tags: None,
                value: format!("wal message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i + 1, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("wal_offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };

            let wal_message = WalMessage {
                message: message.clone(),
            };
            let bytes: bytes::Bytes = wal_message.try_into().unwrap();

            tx.send(SegmentWriteMessage::WriteData {
                offset: Some(message.offset.clone()),
                data: bytes,
            })
            .await
            .unwrap();
        }

        drop(tx);
        writer_handle.await.unwrap().unwrap();

        // Now create PBQ components
        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
            ..Default::default()
        };
        let tracker = TrackerHandle::new(None);
        use crate::pipeline::isb::jetstream::reader::ISBReaderComponents;
        let reader_components = ISBReaderComponents {
            vertex_type: "test".to_string(),
            stream: stream.clone(),
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker_handle: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };
        let js_reader: JetStreamReader<crate::typ::WithoutRateLimiter> =
            JetStreamReader::new(reader_components, numaflow_throttling::NoOpRateLimiter)
                .await
                .unwrap();

        // Create new WAL components for PBQ
        let append_only_wal =
            AppendOnlyWal::new(WalType::Data, wal_path.clone(), 10, 100, 100, 300)
                .await
                .unwrap();

        let compactor = Compactor::new(wal_path.clone(), WindowKind::Aligned, 10, 100, 100, 300)
            .await
            .unwrap();

        let wal = WAL {
            append_only_wal,
            compactor,
        };

        let reader_cancel_token = CancellationToken::new();

        // Build PBQ with WAL
        let pbq = PBQBuilder::new(js_reader, tracker.clone()).wal(wal).build();

        // Start reading from PBQ - this should first replay from WAL, then read from ISB
        let (mut pbq_stream, handle) = pbq
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        // Read the first 5 messages which should come from WAL
        let mut replayed_messages = vec![];
        for _ in 0..5 {
            let Some(val) = pbq_stream.next().await else {
                break;
            };
            replayed_messages.push(val);
        }

        assert_eq!(
            replayed_messages.len(),
            5,
            "Expected 5 messages to be replayed from WAL"
        );

        // Verify the replayed messages match what we wrote to WAL
        for (i, msg) in replayed_messages.iter().enumerate() {
            assert_eq!(
                msg.keys.as_ref(),
                &[format!("wal_key_{}", i)],
                "Replayed message keys don't match"
            );
            assert_eq!(
                msg.value.as_ref(),
                format!("wal message {}", i).as_bytes(),
                "Replayed message value doesn't match"
            );
        }

        // Now publish 5 more messages to ISB
        let mut isb_offsets = vec![];
        for i in 0..5 {
            let offset = Offset::Int(IntOffset::new(i + 100, 0)); // Different offset range
            isb_offsets.push(offset.clone());
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("isb_key_{}", i)]),
                tags: None,
                value: format!("isb message {}", i).as_bytes().to_vec().into(),
                offset,
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("isb_offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        // Read the next 5 messages which should come from ISB
        let mut isb_messages = vec![];
        for _ in 0..5 {
            let Some(val) = pbq_stream.next().await else {
                break;
            };
            isb_messages.push(val);
        }

        assert_eq!(
            isb_messages.len(),
            5,
            "Expected 5 messages to be read from ISB"
        );

        // Verify the ISB messages
        for (i, msg) in isb_messages.iter().enumerate() {
            assert_eq!(
                msg.keys.as_ref(),
                &[format!("isb_key_{}", i)],
                "ISB message keys don't match"
            );
            assert_eq!(
                msg.value.as_ref(),
                format!("isb message {}", i).as_bytes(),
                "ISB message value doesn't match"
            );
        }

        // Verify that the ISB messages were also written to WAL
        reader_cancel_token.cancel();
        handle.await.unwrap().unwrap();

        // Check WAL again - should now have all 10 messages
        let data_replay_wal = ReplayWal::new(WalType::Data, wal_path.clone());
        let (mut replay_stream, replay_handle) = data_replay_wal.streaming_read().unwrap();

        let mut all_wal_messages: Vec<Message> = vec![];
        while let Some(entry) = replay_stream.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let wal_message: WalMessage = data.try_into().unwrap();
                all_wal_messages.push(wal_message.into());
            }
        }
        replay_handle.await.unwrap().unwrap();

        let compact_wal = ReplayWal::new(WalType::Compact, wal_path.clone());
        let (mut replay_stream, replay_handle) = compact_wal.streaming_read().unwrap();
        while let Some(entry) = replay_stream.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let wal_message: WalMessage = data.try_into().unwrap();
                all_wal_messages.push(wal_message.into());
            }
        }
        replay_handle.await.unwrap().unwrap();

        assert_eq!(
            all_wal_messages.len(),
            10,
            "Expected 10 total messages in WAL (5 original + 5 from ISB)"
        );

        // Clean up
        for offset in isb_offsets {
            tracker.discard(offset).await.unwrap();
        }
        context.delete_stream(stream.name).await.unwrap();
    }
}
