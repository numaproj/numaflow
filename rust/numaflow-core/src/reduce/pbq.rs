use crate::error::Result;
use crate::message::Message;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::reduce::wal::WalMessage;
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use crate::reduce::wal::segment::compactor::Compactor;
use crate::tracker::TrackerHandle;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

/// WAL for storing the data. If None, we will not persist the data.
pub(crate) struct WAL {
    append_only_wal: AppendOnlyWal,
    compactor: Compactor,
}

/// PBQBuilder is a builder for PBQ.
pub(crate) struct PBQBuilder {
    isb_reader: JetStreamReader,
    tracker_handle: TrackerHandle,
    wal: Option<WAL>,
}

impl PBQBuilder {
    /// Creates a new PBQBuilder.
    pub(crate) fn new(isb_reader: JetStreamReader, tracker_handle: TrackerHandle) -> Self {
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

    pub(crate) fn build(self) -> PBQ {
        PBQ {
            isb_reader: self.isb_reader,
            wal: self.wal,
            tracker_handle: self.tracker_handle,
        }
    }
}

/// PBQ is a persistent buffer queue.
pub(crate) struct PBQ {
    isb_reader: JetStreamReader,
    wal: Option<WAL>,
    tracker_handle: TrackerHandle,
}

impl PBQ {
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
            // Replay messages from WAL
            Self::replay_wal(wal.compactor, &tx).await?;

            // Read from ISB and write to WAL
            Self::read_isb_and_write_wal(
                self.isb_reader,
                wal.append_only_wal,
                self.tracker_handle,
                tx,
                cancellation_token,
            )
            .await?;

            Ok(())
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    /// Replays messages from the WAL converts them to [crate::message::Message] and sends them to
    /// the tx channel.
    async fn replay_wal(compactor: Compactor, tx: &Sender<Message>) -> Result<()> {
        let (wal_tx, mut wal_rx) = mpsc::channel(100);
        compactor.compact_with_replay(wal_tx).await?;

        while let Some(msg) = wal_rx.recv().await {
            let msg: WalMessage = msg.try_into().unwrap();
            tx.send(msg.into()).await.expect("Receiver dropped");
        }

        Ok(())
    }

    /// Reads from ISB and writes to WAL.
    async fn read_isb_and_write_wal(
        isb_reader: JetStreamReader,
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
                    data: msg
                        .clone()
                        .try_into()
                        .expect("Failed to parse message to bytes"),
                })
                .await
                .expect("Receiver dropped");

            tx.send(msg).await.expect("Receiver dropped");
        }

        wal_handle.await.expect("task failed")?;
        isb_handle.await.expect("task failed")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
    use crate::message::{IntOffset, MessageID, Offset};
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    // #[cfg(feature = "nats-tests")]
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
        };
        let tracker = TrackerHandle::new(None, None);
        let js_reader = JetStreamReader::new(
            "test".to_string(),
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
    }
}
