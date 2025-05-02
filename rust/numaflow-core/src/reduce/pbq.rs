use crate::error::Result;
use crate::message::Message;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
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
    /// Starts the PBQ and returns a ReceiverStream and a JoinHandle for monitoring errors.
    pub(crate) async fn start(
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
            tx.send(msg.try_into().expect("Failed to parse message from Bytes"))
                .await
                .expect("Receiver dropped");
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
