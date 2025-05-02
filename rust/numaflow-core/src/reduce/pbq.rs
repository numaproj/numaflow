//! PBQ is a persistent buffer queue. It is used to store the data that is not yet processed and
//! forwarded. It exposes a ReceiverStream that can be used to read the data from the PBQ.
//! Reading in PBQ is done as follows:
//! - Before it can read from ISB (during bootup)
//!   - it will read from WAL and replay the data.
//! - After it has replayed from the WAL (after bootup)
//!   - Read the data from the ISB
//!   - Write the data to WAL

use crate::error::Result;
use crate::message::Message;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use crate::reduce::wal::segment::compactor::Compactor;
use crate::tracker::TrackerHandle;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

/// WAL for storing the data. If None, we will not persist the data.
pub(crate) struct WAL {
    append_only_wal: AppendOnlyWal,
    compactor: Compactor,
}

/// PBQ is a persistent buffer queue.
pub(crate) struct PBQ {
    /// JetStream Reader.
    isb_reader: JetStreamReader,
    wal: Option<WAL>,
    tracker_handle: TrackerHandle,
}

impl PBQ {
    /// Creates a new PBQ.
    pub(crate) fn new(
        isb_reader: JetStreamReader,
        wal: Option<WAL>,
        tracker_handle: TrackerHandle,
    ) -> Self {
        Self {
            isb_reader,
            wal,
            tracker_handle,
        }
    }

    /// Starts the PBQ and returns a ReceiverStream that can be used to read the data from the PBQ.
    /// It replays data from WAL and then starts reading from ISB.
    pub(crate) async fn streaming_read(
        self,
        cancellation_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let Some(wal) = self.wal else {
            // No WAL, just read from ISB
            return self.isb_reader.streaming_read(cancellation_token).await;
        };

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let task_handle = tokio::spawn(async move {
            let (wal_tx, wal_rx) = tokio::sync::mpsc::channel(100);
            // Replay the messages from WAL
            Self::replay_messages_from_wal(wal.compactor, tx.clone()).await?;
            let (mut isb_stream, handle) =
                self.isb_reader.streaming_read(cancellation_token).await?;

            let (mut offset_stream, wal_handle) = wal
                .append_only_wal
                .streaming_write(ReceiverStream::new(wal_rx))
                .await?;

            // start a background task to listen on the offset stream and invoke delete on the tracker
            // so that the messages gets acked.
            tokio::spawn(async move {
                while let Some(offset) = offset_stream.next().await {
                    self.tracker_handle
                        .delete(offset)
                        .await
                        .expect("failed to delete offset");
                }
            });

            while let Some(msg) = isb_stream.next().await {
                wal_tx
                    .send(SegmentWriteMessage::WriteData {
                        offset: Some(msg.offset.clone()),
                        data: msg
                            .clone()
                            .try_into()
                            .expect("failed to parse message to bytes"),
                    })
                    .await
                    .expect("rx dropped");

                tx.send(msg.clone()).await.expect("rx dropped");
            }
            Ok(())
        });

        Ok((ReceiverStream::new(rx), task_handle))
    }

    async fn replay_messages_from_wal(
        compactor: Compactor,
        tx: tokio::sync::mpsc::Sender<Message>,
    ) -> Result<()> {
        let (wal_tx, mut wal_rx) = tokio::sync::mpsc::channel(100);
        let wal_handle = compactor.compact_with_replay(wal_tx).await?;

        while let Some(msg) = wal_rx.recv().await {
            tx.send(msg.try_into().expect("failed to parse message from Bytes"))
                .await
                .expect("tx dropped");
        }

        wal_handle.await.expect("wal task failed")?;
        Ok(())
    }
}
