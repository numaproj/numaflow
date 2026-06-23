use crate::mark_success;
use crate::message::MessageHandle;
use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::wal::SegmentWriter;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};

/// Duration after which the WAL Segment is considered stale.
const ROTATE_IF_STALE_DURATION: chrono::Duration = chrono::Duration::seconds(30);

/// The Command that has to be operated on the Segment.
pub(crate) enum SegmentWriteMessage {
    /// Writes a message to the WAL. The message will be converted to bytes internally.
    /// After successful write, mark_success() is called on the MessageHandle to ACK.
    WriteMessage { read_message: MessageHandle },
    /// Writes GC Events to the WAL
    WriteGcEvent {
        /// Raw data to be written to the WAL.
        data: Bytes,
    },
    /// Rotates the file on demand.
    Rotate {
        /// When "true" Rotate only if the size is >= the size
        on_size: bool,
    },
}

/// Append only Segment writer that manages and rotates the WAL Segment. It owns the rotation
/// policy; the segment I/O is delegated to the [SegmentWriter].
struct SegmentWriteActor {
    /// Kind of WAL.
    wal_type: WalType,
    /// The storage that performs the segment I/O.
    writer: Box<dyn SegmentWriter>,
    /// Time when the current segment was opened.
    create_time: DateTime<Utc>,
    /// Size of the current segment.
    current_size: u64,
    /// The interval at which the buffer has to be flushed.
    flush_interval: Duration,
    /// Maximum file size per segment.
    max_file_size: u64,
    /// Maximum age of a segment before rotation.
    max_segment_age: chrono::Duration,
}

impl Drop for SegmentWriteActor {
    fn drop(&mut self) {
        // TODO: do drop on shutdown on error path
    }
}

impl SegmentWriteActor {
    /// Creates a new SegmentWriteActor over the given storage backend.
    fn new(
        wal_type: WalType,
        writer: Box<dyn SegmentWriter>,
        max_file_size: u64,
        flush_interval: Duration,
        max_segment_age: chrono::Duration,
    ) -> Self {
        Self {
            wal_type,
            writer,
            create_time: Utc::now(),
            current_size: 0,
            flush_interval,
            max_file_size,
            max_segment_age,
        }
    }

    /// Starts processing the [SegmentWriteMessage] operation.
    async fn start_processing(
        mut self,
        in_rx: ReceiverStream<SegmentWriteMessage>,
    ) -> WalResult<()> {
        let mut flush_timer = interval(self.flush_interval);

        let in_rx = in_rx.timeout(Duration::from_secs(15));
        tokio::pin!(in_rx);

        loop {
            tokio::select! {
                maybe_msg = in_rx.next() => {
                    let Some(msg) = maybe_msg else {
                        break;
                    };
                    match msg {
                        Ok(msg) => {
                            self.handle_message(msg).await?;
                        }
                        Err(_) => {
                            debug!("calling flush and possible rotate on timeout");
                            self.flush_and_rotate().await?;
                        }
                    }
                }
                _ = flush_timer.tick() => {
                    self.flush().await?;
                }
            }
        }

        info!(?self.wal_type, "Stopping, doing a final flush and rotate!");
        self.rotate(false).await
    }

    /// Processes each [SegmentWriteMessage] operation. This should only return critical errors, and
    /// we should exit upon errors.
    async fn handle_message(&mut self, msg: SegmentWriteMessage) -> WalResult<()> {
        match msg {
            SegmentWriteMessage::WriteMessage { read_message } => {
                // Convert message to bytes
                let data: Bytes = crate::reduce::wal::WalMessage {
                    message: read_message.message().clone(),
                }
                .try_into()
                .expect("Failed to convert message to bytes");

                // Write to WAL and ACK on success
                match self.write_data(data).await {
                    Ok(_) => {
                        // Successfully written to WAL, ACK the message
                        mark_success!(read_message);
                        Ok(())
                    }
                    Err(e) => {
                        error!(?e, "Failed to write message to WAL");
                        read_message.mark_failed(&e);
                        Err(e)
                    }
                }
            }
            SegmentWriteMessage::WriteGcEvent { data } => {
                // Just write the raw data
                self.write_data(data).await?;
                Ok(())
            }
            SegmentWriteMessage::Rotate { on_size } => {
                // Rotate if forced (`on_size` is false) OR if size threshold is met
                if !on_size || self.current_size >= self.max_file_size {
                    self.rotate(true).await?;
                } else {
                    debug!(
                        current_size = self.current_size,
                        max_size = self.max_file_size,
                        "Skipping rotation: size threshold not met and not forced."
                    );
                }
                Ok(())
            }
        }
    }

    /// Writes the data to the Segment, rotating first if the size or age threshold is hit.
    /// ### CANCEL SAFETY:
    /// This is not Cancel Safe since the writes are buffered.
    async fn write_data(&mut self, data: Bytes) -> WalResult<()> {
        // Check if we need to rotate based on size
        if self.current_size > 0 && self.current_size + data.len() as u64 >= self.max_file_size {
            debug!(
                current_size = self.current_size,
                max_size = self.max_file_size,
                "Rotating segment file due to size threshold"
            );
            self.rotate(true).await?;
        }

        // Check if we need to rotate based on time
        if self.current_size > 0
            && Utc::now().signed_duration_since(self.create_time) > self.max_segment_age
        {
            debug!(
                max_age = ?self.max_segment_age,
                "Rotating segment file due to age threshold"
            );
            self.rotate(true).await?;
        }

        let data_len = data.len() as u64;
        self.writer.write(data).await?;
        self.current_size += data_len;

        Ok(())
    }

    /// Flush the buffer to the underlying Segment.
    async fn flush(&mut self) -> WalResult<()> {
        self.writer.flush().await
    }

    /// Flush and possibly rotate since there are no new writes to the file.
    async fn flush_and_rotate(&mut self) -> WalResult<()> {
        // if the file has not been rotated in these many seconds, let's rotate
        if Utc::now().signed_duration_since(self.create_time) > ROTATE_IF_STALE_DURATION {
            debug!(duration = ?ROTATE_IF_STALE_DURATION, "Rotating stale file, no entries for a while");
            self.rotate(true).await
        } else {
            self.flush().await
        }
    }

    /// Seals the current segment and opens a new one if `open_new`. Resets the size/age tracking.
    async fn rotate(&mut self, open_new: bool) -> WalResult<()> {
        if self.current_size == 0 {
            return Ok(());
        }

        info!(?self.wal_type, current_size = ?self.current_size, "Rotating WAL segment");
        self.writer.rotate(open_new).await?;

        if open_new {
            self.create_time = Utc::now();
            self.current_size = 0;
        }
        Ok(())
    }
}

/// Append Only WAL.
pub(crate) struct AppendOnlyWal {
    wal_type: WalType,
    writer: Box<dyn SegmentWriter>,
    max_file_size_mb: u64,
    flush_interval_ms: u64,
    max_segment_age_secs: u64,
}

impl AppendOnlyWal {
    /// Creates an [AppendOnlyWal] over the given writer.
    pub(in crate::reduce) fn new(
        wal_type: WalType,
        writer: Box<dyn SegmentWriter>,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        max_segment_age_secs: u64,
    ) -> Self {
        Self {
            wal_type,
            writer,
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        }
    }

    /// Start the WAL for streaming write.
    /// Messages are kept alive until write completes, ensuring Arc<AckHandle> is not dropped prematurely.
    /// CANCEL SAFETY: This is not cancel safe. This is because our writes are buffered.
    pub(crate) async fn streaming_write(
        self,
        stream: ReceiverStream<SegmentWriteMessage>,
    ) -> WalResult<JoinHandle<WalResult<()>>> {
        let max_file_size_bytes = self.max_file_size_mb * 1024 * 1024;
        let flush_duration = Duration::from_millis(self.flush_interval_ms);
        let max_segment_age = chrono::Duration::seconds(self.max_segment_age_secs as i64);

        let actor = SegmentWriteActor::new(
            self.wal_type,
            self.writer,
            max_file_size_bytes,
            flush_duration,
            max_segment_age,
        );

        let handle = tokio::spawn(async move {
            actor.start_processing(stream).await?;
            Ok(())
        });

        info!("FileWriterActor spawned and running.");
        Ok(handle)
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)] // Tests use indexing for simplicity
mod tests {
    use super::*;
    use crate::message::{IntOffset, Message, Offset};
    use crate::reduce::wal::segment::WalType;
    use crate::reduce::wal::segment::wal::{FsStore, WalStore};
    use std::fs;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_wal_write_receive_results_and_rotate() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let max_file_size_mb = 1;
        let flush_interval_ms = 50;
        let channel_buffer = 10;
        let max_segment_age_secs = 300; // 5 minutes

        let writer = FsStore::new(base_path.clone())
            .writer(WalType::Data)
            .await
            .unwrap();
        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            writer,
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        );

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let _writer_handle = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        let id1 = Offset::Int(IntOffset::new(1, 0));
        let msg1 = Message {
            offset: id1.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg1.into(),
            })
            .await
            .unwrap();

        let id2 = Offset::Int(IntOffset::new(2, 0));
        let msg2 = Message {
            offset: id2.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg2.into(),
            })
            .await
            .unwrap();

        let id3 = Offset::Int(IntOffset::new(3, 0));
        let large_data_size = (0.6 * 1024.0 * 1024.0) as usize;
        let msg3 = Message {
            offset: id3.clone(),
            value: vec![b'A'; large_data_size].into(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg3.into(),
            })
            .await
            .unwrap();

        let id4 = Offset::Int(IntOffset::new(4, 0));
        let msg4 = Message {
            offset: id4.clone(),
            value: vec![b'B'; large_data_size].into(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg4.into(),
            })
            .await
            .unwrap();

        let id5 = Offset::Int(IntOffset::new(5, 0));
        let msg5 = Message {
            offset: id5.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg5.into(),
            })
            .await
            .unwrap();

        let id6 = Offset::Int(IntOffset::new(6, 0));
        let msg6 = Message {
            offset: id6.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg6.into(),
            })
            .await
            .unwrap();

        drop(wal_tx);

        // Wait a bit for writes to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|ext| ext == "wal" || ext == "frozen")
            }) // Ensure we only check .wal files
            .collect();
        files.sort();

        assert_eq!(
            files.len(),
            2,
            "There should be at-most 2 WAL segment files (segment_0 and segment_1)"
        );

        // File content checks removed since messages are now serialized via WalMessage
        // which includes additional metadata beyond just the raw data

        // FIXME: why does this hang?
        // writer_handle
        //     .await
        //     .expect("writer shouldn't fail")
        //     .expect("WAL shouldn't raise error");
    }

    #[tokio::test]
    async fn test_wal_periodic_flush() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let max_file_size_mb = 10;
        let flush_interval_ms = 50;
        let channel_buffer = 10;
        let max_segment_age_secs = 300; // 5 minutes

        let writer = FsStore::new(base_path.clone())
            .writer(WalType::Data)
            .await
            .unwrap();
        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            writer,
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        );

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let _writer_handle = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        let id1 = Offset::Int(IntOffset::new(1, 0));
        let msg1 = Message {
            offset: id1.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg1.into(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(flush_interval_ms * 3)).await;

        let files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|ext| ext == "wal" || ext == "frozen")
            })
            .collect();

        assert_eq!(files.len(), 1, "There should be 1 WAL file");

        // FIXME: why does this hang?
        // writer_handle
        //     .await
        //     .expect("writer shouldn't fail")
        //     .expect("WAL shouldn't raise error");
    }

    #[tokio::test]
    async fn test_wal_rotate_command() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let max_file_size_mb = 10;
        let flush_interval_ms = 50;
        let channel_buffer = 10;
        let max_segment_age_secs = 300; // 5 minutes

        let writer = FsStore::new(base_path.clone())
            .writer(WalType::Data)
            .await
            .unwrap();
        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            writer,
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        );

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let _writer_handle = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        // Send some data to the WAL
        let id1 = Offset::Int(IntOffset::new(1, 0));
        let msg1 = Message {
            offset: id1.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg1.into(),
            })
            .await
            .unwrap();

        // Send the Rotate command
        wal_tx
            .send(SegmentWriteMessage::Rotate { on_size: false })
            .await
            .unwrap();

        // Send more data after rotation
        let id2 = Offset::Int(IntOffset::new(2, 0));
        let msg2 = Message {
            offset: id2.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg2.into(),
            })
            .await
            .unwrap();

        drop(wal_tx);

        // Allow some time for the rotation to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the files
        let mut files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "frozen"))
            .collect();
        files.sort();

        assert_eq!(files.len(), 2, "There should be 2 WAL segment files");

        // Verify the rotated file is renamed correctly
        assert!(
            files
                .first()
                .expect("Expected at least one frozen file")
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(".frozen")
        );

        // File content checks removed since messages are now serialized via WalMessage

        // FIXME: why does this hang?
        // writer_handle
        //     .await
        //     .expect("writer shouldn't fail")
        //     .expect("WAL shouldn't raise error");
    }

    #[tokio::test]
    async fn test_wal_time_based_rotation() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let max_file_size_mb = 10; // Large enough to not trigger size-based rotation
        let flush_interval_ms = 50;
        let channel_buffer = 10;
        let max_segment_age_secs = 2; // Very short for testing

        let writer = FsStore::new(base_path.clone())
            .writer(WalType::Data)
            .await
            .unwrap();
        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            writer,
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        );

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let _writer_handle = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        // Send some data to the WAL
        let id1 = Offset::Int(IntOffset::new(1, 0));
        let msg1 = Message {
            offset: id1.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg1.into(),
            })
            .await
            .unwrap();

        // Wait for the segment age to exceed the threshold
        tokio::time::sleep(Duration::from_secs(max_segment_age_secs + 1)).await;

        // Send more data after the time threshold
        let id2 = Offset::Int(IntOffset::new(2, 0));
        let msg2 = Message {
            offset: id2.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: msg2.into(),
            })
            .await
            .unwrap();

        // Allow some time for the rotation to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify the files
        let mut files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "frozen"))
            .collect();
        files.sort();

        assert!(
            !files.is_empty(),
            "There should be at least one frozen file after time-based rotation"
        );

        // File content check removed since messages are now serialized via WalMessage

        drop(wal_tx);
    }
}
