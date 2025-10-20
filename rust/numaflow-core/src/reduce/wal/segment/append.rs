use crate::message::Message;
use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use tokio::io::BufWriter;
use tokio::task::JoinHandle;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    time::{Duration, interval},
};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};

/// Duration after which the WAL Segment is considered stale.
const ROTATE_IF_STALE_DURATION: chrono::Duration = chrono::Duration::seconds(30);

/// The Command that has to be operated on the Segment.
pub(crate) enum SegmentWriteMessage {
    /// Writes a message to the WAL. The message will be converted to bytes internally.
    /// The message is kept alive until the write completes, ensuring Arc<AckHandle> is not
    /// dropped prematurely.
    WriteMessage {
        /// Message to be written. Will be dropped after successful write.
        message: Message,
    },
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

/// Append only Segment writer that manages and rotates the WAL Segment.
struct SegmentWriteActor {
    /// Kind of WAL
    wal_type: WalType,
    /// Path where the WALs are persisted.
    base_path: PathBuf,
    /// Name of the current WAL Segment.
    current_file_name: String,
    /// Time when the segment was created.
    create_time: DateTime<Utc>,
    /// The current segment writer exposed via a buffer.
    current_file_buf: BufWriter<File>,
    /// Size of the current segment.
    current_size: u64,
    /// The file index since the last restart.
    file_index: usize,
    /// The interval at which the files have to be flushed.
    flush_interval: Duration,
    /// Maximum file size per segment.
    max_file_size: u64,
    /// Maximum age of a segment file before rotation
    max_segment_age: chrono::Duration,
}

impl Drop for SegmentWriteActor {
    fn drop(&mut self) {
        // TODO: do drop on shutdown on error path
    }
}

impl SegmentWriteActor {
    #[allow(clippy::too_many_arguments)]
    /// Creates a new SegmentWriteActor.
    fn new(
        wal_type: WalType,
        base_path: PathBuf,
        current_file_name: String,
        create_time: DateTime<Utc>,
        current_file: BufWriter<File>,
        max_file_size: u64,
        flush_interval: Duration,
        max_segment_age: chrono::Duration,
    ) -> Self {
        Self {
            wal_type,
            base_path,
            current_file_name,
            create_time,
            current_file_buf: current_file,
            current_size: 0,
            file_index: 0,
            max_file_size,
            flush_interval,
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
        self.rotate_file(false).await
    }

    /// Processes each [SegmentWriteMessage] operation. This should only return critical errors, and
    /// we should exit upon errors.
    async fn handle_message(&mut self, msg: SegmentWriteMessage) -> WalResult<()> {
        match msg {
            SegmentWriteMessage::WriteMessage { message } => {
                // Convert message to bytes
                let data: Bytes = crate::reduce::wal::WalMessage {
                    message: message.clone(),
                }
                .try_into()
                .expect("Failed to convert message to bytes");

                // Message is dropped here after successful write, triggering ack/nack.
                return match self.write_data(data).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // message failed to write to WAL, mark it as failed so that it gets nacked.
                        error!(?e, "Failed to write message to WAL");
                        message
                            .ack_handle
                            .as_ref()
                            .expect("ack handle should be present")
                            .is_failed
                            .store(true, Ordering::Relaxed);
                        Err(e)
                    }
                };
            }
            SegmentWriteMessage::WriteGcEvent { data } => {
                // Just write the raw data
                self.write_data(data).await?;
            }
            SegmentWriteMessage::Rotate { on_size } => {
                // Rotate if forced (`on_size` is false) OR if size threshold is met
                if !on_size || self.current_size >= self.max_file_size {
                    self.rotate_file(true).await?;
                } else {
                    debug!(
                        current_size = self.current_size,
                        max_size = self.max_file_size,
                        "Skipping rotation: size threshold not met and not forced."
                    );
                }
            }
        }
        Ok(())
    }

    /// Writes the data to the Segment.
    /// The writes are in this format `<u64(data_len)><[u8;data_len]>`.
    /// ### CANCEL SAFETY:
    /// This is not Cancel Safe. We can make it cancel safe by removing the buffering and instead of
    /// `write_all`, we should use `write`.
    async fn write_data(&mut self, data: Bytes) -> WalResult<()> {
        // Check if we need to rotate based on size
        if self.current_size > 0 && self.current_size + data.len() as u64 >= self.max_file_size {
            debug!(
                current_size = self.current_size,
                max_size = self.max_file_size,
                "Rotating segment file due to size threshold"
            );
            self.rotate_file(true).await?;
        }

        // Check if we need to rotate based on time
        if self.current_size > 0
            && Utc::now().signed_duration_since(self.create_time) > self.max_segment_age
        {
            debug!(
                max_age = ?self.max_segment_age,
                "Rotating segment file due to age threshold"
            );
            self.rotate_file(true).await?;
        }

        let data_len = data.len() as u64;

        // TODO: this will have data loss if disk is full, we need to fix this late to be CANCEL SAFE.
        self.current_file_buf.write_u64_le(data_len).await?;
        self.current_file_buf.write_all(&data).await?;

        self.current_size += data_len;

        Ok(())
    }

    /// Flush the buffer to the underlying Segment file.
    async fn flush(&mut self) -> WalResult<()> {
        self.current_file_buf.flush().await?;
        Ok(())
    }

    /// Open a segment for Appending. The file will be truncated if it exists (it shouldn't!).
    async fn open_segment(
        wal_type: &WalType,
        base_path: &Path,
        idx: usize,
    ) -> WalResult<(String, BufWriter<File>)> {
        let timestamp = Utc::now().timestamp_micros();

        // the name is important. sorting is based on the timestamp and then on file-index.
        let filename = format!(
            "{}_{}_{}.wal{}",
            wal_type.segment_prefix(),
            idx,
            timestamp,
            wal_type.segment_suffix()
        );

        let new_path = base_path.join(filename.clone());

        debug!(path = %new_path.display(), "Opening new WAL segment file");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .truncate(true)
            .open(&new_path)
            .await?;

        Ok((new_path.display().to_string(), BufWriter::new(file)))
    }

    /// Flush and possibly rotate since there are no new writes to the file.
    async fn flush_and_rotate(&mut self) -> WalResult<()> {
        // if the file has not been rotated in these many seconds, let's rotate
        if Utc::now().signed_duration_since(self.create_time) > ROTATE_IF_STALE_DURATION {
            debug!(duration = ?ROTATE_IF_STALE_DURATION, "Rotating stale file, no entries for a while");
            self.rotate_file(true).await
        } else {
            self.flush().await
        }
    }

    /// Rotates the file and opens a new one if `open_new` is `true`. It renames the file on rotation
    /// and resets the internal fields.
    async fn rotate_file(&mut self, open_new: bool) -> WalResult<()> {
        if self.current_size == 0 {
            return Ok(());
        }

        info!(
            current_size = ?self.current_size,
            file_name = ?self.current_file_name,
            "Rotating WAL segment file"
        );
        self.flush().await?;

        // rename the current file before we start a new one. Remove the suffix before freezing.
        let to_file_name = if self.wal_type.segment_suffix().is_empty() {
            format!("{}.frozen", self.current_file_name)
        } else {
            // trim the suffix if suffix exists
            let to_file_name = self
                .current_file_name
                .trim_end_matches(&self.wal_type.segment_suffix());
            format!("{to_file_name}.frozen")
        };

        tokio::fs::rename(&self.current_file_name, &to_file_name).await?;
        info!(?self.current_file_name, ?to_file_name, "rename successful");

        if open_new {
            // open new segment
            self.file_index += 1;
            let (file_name, buf_file) =
                Self::open_segment(&self.wal_type, &self.base_path, self.file_index).await?;

            self.current_file_name = file_name;
            self.current_file_buf = buf_file;
            self.create_time = Utc::now();
            self.current_size = 0;
        }
        Ok(())
    }
}

/// Append Only WAL.
#[derive(Clone)]
pub(crate) struct AppendOnlyWal {
    wal_type: WalType,
    base_path: PathBuf,
    max_file_size_mb: u64,
    flush_interval_ms: u64,
    max_segment_age_secs: u64,
}

impl AppendOnlyWal {
    /// Creates an [AppendOnlyWal]
    pub(crate) async fn new(
        wal_type: WalType,
        base_path: PathBuf,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        max_segment_age_secs: u64,
    ) -> WalResult<Self> {
        tokio::fs::create_dir_all(&base_path).await?;
        Ok(Self {
            wal_type,
            base_path,
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        })
    }

    /// Start the WAL for streaming write.
    /// Messages are kept alive until write completes, ensuring Arc<AckHandle> is not dropped prematurely.
    /// CANCEL SAFETY: This is not cancel safe. This is because our writes are buffered.
    pub(crate) async fn streaming_write(
        self,
        stream: ReceiverStream<SegmentWriteMessage>,
    ) -> WalResult<JoinHandle<WalResult<()>>> {
        let (file_name, file_buf) =
            SegmentWriteActor::open_segment(&self.wal_type, &self.base_path, 0).await?;

        let max_file_size_bytes = self.max_file_size_mb * 1024 * 1024;
        let flush_duration = Duration::from_millis(self.flush_interval_ms);
        let max_segment_age = chrono::Duration::seconds(self.max_segment_age_secs as i64);

        let mut actor = SegmentWriteActor::new(
            self.wal_type,
            self.base_path,
            file_name,
            Utc::now(),
            file_buf,
            max_file_size_bytes,
            flush_duration,
            max_segment_age,
        );

        actor.current_size = 0;
        actor.file_index = 0;

        let handle = tokio::spawn(async move {
            actor.start_processing(stream).await?;
            Ok(())
        });

        info!("FileWriterActor spawned and running.");
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{IntOffset, Message, Offset};
    use crate::reduce::wal::segment::WalType;
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

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        )
        .await
        .expect("WAL creation failed");

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
            .send(SegmentWriteMessage::WriteMessage { message: msg1 })
            .await
            .unwrap();

        let id2 = Offset::Int(IntOffset::new(2, 0));
        let msg2 = Message {
            offset: id2.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage { message: msg2 })
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
            .send(SegmentWriteMessage::WriteMessage { message: msg3 })
            .await
            .unwrap();

        let id4 = Offset::Int(IntOffset::new(4, 0));
        let msg4 = Message {
            offset: id4.clone(),
            value: vec![b'B'; large_data_size].into(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage { message: msg4 })
            .await
            .unwrap();

        let id5 = Offset::Int(IntOffset::new(5, 0));
        let msg5 = Message {
            offset: id5.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage { message: msg5 })
            .await
            .unwrap();

        let id6 = Offset::Int(IntOffset::new(6, 0));
        let msg6 = Message {
            offset: id6.clone(),
            ..Default::default()
        };
        wal_tx
            .send(SegmentWriteMessage::WriteMessage { message: msg6 })
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

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        )
        .await
        .expect("failed to create wal");

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
            .send(SegmentWriteMessage::WriteMessage { message: msg1 })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(flush_interval_ms * 3)).await;

        let files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .map_or(false, |ext| ext == "wal" || ext == "frozen")
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

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        )
        .await
        .expect("failed to create wal");

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
            .send(SegmentWriteMessage::WriteMessage { message: msg1 })
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
            .send(SegmentWriteMessage::WriteMessage { message: msg2 })
            .await
            .unwrap();

        drop(wal_tx);

        // Allow some time for the rotation to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the files
        let mut files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().map_or(false, |ext| ext == "frozen"))
            .collect();
        files.sort();

        assert_eq!(files.len(), 2, "There should be 2 WAL segment files");

        // Verify the rotated file is renamed correctly
        assert!(
            files[0]
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

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        )
        .await
        .expect("failed to create wal");

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
            .send(SegmentWriteMessage::WriteMessage { message: msg1 })
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
            .send(SegmentWriteMessage::WriteMessage { message: msg2 })
            .await
            .unwrap();

        // Allow some time for the rotation to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify the files
        let mut files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().map_or(false, |ext| ext == "frozen"))
            .collect();
        files.sort();

        assert!(
            files.len() >= 1,
            "There should be at least one frozen file after time-based rotation"
        );

        // File content check removed since messages are now serialized via WalMessage

        drop(wal_tx);
    }
}
