use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::path::Path;
use std::{io, path::PathBuf};
use tokio::io::BufWriter;
use tokio::task::JoinHandle;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::mpsc::{self, Sender},
    time::{Duration, interval},
};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

/// Duration after which the WAL Segment is considered stale.
const ROTATE_IF_STALE_DURATION: chrono::Duration = chrono::Duration::seconds(30);

/// The Command that has to be operated on the Segment.
pub(crate) enum SegmentWriteMessage {
    /// Writes the given payload to the WAL.
    WriteData {
        /// Unique ID of the payload. Useful to detect write failures.
        id: Option<String>,
        /// Data to be written on do the WAL.
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
    /// The result of [SegmentWriteMessage] operation.
    result_tx: Sender<String>,
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
        result_tx: Sender<String>,
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
            result_tx,
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
            SegmentWriteMessage::WriteData { id, data } => {
                self.write_data(data).await?;
                // we need to respond only if ID is provided
                if let Some(id) = id {
                    self.result_tx.send(id).await.unwrap();
                }
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
        if self.current_size > 0 && self.current_size + data.len() as u64 >= self.max_file_size {
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
        let timestamp_nanos = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros());

        // the name is important. sorting is based on the timestamp and then on file-index.
        let filename = format!(
            "{}_{}_{}.wal{}",
            wal_type.segment_prefix(),
            idx,
            timestamp_nanos,
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
            format!("{}.frozen", to_file_name)
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
    channel_buffer_size: usize,
}

impl AppendOnlyWal {
    /// Creates an [AppendOnlyWal]
    pub(crate) async fn new(
        wal_type: WalType,
        base_path: PathBuf,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        channel_buffer_size: usize,
    ) -> Result<Self, io::Error> {
        tokio::fs::create_dir_all(&base_path).await?;
        Ok(Self {
            wal_type,
            base_path,
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer_size,
        })
    }

    /// Start the WAL for streaming write.
    /// CANCEL SAFETY: This is not cancel safe. This is because our writes are buffered.
    pub(crate) async fn streaming_write(
        self,
        stream: ReceiverStream<SegmentWriteMessage>,
    ) -> WalResult<(ReceiverStream<String>, JoinHandle<WalResult<()>>)> {
        let (file_name, file_buf) =
            SegmentWriteActor::open_segment(&self.wal_type, &self.base_path, 0).await?;

        let (result_tx, result_rx) = mpsc::channel::<String>(self.channel_buffer_size);

        let max_file_size_bytes = self.max_file_size_mb * 1024 * 1024;
        let flush_duration = Duration::from_millis(self.flush_interval_ms);

        let mut actor = SegmentWriteActor::new(
            self.wal_type,
            self.base_path,
            file_name,
            Utc::now(),
            file_buf,
            max_file_size_bytes,
            flush_duration,
            result_tx,
        );

        actor.current_size = 0;
        actor.file_index = 0;

        let handle = tokio::spawn(async move {
            actor.start_processing(stream).await?;
            Ok(())
        });

        info!("FileWriterActor spawned and running.");

        Ok((ReceiverStream::new(result_rx), handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reduce::wal::segment::WalType;
    use bytes::{Bytes, BytesMut};
    use futures::stream::StreamExt;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_write_receive_results_and_rotate() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let max_file_size_mb = 1;
        let flush_interval_ms = 50;
        let channel_buffer = 10;

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("WAL creation failed");

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let (mut result_rx, _writer_handle) = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        let mut expected_ids = Vec::new();
        let mut received_results = Vec::new();

        let id1 = "msg-001".to_string();
        let data1 = Bytes::from("some initial data");
        expected_ids.push(id1.clone());
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: Some(id1),
                data: data1.clone(),
            })
            .await
            .unwrap();

        let id2 = "msg-002".to_string();
        let data2 = Bytes::from(" more data here");
        expected_ids.push(id2.clone());
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: Some(id2),
                data: data2.clone(),
            })
            .await
            .unwrap();

        let large_data_size = (0.6 * 1024.0 * 1024.0) as usize;
        let large_data1 = Bytes::from(vec![b'A'; large_data_size]);
        let id3 = "msg-large-1".to_string();
        expected_ids.push(id3.clone());
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: Some(id3),
                data: large_data1.clone(),
            })
            .await
            .unwrap();

        let large_data2 = Bytes::from(vec![b'B'; large_data_size]);
        let id4 = "msg-large-2".to_string();
        expected_ids.push(id4.clone());
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: Some(id4),
                data: large_data2.clone(),
            })
            .await
            .unwrap();

        let id5 = "msg-005-rotated".to_string();
        let data5 = Bytes::from("data after rotation");
        expected_ids.push(id5.clone());
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: Some(id5),
                data: data5.clone(),
            })
            .await
            .unwrap();

        let id6 = "msg-006-rotated".to_string();
        let data6 = Bytes::from(" more data after rotation");
        expected_ids.push(id6.clone());
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: Some(id6),
                data: data6.clone(),
            })
            .await
            .unwrap();

        drop(wal_tx);

        while let Some(result) = result_rx.next().await {
            received_results.push(result);
        }

        assert_eq!(
            received_results.len(),
            expected_ids.len(),
            "Should receive a result for every message sent"
        );

        let mut received_ids = Vec::new();
        for result in received_results {
            received_ids.push(result);
        }

        received_ids.sort();
        expected_ids.sort();
        assert_eq!(
            received_ids, expected_ids,
            "Mismatch between expected and received successful IDs"
        );

        let mut files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .map_or(false, |ext| ext == "wal" || ext == "frozen")
            }) // Ensure we only check .wal files
            .collect();
        files.sort();

        assert_eq!(
            files.len(),
            2,
            "There should be at-most 2 WAL segment files (segment_0 and segment_1)"
        );

        let first_file_content = fs::read(&files[0]).unwrap();
        let expected_first_file_content =
            [data1.as_ref(), data2.as_ref(), large_data1.as_ref()].concat();
        assert_eq!(
            first_file_content.len(),
            expected_first_file_content.len() + size_of::<[u8; 8]>() * 3,
            "Content mismatch in first segment file"
        );

        let second_file_content = fs::read(&files[1]).unwrap();
        let expected_second_file_content =
            [large_data2.as_ref(), data5.as_ref(), data6.as_ref()].concat();
        assert_eq!(
            second_file_content.len(),
            expected_second_file_content.len() + size_of::<[u8; 8]>() * 3,
            "Content mismatch in second segment file"
        );

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

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("failed to create wal");

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let (mut result_rx, _writer_handle) = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        let id1 = Some("flush-test-1".to_string());
        let data1 = Bytes::from("Data to be flushed");
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: id1.clone(),
                data: data1.clone(),
            })
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
        let file_content = fs::read(&files[0]).unwrap();
        let mut expected = BytesMut::new();
        expected.extend_from_slice(&data1.len().to_le_bytes());
        expected.extend_from_slice(&data1);
        assert_eq!(
            file_content, expected,
            "File content should match flushed data"
        );

        let result = result_rx.next().await.expect("Should receive a result");
        assert_eq!(
            result,
            id1.unwrap(),
            "Should have received the correct ID back"
        );

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

        let wal_writer = AppendOnlyWal::new(
            WalType::Data,
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("failed to create wal");

        let (wal_tx, wal_rx) = mpsc::channel::<SegmentWriteMessage>(channel_buffer);
        let (_result_rx, _writer_handle) = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        // Send some data to the WAL
        let id1 = Some("msg-001".to_string());
        let data1 = Bytes::from("data before rotation");
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: id1.clone(),
                data: data1.clone(),
            })
            .await
            .unwrap();

        // Send the Rotate command
        wal_tx
            .send(SegmentWriteMessage::Rotate { on_size: false })
            .await
            .unwrap();

        // Send more data after rotation
        let id2 = Some("msg-002".to_string());
        let data2 = Bytes::from("data after rotation");
        wal_tx
            .send(SegmentWriteMessage::WriteData {
                id: id2.clone(),
                data: data2.clone(),
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

        // Verify the content of the rotated file
        let rotated_file_content = fs::read(&files[0]).unwrap();
        let mut expected_rotated_content = BytesMut::new();
        expected_rotated_content.extend_from_slice(&data1.len().to_le_bytes());
        expected_rotated_content.extend_from_slice(&data1);
        assert_eq!(
            rotated_file_content, expected_rotated_content,
            "Rotated file content mismatch"
        );

        // Verify the content of the active file
        let active_file_content = fs::read(&files[1]).unwrap();
        let mut expected_active_content = BytesMut::new();
        expected_active_content.extend_from_slice(&data2.len().to_le_bytes());
        expected_active_content.extend_from_slice(&data2);
        assert_eq!(
            active_file_content, expected_active_content,
            "Active file content mismatch"
        );

        // FIXME: why does this hang?
        // writer_handle
        //     .await
        //     .expect("writer shouldn't fail")
        //     .expect("WAL shouldn't raise error");
    }
}
