use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::WalType;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use std::{io, path::PathBuf};
use tokio::io::BufWriter;
use tokio::task::JoinHandle;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::mpsc::{self, Sender},
    time::{interval, Duration},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

pub(crate) enum FileWriterMessage {
    /// Writes the given payload to the WAL.
    WriteData {
        /// Unique ID of the payload. Useful to detect write failures.
        id: String,
        // TODO: add Option<event-time>
        /// Data to be written on do the WAL.
        data: Bytes,
    },
    /// Rotates a file on demand. We do not have use-case for this in the normal code path.
    #[cfg(test)]
    Rotate,
}

struct FileWriterActor {
    wal_type: WalType,
    base_path: PathBuf,
    current_file_name: String,
    current_file: BufWriter<File>,
    current_size: u64,
    file_index: usize,
    flush_interval: Duration,
    max_file_size: u64,
    result_tx: Sender<String>,
    in_rx: ReceiverStream<FileWriterMessage>,
}

impl Drop for FileWriterActor {
    fn drop(&mut self) {
        // TODO: do drop on shutdown on error path
    }
}

impl FileWriterActor {
    fn new(
        wal_type: WalType,
        base_path: PathBuf,
        current_file_name: String,
        current_file: BufWriter<File>,
        max_file_size: u64,
        flush_interval: Duration,
        result_tx: Sender<String>,
        in_rx: ReceiverStream<FileWriterMessage>,
    ) -> Self {
        Self {
            wal_type,
            base_path,
            current_file_name,
            current_file,
            current_size: 0,
            file_index: 0,
            max_file_size,
            flush_interval,
            result_tx,
            in_rx,
        }
    }

    async fn start_processing(mut self) -> WalResult<()> {
        let mut flush_timer = interval(self.flush_interval);
        loop {
            tokio::select! {
                maybe_msg = self.in_rx.next() => {
                    let Some(msg) = maybe_msg else {
                        break;
                    };
                    self.handle_message(msg).await?;
                }
                _ = flush_timer.tick() => {
                    self.flush().await?;
                }
            }
        }
        self.flush().await
    }

    // handle message should only return critical errors
    async fn handle_message(&mut self, msg: FileWriterMessage) -> WalResult<()> {
        match msg {
            FileWriterMessage::WriteData { id, data } => {
                self.write_data(data).await?;
                self.result_tx.send(id).await.unwrap();
            }
            #[cfg(test)]
            FileWriterMessage::Rotate => {
                self.rotate_file().await?;
            }
        }
        Ok(())
    }

    async fn write_data(&mut self, data: Bytes) -> WalResult<()> {
        // TODO: we should add support to rotate based on time as well, gc event wals should be
        // rotated quickly
        if self.current_size > 0 && self.current_size + data.len() as u64 > self.max_file_size {
            self.rotate_file().await?;
        }

        let data_len = data.len() as u64;

        // TODO: this will have data loss if disk is full, we need to fix this late to be CANCEL SAFE.
        self.current_file.write_u64_le(data_len).await?;
        self.current_file.write_all(&data).await?;

        self.current_size += data_len;

        Ok(())
    }

    async fn flush(&mut self) -> WalResult<()> {
        self.current_file.flush().await?;
        Ok(())
    }

    async fn open_file(
        wal_type: &WalType,
        base_path: &PathBuf,
        idx: usize,
    ) -> WalResult<(String, BufWriter<File>)> {
        let timestamp_nanos = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros());

        let filename = format!(
            "{}_{}_{}.wal",
            wal_type.segment_prefix(),
            idx,
            timestamp_nanos
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

    async fn rotate_file(&mut self) -> WalResult<()> {
        info!(
            current_size = ?self.current_size,
            file_name = ?self.current_file_name,
            "Rotating WAL segment file"
        );
        self.flush().await?;

        // rename the current file before we start a new one.
        tokio::fs::rename(
            &self.current_file_name,
            format!("{}.frozen", self.current_file_name),
        )
        .await?;

        self.file_index += 1;
        let (file_name, buf_file) =
            Self::open_file(&self.wal_type, &self.base_path, self.file_index).await?;

        self.current_file_name = file_name;
        self.current_file = buf_file;
        self.current_size = 0;

        Ok(())
    }
}

/// Creates an AppendOnly WAL.
pub(crate) struct AppendOnlyWal {
    wal_type: WalType,
    base_path: PathBuf,
    max_file_size_mb: u64,
    flush_interval_ms: u64,
    channel_buffer_size: usize,
}

impl AppendOnlyWal {
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
        stream: ReceiverStream<FileWriterMessage>,
    ) -> WalResult<(ReceiverStream<String>, JoinHandle<WalResult<()>>)> {
        let (file_name, initial_file) =
            FileWriterActor::open_file(&self.wal_type, &self.base_path, 0).await?;

        let (result_tx, result_rx) = mpsc::channel::<String>(self.channel_buffer_size);

        let max_file_size_bytes = self.max_file_size_mb * 1024 * 1024;
        let flush_duration = Duration::from_millis(self.flush_interval_ms);

        let mut actor = FileWriterActor::new(
            self.wal_type,
            self.base_path,
            file_name,
            initial_file,
            max_file_size_bytes,
            flush_duration,
            result_tx,
            stream,
        );

        actor.current_size = 0;
        actor.file_index = 0;

        let handle = tokio::spawn(async move {
            actor.start_processing().await?;
            Ok(())
        });

        info!("FileWriterActor spawned and running.");

        Ok((ReceiverStream::new(result_rx), handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            WalType::new("segment"),
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("WAL creation failed");

        let (wal_tx, wal_rx) = mpsc::channel::<FileWriterMessage>(channel_buffer);
        let (mut result_rx, writer_handle) = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        let mut expected_ids = Vec::new();
        let mut received_results = Vec::new();

        let id1 = "msg-001".to_string();
        let data1 = Bytes::from("some initial data");
        expected_ids.push(id1.clone());
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id1,
                data: data1.clone(),
            })
            .await
            .unwrap();

        let id2 = "msg-002".to_string();
        let data2 = Bytes::from(" more data here");
        expected_ids.push(id2.clone());
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id2,
                data: data2.clone(),
            })
            .await
            .unwrap();

        let large_data_size = (0.6 * 1024.0 * 1024.0) as usize;
        let large_data1 = Bytes::from(vec![b'A'; large_data_size]);
        let id3 = "msg-large-1".to_string();
        expected_ids.push(id3.clone());
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id3,
                data: large_data1.clone(),
            })
            .await
            .unwrap();

        let large_data2 = Bytes::from(vec![b'B'; large_data_size]);
        let id4 = "msg-large-2".to_string();
        expected_ids.push(id4.clone());
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id4,
                data: large_data2.clone(),
            })
            .await
            .unwrap();

        let id5 = "msg-005-rotated".to_string();
        let data5 = Bytes::from("data after rotation");
        expected_ids.push(id5.clone());
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id5,
                data: data5.clone(),
            })
            .await
            .unwrap();

        let id6 = "msg-006-rotated".to_string();
        let data6 = Bytes::from(" more data after rotation");
        expected_ids.push(id6.clone());
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id6,
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
            WalType::new("segment"),
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("failed to create wal");

        let (wal_tx, wal_rx) = mpsc::channel::<FileWriterMessage>(channel_buffer);
        let (mut result_rx, writer_handle) = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        let id1 = "flush-test-1".to_string();
        let data1 = Bytes::from("Data to be flushed");
        wal_tx
            .send(FileWriterMessage::WriteData {
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
        assert_eq!(result, id1, "Should have received the correct ID back");

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
            WalType::new("segment"),
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("failed to create wal");

        let (wal_tx, wal_rx) = mpsc::channel::<FileWriterMessage>(channel_buffer);
        let (_result_rx, writer_handle) = wal_writer
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .expect("Failed to start WAL service");

        // Send some data to the WAL
        let id1 = "msg-001".to_string();
        let data1 = Bytes::from("data before rotation");
        wal_tx
            .send(FileWriterMessage::WriteData {
                id: id1.clone(),
                data: data1.clone(),
            })
            .await
            .unwrap();

        // Send the Rotate command
        wal_tx.send(FileWriterMessage::Rotate).await.unwrap();

        // Send more data after rotation
        let id2 = "msg-002".to_string();
        let data2 = Bytes::from("data after rotation");
        wal_tx
            .send(FileWriterMessage::WriteData {
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
            .filter(|path| {
                path.extension()
                    .map_or(false, |ext| ext == "wal" || ext == "frozen")
            })
            .collect();
        files.sort();

        assert_eq!(
            files.len(),
            2,
            "There should be 2 WAL segment files (one rotated and one active)"
        );

        // Verify the rotated file is renamed correctly
        assert!(files[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".frozen"));

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
