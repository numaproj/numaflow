use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use std::{io, path::PathBuf};
use tokio::io::BufWriter;
use tokio::task::JoinHandle;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::mpsc::{self, Receiver, Sender},
    time::{interval, Duration},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

// TODO: we should use a custom error which will have the information of id, so that the callee can nack the message when there is an error.
pub(crate) type WriteResult = Result<String, io::Error>;

pub(crate) enum FileWriterMessage {
    WriteData { id: String, data: Bytes },
    Rotate,
}

struct FileWriterActor {
    base_path: PathBuf,
    current_file: BufWriter<File>,
    current_size: u64,
    file_index: usize,
    flush_interval: Duration,
    max_file_size: u64,
    result_tx: Sender<String>,
    in_rx: ReceiverStream<FileWriterMessage>,
}

impl Drop for FileWriterActor {
    fn drop(&mut self) {}
}

impl FileWriterActor {
    async fn start_processing(mut self) -> Result<(), io::Error> {
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
    async fn handle_message(&mut self, msg: FileWriterMessage) -> io::Result<()> {
        match msg {
            FileWriterMessage::WriteData { id, data } => {
                self.write_data(data).await?;
                self.result_tx.send(id).await.unwrap();
            }
            FileWriterMessage::Rotate => {
                self.rotate_file().await?;
            }
        }
        Ok(())
    }

    async fn write_data(&mut self, data: Bytes) -> io::Result<()> {
        if self.current_size > 0 && self.current_size + data.len() as u64 > self.max_file_size {
            self.rotate_file().await?;
        }

        let data_len = data.len() as u64;
        self.current_file.write_all(&data).await?;
        self.current_size += data_len;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        self.current_file.flush().await?;
        Ok(())
    }

    async fn open_file(base_path: &PathBuf, idx: usize) -> io::Result<BufWriter<File>> {
        let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let new_path = base_path.join(format!("segment_{:06}_{}.wal", idx, timestamp_nanos));

        debug!(path = %new_path.display(), "Opening new WAL segment file");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .truncate(true)
            .open(&new_path)
            .await?;

        Ok(BufWriter::new(file))
    }

    async fn rotate_file(&mut self) -> Result<(), io::Error> {
        info!(
            current_size = ?self.current_size,
            file_index = ?self.file_index,
            "Rotating WAL segment file"
        );
        self.flush().await?;

        self.file_index += 1;
        let new_file = Self::open_file(&self.base_path, self.file_index).await?;

        self.current_file = new_file;
        self.current_size = 0;
        Ok(())
    }
}

impl FileWriterActor {
    pub(crate) fn new(
        base_path: PathBuf,
        current_file: BufWriter<File>,
        max_file_size: u64,
        flush_interval: Duration,
        result_tx: Sender<String>,
        in_rx: ReceiverStream<FileWriterMessage>,
    ) -> Self {
        Self {
            base_path,
            current_file,
            current_size: 0,
            file_index: 0,
            max_file_size,
            flush_interval,
            result_tx,
            in_rx,
        }
    }
}

pub(crate) struct WAL {
    base_path: PathBuf,
    max_file_size_mb: u64,
    flush_interval_ms: u64,
    channel_buffer_size: usize,
}

impl WAL {
    pub(crate) async fn new(
        base_path: PathBuf,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        channel_buffer_size: usize,
    ) -> Result<Self, io::Error> {
        tokio::fs::create_dir_all(&base_path).await?;
        Ok(Self {
            base_path,
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer_size,
        })
    }

    // TODO: should streaming write return the spawned task handle so that we can await on that for any critical errors
    pub(crate) async fn streaming_write(
        self,
        stream: ReceiverStream<FileWriterMessage>,
    ) -> Result<(ReceiverStream<String>, JoinHandle<Result<(), io::Error>>), io::Error> {
        let initial_file = FileWriterActor::open_file(&self.base_path, 0).await?;

        let (result_tx, result_rx) = mpsc::channel::<String>(self.channel_buffer_size);

        let max_file_size_bytes = self.max_file_size_mb * 1024 * 1024;
        let flush_duration = Duration::from_millis(self.flush_interval_ms);

        let mut actor = FileWriterActor::new(
            self.base_path,
            initial_file,
            max_file_size_bytes,
            flush_duration,
            result_tx,
            stream,
        );

        actor.current_size = 0;
        actor.file_index = 0;

        let handle: JoinHandle<Result<(), io::Error>> = tokio::spawn(async move {
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
    use bytes::Bytes;
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

        let wal_writer = WAL::new(
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("WAL creation failed");

        let (wal_tx, wal_rx) = mpsc::channel::<FileWriterMessage>(channel_buffer);
        let (mut result_rx, handle) = wal_writer
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
            .filter(|path| path.extension().map_or(false, |ext| ext == "wal")) // Ensure we only check .wal files
            .collect();
        files.sort();

        assert_eq!(
            files.len(),
            2,
            "There should be 2 WAL segment files (segment_0 and segment_1)"
        );

        let first_file_content = fs::read(&files[0]).unwrap();
        let expected_first_file_content =
            [data1.as_ref(), data2.as_ref(), large_data1.as_ref()].concat();
        assert_eq!(
            first_file_content, expected_first_file_content,
            "Content mismatch in first segment file"
        );

        let second_file_content = fs::read(&files[1]).unwrap();
        let expected_second_file_content =
            [large_data2.as_ref(), data5.as_ref(), data6.as_ref()].concat();
        assert_eq!(
            second_file_content, expected_second_file_content,
            "Content mismatch in second segment file"
        );
    }

    #[tokio::test]
    async fn test_wal_periodic_flush() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let max_file_size_mb = 10;
        let flush_interval_ms = 50;
        let channel_buffer = 10;

        let wal_writer = WAL::new(
            base_path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer,
        )
        .await
        .expect("failed to create wal");

        let (wal_tx, wal_rx) = mpsc::channel::<FileWriterMessage>(channel_buffer);
        let (mut result_rx, handle) = wal_writer
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
            .filter(|path| path.extension().map_or(false, |ext| ext == "wal"))
            .collect();

        assert_eq!(files.len(), 1, "There should be 1 WAL file");
        let file_content = fs::read(&files[0]).unwrap();
        assert_eq!(
            file_content, data1,
            "File content should match flushed data"
        );

        let result = result_rx.next().await.expect("Should receive a result");
        assert_eq!(result, id1, "Should have received the correct ID back");
    }
}
