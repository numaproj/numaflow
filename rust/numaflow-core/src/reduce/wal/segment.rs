use bytes::Bytes;
use chrono::Utc;
use std::{io, path::PathBuf};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::mpsc::{self, Receiver, Sender},
    time::{interval, Duration},
};
use tracing::{debug, error, info};

pub(crate) enum FileWriterMessage {
    Data(Bytes),
    Rotate,
}

struct FileWriterActor {
    base_path: PathBuf,
    current_file: File,
    current_size: u64,
    file_index: usize,
    flush_interval_millis: Duration,
    max_file_size: u64,
    rotate: bool,
}

impl FileWriterActor {
    async fn run(mut self, mut rx: Receiver<FileWriterMessage>) -> io::Result<()> {
        let mut flush_timer = interval(self.flush_interval_millis);
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    match msg {
                        FileWriterMessage::Data(data) => {
                            self.write_data(data).await?;
                        }
                        FileWriterMessage::Rotate => {
                            self.rotate = true
                        }
                    }
                }
                _ = flush_timer.tick() => {
                    self.flush().await?;
                }
            }
        }

        info!("WAL Segment rx has ended, doing a final flush and exiting");

        self.flush().await?;

        Ok(())
    }

    async fn write_data(&mut self, data: Bytes) -> io::Result<()> {
        if self.current_size >= self.max_file_size || self.rotate {
            self.rotate_file().await?;
        }

        self.current_file.write_all(&data).await?;
        self.current_size += data.len() as u64;

        self.rotate = false;

        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.current_file.flush().await
    }

    async fn open_file(base_path: &PathBuf, idx: usize) -> io::Result<File> {
        let new_path = base_path.join(format!(
            "segment_{}_{}.data",
            idx,
            Utc::now().timestamp_nanos_opt().unwrap_or(0) // this is to avoid collision across restarts
        ));

        debug!(?new_path, "opened new WAL segment");

        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(new_path)
            .await
    }

    async fn rotate_file(&mut self) -> io::Result<()> {
        self.current_file.flush().await?;

        let file = Self::open_file(&self.base_path, self.file_index).await?;

        self.current_file = file;
        self.current_size = 0;
        Ok(())
    }
}

impl FileWriterActor {
    pub(crate) fn new(
        base_path: PathBuf,
        current_file: File,
        max_file_size: u64,
        flush_interval_millis: Duration,
    ) -> Self {
        Self {
            base_path,
            current_file,
            current_size: 0,
            file_index: 1,
            max_file_size,
            flush_interval_millis,
            rotate: false,
        }
    }
}

async fn start_file_writer(
    file_writer_actor: FileWriterActor,
) -> io::Result<Sender<FileWriterMessage>> {
    let (tx, rx) = mpsc::channel(100);

    tokio::spawn(async move {
        if let Err(e) = file_writer_actor.run(rx).await {
            error!(?e, "FileWriterActor error");
        }
    });

    Ok(tx)
}

// pub(crate) async fn start(base_path: PathBuf) -> io::Result<Sender<FileWriterMessage>> {
//     let file = FileWriterActor::open_file(&base_path, 0).await?;
//
//     let actor = FileWriterActor::new(
//         base_path,
//         file,
//         100 * 1024 * 1024,
//         Duration::from_millis(1000),
//     );
//
//     start_file_writer(actor).await
// }

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_writer_actor_write_and_rotate() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let initial_file = FileWriterActor::open_file(&base_path, 0).await.unwrap();
        let max_file_size = 10; // Small size to trigger rotation
        let flush_interval = Duration::from_millis(10);

        let actor = FileWriterActor::new(
            base_path.clone(),
            initial_file,
            max_file_size,
            flush_interval,
        );
        let sender = start_file_writer(actor).await.unwrap();

        // Send data to the actor
        sender
            .send(FileWriterMessage::Data(Bytes::from("12345")))
            .await
            .unwrap();
        sender
            .send(FileWriterMessage::Data(Bytes::from("67890")))
            .await
            .unwrap();

        // Trigger rotation
        sender.send(FileWriterMessage::Rotate).await.unwrap();

        // Send more data after rotation
        sender
            .send(FileWriterMessage::Data(Bytes::from("abcde")))
            .await
            .unwrap();

        // Allow some time for the actor to process messages
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Verify files
        let files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();

        assert_eq!(files.len(), 2, "There should be 2 rotated files");

        // Verify content of the first file
        let first_file_content = fs::read_to_string(&files[0]).unwrap();
        assert_eq!(first_file_content, "1234567890");

        // Verify content of the second file
        let second_file_content = fs::read_to_string(&files[1]).unwrap();
        assert_eq!(second_file_content, "abcde");
    }

    #[tokio::test]
    async fn test_file_writer_actor_flush() {
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let initial_file = FileWriterActor::open_file(&base_path, 0).await.unwrap();
        let max_file_size = 100;
        let flush_interval = Duration::from_millis(10);

        let actor = FileWriterActor::new(
            base_path.clone(),
            initial_file,
            max_file_size,
            flush_interval,
        );
        let sender = start_file_writer(actor).await.unwrap();

        // Send data to the actor
        sender
            .send(FileWriterMessage::Data(Bytes::from("test data")))
            .await
            .unwrap();

        // Allow some time for the actor to flush
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Verify file content
        let files: Vec<_> = fs::read_dir(&base_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();

        assert_eq!(files.len(), 1, "There should be 1 file");

        let file_content = fs::read_to_string(&files[0]).unwrap();
        assert_eq!(file_content, "test data");
    }
}
