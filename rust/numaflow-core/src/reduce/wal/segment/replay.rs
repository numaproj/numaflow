use crate::reduce::wal::error::WalResult;
use bytes::{Bytes, BytesMut};
use std::fs;
use std::io;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::task::JoinHandle;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::mpsc::{self, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

/// Segment Entry as recorded in the WAL.
#[derive(Debug)]
pub(crate) enum SegmentEntry {
    Data { size: u64, data: Bytes },
}

pub(crate) struct ReplayWal {
    segment_prefix: &'static str,
    base_path: PathBuf,
}

impl ReplayWal {
    pub(crate) fn new(segment_prefix: &'static str, base_path: PathBuf) -> Self {
        Self {
            segment_prefix,
            base_path,
        }
    }

    pub(crate) fn streaming_read(
        self,
    ) -> WalResult<(ReceiverStream<SegmentEntry>, JoinHandle<WalResult<()>>)> {
        let mut files: Vec<PathBuf> = list_files(self.segment_prefix, self.base_path.clone());
        files.sort();

        debug!(count = files.len(), "Found WAL segment files for replay");

        let (tx, rx) = mpsc::channel::<SegmentEntry>(128);

        let handle = task::spawn(async move {
            info!("Starting WAL replay...");
            for file_path in files {
                Self::read_segment_file(&file_path, tx.clone()).await?;
            }
            info!("Finished WAL replay task...");
            Ok(())
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    // TODO: sorting

    async fn read_segment_file(path: &PathBuf, tx: Sender<SegmentEntry>) -> WalResult<()> {
        let file = OpenOptions::new().read(true).open(path).await?;

        let mut reader = BufReader::new(file);

        // we read data_len first and then move our offset up till the len
        // refresher: each entry in our file is <u64(data_len)><[u8;data_len]>
        loop {
            // read len first
            let data_len_result = reader.read_u64_le().await;
            let data_len = match data_len_result {
                Ok(len) => len,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("expected to read_u64 but couldn't {}", e).into()),
            };

            // make sure we have data for that len
            let mut buffer = BytesMut::with_capacity(data_len as usize);
            // this is a critical error, we should be able to read data of len data_len
            if let Err(e) = reader.read_exact(&mut buffer).await {
                return Err(format!(
                    "expected to read {}, but couldn't read_exact {}",
                    data_len, e
                )
                .into());
            }

            // send each line
            tx.send(SegmentEntry::Data {
                size: data_len,
                data: buffer.freeze(),
            })
            .await
            .expect("rx dropped while replaying");
        }

        Ok(())
    }
}

fn list_files(segment_prefix: &'static str, base_path: PathBuf) -> Vec<PathBuf> {
    fs::read_dir(&base_path)
        .expect(&format!("directory {} to be present", base_path.display()))
        .map(|entry| entry.expect("expect dirEntry to be good").path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.file_name()
                .expect("filename expected")
                .to_str()
                .expect("conversion should work")
                .starts_with(segment_prefix)
        })
        .filter(|path| path.extension().map_or(false, |ext| ext == "frozen"))
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn test_list_files() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        // Create some test files
        let _file1 =
            File::create(base_path.join("test_segment_1.frozen")).expect("Failed to create file");
        let _file2 =
            File::create(base_path.join("test_segment_2.frozen")).expect("Failed to create file");
        let _file3 =
            File::create(base_path.join("test_segment_3.txt")).expect("Failed to create file");
        let _file4 =
            File::create(base_path.join("other_file.frozen")).expect("Failed to create file");

        // Call the function
        let result = list_files("test_segment", base_path);

        // Verify the result
        let mut result_paths: Vec<String> = result
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        result_paths.sort();

        assert_eq!(
            result_paths,
            vec!["test_segment_1.frozen", "test_segment_2.frozen"]
        );
    }
}
