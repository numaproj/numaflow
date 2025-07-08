use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use bytes::Bytes;
use std::cmp::Ordering;
use std::fs;
use std::io;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::task::JoinHandle;
use tokio::{
    io::{AsyncReadExt, BufReader},
    sync::mpsc::{self, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

/// Segment Entry as recorded in the WAL.
#[derive(Debug)]
pub(in crate::reduce) enum SegmentEntry {
    #[allow(dead_code)]
    /// Data entry in the Segment
    DataEntry { size: u64, data: Bytes },
    /// The file has been switched
    CmdFileSwitch { filename: PathBuf },
    /// Data footer in the Segment.
    /// TODO: This is for optimization which is yet to be implemented
    #[allow(dead_code)]
    DataFooter { size: u64, data: Bytes },
}

/// Replay the WAL in-order.
#[derive(Debug, Clone)]
pub(in crate::reduce) struct ReplayWal {
    wal_type: WalType,
    base_path: PathBuf,
}

impl ReplayWal {
    /// Creates a new Replayer for the WAL.
    pub(in crate::reduce) fn new(wal_type: WalType, base_path: PathBuf) -> Self {
        Self {
            wal_type,
            base_path,
        }
    }

    /// Reads the WAL files and streams it via the stream. Stream will be closed once all the
    /// entries are read.
    pub(in crate::reduce) fn streaming_read(
        self,
    ) -> WalResult<(ReceiverStream<SegmentEntry>, JoinHandle<WalResult<()>>)> {
        let mut files: Vec<PathBuf> = list_files(&self.wal_type, self.base_path.clone());
        files = sort_filenames(files);

        debug!(count = files.len(), "Found WAL segment files for replay");

        let (tx, rx) = mpsc::channel::<SegmentEntry>(128);

        let handle = task::spawn(async move {
            info!("Starting WAL replay...");
            for file_path in files {
                info!(file = %file_path.display(), "Replaying");
                Self::read_segment(&file_path, tx.clone()).await?;

                tx.send(SegmentEntry::CmdFileSwitch {
                    filename: file_path,
                })
                .await
                .expect("rx dropped")
            }
            info!("Finished WAL replay task...");

            Ok(())
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    /// Read the segment file in-order and write to the channel.
    async fn read_segment(path: &PathBuf, tx: Sender<SegmentEntry>) -> WalResult<()> {
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
                Err(e) => return Err(format!("expected to read_u64 but couldn't {e}").into()),
            };

            // make sure we have data for that len
            let mut buffer = vec![0; data_len as usize];

            // this is a critical error, we should be able to read data of len data_len
            if let Err(e) = reader.read_exact(&mut buffer).await {
                return Err(
                    format!("expected to read {data_len}, but couldn't read_exact {e}").into(),
                );
            }

            // send each line
            tx.send(SegmentEntry::DataEntry {
                size: data_len,
                data: Bytes::from(buffer),
            })
            .await
            .expect("rx dropped while replaying");
        }

        Ok(())
    }
}

/// Sort the filenames based on the file name. It is first sorted based on the timestamp and on
/// conflict sorted on the file-index.
fn sort_filenames(mut files: Vec<PathBuf>) -> Vec<PathBuf> {
    files.sort_by(|a, b| {
        let parse = |s: &str| {
            let parts: Vec<&str> = s.split('_').collect();
            let index = parts
                .get(1)
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);
            let ts_part = parts.get(2).unwrap_or(&"0");
            let timestamp = ts_part
                .split('.')
                .next()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
            (timestamp, index)
        };

        let (ts_a, idx_a) = parse(
            a.file_name()
                .expect("valid unix file")
                .to_str()
                .expect("filename is valid"),
        );
        let (ts_b, idx_b) = parse(
            b.file_name()
                .expect("valid unix file")
                .to_str()
                .expect("filename is valid"),
        );

        // first sort on timestamp, if it matches, then sort on index
        match ts_a.cmp(&ts_b) {
            Ordering::Equal => idx_a.cmp(&idx_b),
            v => v,
        }
    });

    files
}

/// List all the files for the given [WalType].
fn list_files(wal_type: &WalType, base_path: PathBuf) -> Vec<PathBuf> {
    fs::read_dir(&base_path)
        .unwrap_or_else(|_| panic!("directory {} to be present", base_path.display()))
        .map(|entry| entry.expect("expect dirEntry to be good").path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.file_name()
                .expect("filename expected")
                .to_str()
                .expect("conversion should work")
                .starts_with(wal_type.segment_prefix())
        })
        .filter(|path| path.extension().is_some_and(|ext| ext == "frozen"))
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reduce::wal::segment::WalType;
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn test_list_files() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        // Create some test files
        let _file1 = File::create(base_path.join("data_1.frozen")).expect("Failed to create file");
        let _file2 = File::create(base_path.join("data_2.frozen")).expect("Failed to create file");
        let _file3 = File::create(base_path.join("data_3.txt")).expect("Failed to create file");
        let _file4 =
            File::create(base_path.join("other_file.frozen")).expect("Failed to create file");

        // Call the function
        let result = list_files(&WalType::Data, base_path);

        // Verify the result
        let mut result_paths: Vec<String> = result
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        result_paths.sort();

        assert_eq!(result_paths, vec!["data_1.frozen", "data_2.frozen"]);
    }

    #[test]
    fn test_sort_filenames() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        // Create test files with the specified format
        let _file1 = File::create(base_path.join("segment_000001_1000000001.wal"))
            .expect("Failed to create file");
        let _file4 = File::create(base_path.join("segment_000002_999999999.wal"))
            .expect("Failed to create file");
        let _file2 = File::create(base_path.join("segment_000001_1000000000.wal"))
            .expect("Failed to create file");
        let _file3 = File::create(base_path.join("segment_000001_999999999.wal"))
            .expect("Failed to create file");

        // Collect the file paths
        let mut files: Vec<PathBuf> = fs::read_dir(&base_path)
            .expect("Failed to read directory")
            .map(|entry| entry.expect("Failed to read entry").path())
            .collect();

        // Sort the files using the function
        files = sort_filenames(files);

        // Verify the sorted order
        let sorted_filenames: Vec<String> = files
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();

        assert_eq!(
            sorted_filenames,
            vec![
                "segment_000001_999999999.wal",
                "segment_000002_999999999.wal",
                "segment_000001_1000000000.wal",
                "segment_000001_1000000001.wal",
            ],
            "File names are not sorted correctly"
        );
    }
}
