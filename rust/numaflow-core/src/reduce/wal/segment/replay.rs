use crate::reduce::wal::error::WalResult;
use bytes::Bytes;
use std::fs;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;

/// Segment Entry as recorded in the WAL.
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

    pub(crate) fn streaming_read(self) -> WalResult<Sender<SegmentEntry>> {
        todo!()
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
