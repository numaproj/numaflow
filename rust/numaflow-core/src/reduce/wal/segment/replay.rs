use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::wal::SegmentReader;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
#[derive(Clone)]
pub(in crate::reduce) struct ReplayWal {
    reader: Arc<dyn SegmentReader>,
}

impl ReplayWal {
    /// Creates a new Replayer over the given reader.
    pub(in crate::reduce) fn new(reader: Arc<dyn SegmentReader>) -> Self {
        Self { reader }
    }

    /// Reads the WAL segments and streams them in-order. The stream is closed once all entries are
    /// read. A [SegmentEntry::CmdFileSwitch] is emitted after each segment with its id.
    pub(in crate::reduce) fn streaming_read(
        self,
    ) -> WalResult<(ReceiverStream<SegmentEntry>, JoinHandle<WalResult<()>>)> {
        let (tx, rx) = mpsc::channel::<SegmentEntry>(128);

        let handle = tokio::spawn(async move {
            let reader = self.reader;
            let segments = reader.list_segments().await?;
            debug!(count = segments.len(), "Found WAL segments for replay");

            info!("Starting WAL replay...");
            for segment in segments {
                info!(segment = %segment, "Replaying");
                reader.read_segment(&segment, tx.clone()).await?;

                tx.send(SegmentEntry::CmdFileSwitch {
                    filename: PathBuf::from(segment.as_str()),
                })
                .await
                .expect("rx dropped")
            }
            info!("Finished WAL replay task...");

            Ok(())
        });

        Ok((ReceiverStream::new(rx), handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reduce::wal::segment::WalType;
    use crate::reduce::wal::segment::wal::fs::FileSystemWal;
    use crate::reduce::wal::segment::wal::{FsStore, SegmentWriter, WalStore};
    use bytes::Bytes;
    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    // Listing/sorting/framing are exercised directly by the FileSystemWal tests in `wal/fs.rs`.
    // Here we cover the replay-level contract: entries stream in WAL order with a CmdFileSwitch
    // (carrying the segment id) emitted after each segment.
    #[tokio::test]
    async fn streaming_read_in_order_with_file_switch() {
        let dir = tempdir().expect("temp dir");
        let base = dir.path().to_path_buf();

        // Write two segments via the fs backend.
        let mut writer = FileSystemWal::new(WalType::Data, base.clone())
            .await
            .unwrap();
        writer.write(Bytes::from_static(b"a")).await.unwrap();
        writer.write(Bytes::from_static(b"b")).await.unwrap();
        writer.rotate(true).await.unwrap();
        writer.write(Bytes::from_static(b"c")).await.unwrap();
        writer.rotate(false).await.unwrap();

        let reader = FsStore::new(base).reader(WalType::Data).await.unwrap();
        let (mut stream, handle) = ReplayWal::new(reader).streaming_read().unwrap();

        let mut data = Vec::new();
        let mut switches = 0;
        while let Some(entry) = stream.next().await {
            match entry {
                SegmentEntry::DataEntry { data: d, .. } => data.push(d),
                SegmentEntry::CmdFileSwitch { filename } => {
                    assert!(filename.to_string_lossy().ends_with(".frozen"));
                    switches += 1;
                }
                SegmentEntry::DataFooter { .. } => unreachable!(),
            }
        }
        handle.await.unwrap().unwrap();

        assert_eq!(
            data,
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]
        );
        assert_eq!(switches, 2, "one CmdFileSwitch per segment");
    }

    #[tokio::test]
    async fn streaming_read_empty_is_clean() {
        let dir = tempdir().expect("temp dir");
        let reader = FsStore::new(dir.path().to_path_buf())
            .reader(WalType::Data)
            .await
            .unwrap();
        let (mut stream, handle) = ReplayWal::new(reader).streaming_read().unwrap();
        assert!(stream.next().await.is_none());
        handle.await.unwrap().unwrap();
    }
}
