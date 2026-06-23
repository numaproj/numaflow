//! Filesystem WAL storage. Segments are files named `{prefix}_{idx}_{ts}.wal{suffix}`, records are
//! framed `<u64 len><bytes>`, and a seal renames the open file to `<name>.frozen`. Rotation policy
//! (size/age) lives in the caller; this type just appends and seals.

use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::replay::SegmentEntry;
use crate::reduce::wal::segment::wal::{SegmentCompactor, SegmentId, SegmentReader, SegmentWriter};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use std::cmp::Ordering;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

/// Filesystem segment storage for one [WalType] under `base_path`. Readers/compactors built from
/// the same path share the directory on disk.
pub(crate) struct FileSystemWal {
    wal_type: WalType,
    base_path: PathBuf,
    /// The currently-open (unsealed) segment, if any.
    current: Option<OpenSegment>,
    /// Index of the next segment to open.
    next_index: usize,
}

/// The open, not-yet-sealed segment.
struct OpenSegment {
    /// Full path of the open `.wal` file.
    path: PathBuf,
    writer: BufWriter<File>,
    /// Whether anything has been written since open (empty segments are not sealed).
    has_data: bool,
}

impl FileSystemWal {
    /// Create storage for `wal_type` under `base_path`.
    pub(crate) async fn new(wal_type: WalType, base_path: PathBuf) -> WalResult<Self> {
        tokio::fs::create_dir_all(&base_path).await?;
        Ok(Self {
            wal_type,
            base_path,
            current: None,
            next_index: 0,
        })
    }

    /// Open a new segment file for appending. Truncates if it somehow exists.
    /// File name format (unchanged): `{prefix}_{idx}_{ts_micros}.wal{suffix}`.
    async fn open_segment(&mut self) -> WalResult<()> {
        let idx = self.next_index;
        self.next_index += 1;

        let timestamp = Utc::now().timestamp_micros();
        let filename = format!(
            "{}_{}_{}.wal{}",
            self.wal_type.segment_prefix(),
            idx,
            timestamp,
            self.wal_type.segment_suffix()
        );
        let path = self.base_path.join(filename);
        debug!(path = %path.display(), "Opening new WAL segment file");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .truncate(true)
            .open(&path)
            .await?;

        self.current = Some(OpenSegment {
            path,
            writer: BufWriter::new(file),
            has_data: false,
        });
        Ok(())
    }
}

#[async_trait]
impl SegmentWriter for FileSystemWal {
    async fn write(&mut self, data: Bytes) -> WalResult<()> {
        if self.current.is_none() {
            self.open_segment().await?;
        }
        let seg = self.current.as_mut().expect("segment just opened");

        // Framing: <u64 little-endian len><bytes>.
        seg.writer.write_u64_le(data.len() as u64).await?;
        seg.writer.write_all(&data).await?;
        seg.has_data = true;
        Ok(())
    }

    async fn flush(&mut self) -> WalResult<()> {
        if let Some(seg) = self.current.as_mut() {
            seg.writer.flush().await?;
        }
        Ok(())
    }

    async fn rotate(&mut self, open_new: bool) -> WalResult<()> {
        // nothing open: optionally open a fresh segment and return
        let Some(seg) = self.current.as_mut() else {
            if open_new {
                self.open_segment().await?;
            }
            return Ok(());
        };

        // empty segment is not sealed
        if !seg.has_data {
            return Ok(());
        }

        info!(file = %seg.path.display(), "Rotating WAL segment file");
        seg.writer.flush().await?;

        // rename to `<name>.frozen`, trimming the suffix first
        let path_str = seg.path.display().to_string();
        let suffix = self.wal_type.segment_suffix();
        let frozen = if suffix.is_empty() {
            format!("{path_str}.frozen")
        } else {
            format!("{}.frozen", path_str.trim_end_matches(suffix))
        };

        tokio::fs::rename(&seg.path, &frozen).await?;
        info!(from = %path_str, to = %frozen, "rename successful");

        self.current = None;
        if open_new {
            self.open_segment().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SegmentReader for FileSystemWal {
    async fn list_segments(&self) -> WalResult<Vec<SegmentId>> {
        let mut files = list_frozen_files(&self.wal_type, &self.base_path)?;
        sort_filenames(&mut files);
        Ok(files
            .into_iter()
            .map(|p| SegmentId::new(p.display().to_string()))
            .collect())
    }

    async fn read_segment(&self, id: &SegmentId, tx: Sender<SegmentEntry>) -> WalResult<()> {
        let path = PathBuf::from(id.as_str());
        let file = OpenOptions::new().read(true).open(&path).await?;
        let mut reader = BufReader::new(file);

        // Each record is <u64 little-endian len><bytes>.
        loop {
            let data_len = match reader.read_u64_le().await {
                Ok(len) => len,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("expected to read_u64 but couldn't {e}").into()),
            };

            let mut buffer = vec![0; data_len as usize];
            if let Err(e) = reader.read_exact(&mut buffer).await {
                return Err(
                    format!("expected to read {data_len}, but couldn't read_exact {e}").into(),
                );
            }

            tx.send(SegmentEntry::DataEntry {
                size: data_len,
                data: Bytes::from(buffer),
            })
            .await
            .map_err(|_| "replay receiver dropped".to_string())?;
        }
        Ok(())
    }
}

#[async_trait]
impl SegmentCompactor for FileSystemWal {
    async fn delete(&self, id: &SegmentId) -> WalResult<()> {
        match tokio::fs::remove_file(id.as_str()).await {
            Ok(()) => Ok(()),
            // Idempotent: a missing segment is not an error.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// List all `.frozen` segment files for the given [WalType].
fn list_frozen_files(wal_type: &WalType, base_path: &Path) -> WalResult<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(base_path) {
        Ok(e) => e,
        // no directory yet, so no segments
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(wal_type.segment_prefix())
            && path.extension().is_some_and(|ext| ext == "frozen")
        {
            files.push(path);
        }
    }
    Ok(files)
}

/// Sort filenames on the timestamp and, on conflict, on the file-index.
fn sort_filenames(files: &mut [PathBuf]) {
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

        match ts_a.cmp(&ts_b) {
            Ordering::Equal => idx_a.cmp(&idx_b),
            v => v,
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn rec(b: &[u8]) -> Bytes {
        Bytes::copy_from_slice(b)
    }

    async fn drain(reader: &FileSystemWal, id: &SegmentId) -> Vec<Bytes> {
        let (tx, mut rx) = mpsc::channel(16);
        let read = reader.read_segment(id, tx);
        let collect = async {
            let mut out = Vec::new();
            while let Some(entry) = rx.recv().await {
                if let SegmentEntry::DataEntry { data, .. } = entry {
                    out.push(data);
                }
            }
            out
        };
        let (res, out) = tokio::join!(read, collect);
        res.unwrap();
        out
    }

    #[tokio::test]
    async fn write_rotate_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let mut wal = FileSystemWal::new(WalType::Data, base.clone())
            .await
            .unwrap();

        wal.write(rec(b"hello")).await.unwrap();
        wal.write(rec(b"world")).await.unwrap();
        wal.rotate(true).await.unwrap();

        wal.write(rec(b"second-seg")).await.unwrap();
        wal.rotate(false).await.unwrap();

        // A reader sharing the directory sees both frozen segments, in WAL order.
        let reader = FileSystemWal::new(WalType::Data, base).await.unwrap();
        let segments = reader.list_segments().await.unwrap();
        let [first, second] = segments.as_slice() else {
            panic!("expected two frozen segments, got {}", segments.len());
        };
        assert!(first < second);

        assert_eq!(
            drain(&reader, first).await,
            vec![rec(b"hello"), rec(b"world")]
        );
        assert_eq!(drain(&reader, second).await, vec![rec(b"second-seg")]);
    }

    #[tokio::test]
    async fn empty_rotate_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = FileSystemWal::new(WalType::Data, dir.path().to_path_buf())
            .await
            .unwrap();

        // Rotate with nothing written: no frozen files appear.
        wal.rotate(true).await.unwrap();
        wal.write(rec(b"x")).await.unwrap();
        // Rotate sees data and freezes; a follow-up empty rotate must not create a second segment.
        wal.rotate(true).await.unwrap();
        wal.rotate(true).await.unwrap();

        let reader = FileSystemWal::new(WalType::Data, dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(reader.list_segments().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn frozen_naming_and_prefix_filter() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();

        let mut data = FileSystemWal::new(WalType::Data, base.clone())
            .await
            .unwrap();
        let mut gc = FileSystemWal::new(WalType::Gc, base.clone()).await.unwrap();
        data.write(rec(b"d")).await.unwrap();
        data.rotate(true).await.unwrap();
        gc.write(rec(b"g")).await.unwrap();
        gc.rotate(true).await.unwrap();

        // Each reader only sees its own WalType's frozen files.
        let data_r = FileSystemWal::new(WalType::Data, base.clone())
            .await
            .unwrap();
        let gc_r = FileSystemWal::new(WalType::Gc, base.clone()).await.unwrap();
        let data_segs = data_r.list_segments().await.unwrap();
        let gc_segs = gc_r.list_segments().await.unwrap();
        let [data_seg] = data_segs.as_slice() else {
            panic!("expected one data segment, got {}", data_segs.len());
        };
        let [gc_seg] = gc_segs.as_slice() else {
            panic!("expected one gc segment, got {}", gc_segs.len());
        };
        assert!(data_seg.as_str().contains("data_"));
        assert!(data_seg.as_str().ends_with(".frozen"));
        assert!(gc_seg.as_str().contains("gc_"));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let mut wal = FileSystemWal::new(WalType::Data, base.clone())
            .await
            .unwrap();
        wal.write(rec(b"x")).await.unwrap();
        wal.rotate(true).await.unwrap();

        let reader = FileSystemWal::new(WalType::Data, base.clone())
            .await
            .unwrap();
        let segments = reader.list_segments().await.unwrap();
        let [seg] = segments.as_slice() else {
            panic!("expected one segment, got {}", segments.len());
        };

        wal.delete(seg).await.unwrap();
        assert!(reader.list_segments().await.unwrap().is_empty());
        // Deleting a missing segment is fine.
        wal.delete(seg).await.unwrap();
    }

    #[tokio::test]
    async fn list_on_missing_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist-yet");
        // Construct without create_dir_all by skipping `new`: emulate a reader over a dir that the
        // writer hasn't made yet. `new` would create it, so build the struct directly.
        let reader = FileSystemWal {
            wal_type: WalType::Data,
            base_path: missing,
            current: None,
            next_index: 0,
        };
        assert!(reader.list_segments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn boxed_dyn_dispatch_works() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let mut writer: Box<dyn SegmentWriter> = Box::new(
            FileSystemWal::new(WalType::Data, base.clone())
                .await
                .unwrap(),
        );
        let reader: Box<dyn SegmentReader> = Box::new(
            FileSystemWal::new(WalType::Data, base.clone())
                .await
                .unwrap(),
        );
        let compactor: Box<dyn SegmentCompactor> =
            Box::new(FileSystemWal::new(WalType::Data, base).await.unwrap());

        writer.write(rec(b"x")).await.unwrap();
        writer.flush().await.unwrap();
        writer.rotate(true).await.unwrap();

        let segments = reader.list_segments().await.unwrap();
        let [seg] = segments.as_slice() else {
            panic!("expected one segment, got {}", segments.len());
        };
        compactor.delete(seg).await.unwrap();
        assert!(reader.list_segments().await.unwrap().is_empty());
    }
}
