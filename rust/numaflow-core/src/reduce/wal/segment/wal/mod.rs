//! Pluggable storage for the WAL's segments. A segment is written, read, and deleted in full.
//! Each storage medium implements the three traits below; the store is picked at runtime via
//! `Box<dyn ...>`.

pub(crate) mod fs;
pub(crate) mod memory;

use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::replay::SegmentEntry;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

/// Constructs storage handles per [WalType]. A single store is injected at boot and shared by all
/// the WAL components, so all WAL types share the same storage.
#[async_trait]
pub(in crate::reduce) trait WalStore: Send + Sync {
    /// A writer for `wal_type`.
    async fn writer(&self, wal_type: WalType) -> WalResult<Box<dyn SegmentWriter>>;
    /// A reader for `wal_type`. Returned as `Arc` so [ReplayWal](super::replay::ReplayWal) can clone it.
    async fn reader(&self, wal_type: WalType) -> WalResult<Arc<dyn SegmentReader>>;
    /// A compactor for `wal_type` (removes sealed segments).
    async fn compactor(&self, wal_type: WalType) -> WalResult<Box<dyn SegmentCompactor>>;
}

/// Filesystem-backed [WalStore] rooted at a base directory.
pub(crate) struct FsStore {
    base_path: PathBuf,
}

impl FsStore {
    pub(crate) fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl WalStore for FsStore {
    async fn writer(&self, wal_type: WalType) -> WalResult<Box<dyn SegmentWriter>> {
        Ok(Box::new(
            fs::FileSystemWal::new(wal_type, self.base_path.clone()).await?,
        ))
    }

    async fn reader(&self, wal_type: WalType) -> WalResult<Arc<dyn SegmentReader>> {
        Ok(Arc::new(
            fs::FileSystemWal::new(wal_type, self.base_path.clone()).await?,
        ))
    }

    async fn compactor(&self, wal_type: WalType) -> WalResult<Box<dyn SegmentCompactor>> {
        Ok(Box::new(
            fs::FileSystemWal::new(wal_type, self.base_path.clone()).await?,
        ))
    }
}

/// Opaque, orderable identifier for a sealed segment (a path for the fs store, a name for memory).
/// `list_segments` returns ids already sorted in WAL order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SegmentId(pub(crate) String);

impl SegmentId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        SegmentId(id.into())
    }

    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Writes records into the currently-open segment and seals segments on rotation.
#[async_trait]
pub(crate) trait SegmentWriter: Send + Sync {
    /// Append a single record to the open segment.
    async fn write(&mut self, data: Bytes) -> WalResult<()>;

    /// Flush buffered data without sealing the segment. No-op when the seal is the durable point.
    async fn flush(&mut self) -> WalResult<()>;

    /// Seal the current segment, opening a fresh one when `open_new`. No-op if the segment is empty.
    async fn rotate(&mut self, open_new: bool) -> WalResult<()>;
}

/// Lists and reads sealed segments in WAL order.
#[async_trait]
pub(in crate::reduce) trait SegmentReader: Send + Sync {
    /// All sealed segments, sorted in WAL order.
    async fn list_segments(&self) -> WalResult<Vec<SegmentId>>;

    /// Stream one segment's entries in order into `tx`.
    async fn read_segment(&self, id: &SegmentId, tx: Sender<SegmentEntry>) -> WalResult<()>;
}

/// Removes sealed segments. Driven by the [Compactor](super::compactor::Compactor).
#[async_trait]
pub(crate) trait SegmentCompactor: Send + Sync {
    /// Delete a sealed segment. Deleting a missing segment is not an error.
    async fn delete(&self, id: &SegmentId) -> WalResult<()>;
}
