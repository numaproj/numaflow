//! In-memory WAL storage for tests. Sealed segments live in a shared map keyed per [WalType];
//! components built from the same [InMemoryStore] see each other's segments.

use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::replay::SegmentEntry;
use crate::reduce::wal::segment::wal::{
    SegmentCompactor, SegmentId, SegmentReader, SegmentWriter, WalStore,
};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;

/// In-memory storage for one [WalType]. Implements all three storage traits. Segment ids are a
/// zero-padded sequence so their natural ordering is WAL order.
#[derive(Clone)]
pub(crate) struct InMemoryWal {
    wal_type: WalType,
    inner: Arc<Inner>,
    /// Framed bytes of the segment currently being written.
    buffer: Vec<u8>,
}

#[derive(Default)]
struct Inner {
    /// wal_type prefix -> (segment id -> framed bytes of the sealed segment).
    segments: Mutex<BTreeMap<String, BTreeMap<SegmentId, Bytes>>>,
    /// Monotonic counter for building sortable segment ids.
    seq: AtomicU64,
}

/// Shared in-memory store. WAL components built from the same store observe the same segments.
#[derive(Clone, Default)]
pub(crate) struct InMemoryStore {
    inner: Arc<Inner>,
}

impl InMemoryStore {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A handle into this store scoped to `wal_type`.
    pub(crate) fn wal(&self, wal_type: WalType) -> InMemoryWal {
        InMemoryWal {
            wal_type,
            inner: Arc::clone(&self.inner),
            buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl WalStore for InMemoryStore {
    async fn writer(&self, wal_type: WalType) -> WalResult<Box<dyn SegmentWriter>> {
        Ok(Box::new(self.wal(wal_type)))
    }

    async fn reader(&self, wal_type: WalType) -> WalResult<Arc<dyn SegmentReader>> {
        Ok(Arc::new(self.wal(wal_type)))
    }

    async fn compactor(&self, wal_type: WalType) -> WalResult<Box<dyn SegmentCompactor>> {
        Ok(Box::new(self.wal(wal_type)))
    }
}

impl InMemoryWal {
    /// Create storage for `wal_type` with a fresh, empty segment map.
    #[allow(dead_code)]
    pub(crate) fn new(wal_type: WalType) -> Self {
        Self {
            wal_type,
            inner: Arc::new(Inner::default()),
            buffer: Vec::new(),
        }
    }

    /// Another handle to the same segment map for a different [WalType].
    #[allow(dead_code)]
    pub(crate) fn handle_for(&self, wal_type: WalType) -> Self {
        Self {
            wal_type,
            inner: Arc::clone(&self.inner),
            buffer: Vec::new(),
        }
    }

    /// Build a sortable segment id: `{prefix}_{seq:020}`.
    fn next_id(&self) -> SegmentId {
        let seq = self.inner.seq.fetch_add(1, Ordering::Relaxed);
        SegmentId::new(format!("{}_{seq:020}", self.wal_type.segment_prefix()))
    }

    /// Number of sealed segments (test helper).
    #[cfg(test)]
    pub(crate) async fn segment_count(&self) -> usize {
        self.inner
            .segments
            .lock()
            .await
            .get(self.wal_type.segment_prefix())
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

#[async_trait]
impl SegmentWriter for InMemoryWal {
    async fn write(&mut self, data: Bytes) -> WalResult<()> {
        // Same framing as the on-disk segments: <u64 little-endian len><bytes>.
        self.buffer
            .extend_from_slice(&(data.len() as u64).to_le_bytes());
        self.buffer.extend_from_slice(&data);
        Ok(())
    }

    async fn flush(&mut self) -> WalResult<()> {
        // nothing to flush; the segment becomes readable only on rotate
        Ok(())
    }

    async fn rotate(&mut self, _open_new: bool) -> WalResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let id = self.next_id();
        let sealed = Bytes::from(std::mem::take(&mut self.buffer));
        self.inner
            .segments
            .lock()
            .await
            .entry(self.wal_type.segment_prefix().to_string())
            .or_default()
            .insert(id, sealed);
        Ok(())
    }
}

#[async_trait]
impl SegmentReader for InMemoryWal {
    async fn list_segments(&self) -> WalResult<Vec<SegmentId>> {
        let guard = self.inner.segments.lock().await;
        // BTreeMap keys are already sorted == WAL order.
        Ok(guard
            .get(self.wal_type.segment_prefix())
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn read_segment(&self, id: &SegmentId, tx: Sender<SegmentEntry>) -> WalResult<()> {
        let bytes = {
            let guard = self.inner.segments.lock().await;
            guard
                .get(self.wal_type.segment_prefix())
                .and_then(|m| m.get(id))
                .cloned()
        };
        let Some(bytes) = bytes else {
            return Err(format!("segment {id} not found").into());
        };

        // Decode the <u64 len><bytes> framing, mirroring the on-disk replay loop.
        let mut buf = bytes;
        while buf.has_remaining() {
            if buf.remaining() < 8 {
                return Err("truncated length header in segment".to_string().into());
            }
            let len = buf.get_u64_le() as usize;
            if buf.remaining() < len {
                return Err("truncated record body in segment".to_string().into());
            }
            let data = buf.copy_to_bytes(len);
            tx.send(SegmentEntry::DataEntry {
                size: len as u64,
                data,
            })
            .await
            .map_err(|_| "replay receiver dropped".to_string())?;
        }
        Ok(())
    }
}

#[async_trait]
impl SegmentCompactor for InMemoryWal {
    async fn delete(&self, id: &SegmentId) -> WalResult<()> {
        // Idempotent: removing a missing segment is fine.
        if let Some(m) = self
            .inner
            .segments
            .lock()
            .await
            .get_mut(self.wal_type.segment_prefix())
        {
            m.remove(id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn rec(b: &[u8]) -> Bytes {
        Bytes::copy_from_slice(b)
    }

    async fn drain(reader: &InMemoryWal, id: &SegmentId) -> Vec<Bytes> {
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
        let mut wal = InMemoryWal::new(WalType::Data);

        // Empty rotate is a no-op.
        wal.rotate(true).await.unwrap();
        assert_eq!(wal.segment_count().await, 0);

        wal.write(rec(b"hello")).await.unwrap();
        wal.write(rec(b"world")).await.unwrap();
        wal.rotate(true).await.unwrap();

        wal.write(rec(b"second-seg")).await.unwrap();
        wal.rotate(false).await.unwrap();

        let segments = wal.list_segments().await.unwrap();
        let [first, second] = segments.as_slice() else {
            panic!("expected two segments, got {}", segments.len());
        };
        // Ordering must be WAL order (first segment before second).
        assert!(first < second);

        assert_eq!(drain(&wal, first).await, vec![rec(b"hello"), rec(b"world")]);
        assert_eq!(drain(&wal, second).await, vec![rec(b"second-seg")]);
    }

    #[tokio::test]
    async fn delete_removes_segment() {
        let mut wal = InMemoryWal::new(WalType::Gc);

        wal.write(rec(b"gc-event")).await.unwrap();
        wal.rotate(true).await.unwrap();
        let segments = wal.list_segments().await.unwrap();
        let [seg] = segments.as_slice() else {
            panic!("expected one segment, got {}", segments.len());
        };

        wal.delete(seg).await.unwrap();
        assert!(wal.list_segments().await.unwrap().is_empty());

        // Deleting again is idempotent.
        wal.delete(seg).await.unwrap();
    }

    #[tokio::test]
    async fn shared_handles_see_each_others_segments() {
        // A writer and a reader/compactor that share storage via handle_for must agree.
        let mut writer = InMemoryWal::new(WalType::Data);
        let reader = writer.handle_for(WalType::Data);
        let compactor = writer.handle_for(WalType::Data);

        writer.write(rec(b"x")).await.unwrap();
        writer.rotate(true).await.unwrap();

        let segments = reader.list_segments().await.unwrap();
        let [seg] = segments.as_slice() else {
            panic!("expected one segment, got {}", segments.len());
        };
        assert_eq!(drain(&reader, seg).await, vec![rec(b"x")]);

        compactor.delete(seg).await.unwrap();
        assert!(reader.list_segments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn wal_types_are_isolated() {
        // Same underlying map, different WalType => isolated segment lists.
        let mut data_w = InMemoryWal::new(WalType::Data);
        let mut gc_w = data_w.handle_for(WalType::Gc);

        data_w.write(rec(b"d")).await.unwrap();
        data_w.rotate(true).await.unwrap();
        gc_w.write(rec(b"g")).await.unwrap();
        gc_w.rotate(true).await.unwrap();

        assert_eq!(data_w.segment_count().await, 1);
        assert_eq!(gc_w.segment_count().await, 1);
        assert_eq!(data_w.list_segments().await.unwrap().len(), 1);
        assert_eq!(gc_w.list_segments().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn boxed_dyn_dispatch_works() {
        // Validate object-safety: each trait must be usable as a trait object.
        let base = InMemoryWal::new(WalType::Data);
        let mut writer: Box<dyn SegmentWriter> = Box::new(base.clone());
        let reader: Box<dyn SegmentReader> = Box::new(base.clone());
        let compactor: Box<dyn SegmentCompactor> = Box::new(base.clone());

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
