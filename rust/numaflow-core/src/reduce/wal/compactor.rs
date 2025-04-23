//! Compactor has business logic. It knows what kind of WALs have been created and will
//! compact based on the type. WAL inherently is agnostic to data. The compactor will be given
//! multiple WAL types (data, gc, etc.) and it decides how to purge (aka compact).

use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
use crate::reduce::wal::GcEventEntry;
use crate::reduce::wal::WalType;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use tokio_stream::StreamExt;

/// WALs can represent two Kinds of Windows and data is different for each Kind.
pub(crate) enum WindowKind {
    /// Aligned represents Fixed and Sliding Windows.
    Aligned,
    /// Unaligned represents Session Windows and Accumulators (almost like Global Windows).
    Unaligned,
}

/// A Compactor that compacts based on the GC and Segment WAL files in the given path. It can
/// compact both [WindowKind] of WALs.
pub(crate) struct Compactor {
    gc: WalType,
    segment: ReplayWal,
    path: PathBuf,
    kind: WindowKind,
}

const WAL_KEY_SEPERATOR: &'static str = ":";

impl Compactor {
    pub(crate) fn new(gc: WalType, segment: WalType, path: PathBuf, kind: WindowKind) -> Self {
        let segment = ReplayWal::new(segment, path.clone());

        Self {
            gc,
            segment,
            path,
            kind,
        }
    }

    /// Compact first needs to get all the GC files and build a compaction map. This map will have
    /// the oldest data before which all can be deleted.
    pub(crate) async fn compact(&self) -> WalResult<()> {
        todo!()
    }

    // FIXME: BEFORE we implement this, we need to add footer.

    /// Compacts Aligned Segments.
    /// ## Logic
    /// - Get the oldest time by calling Build Aligned Compaction
    /// - Get all the Segment files
    /// - Open Append WAL for Compaction
    /// - For each Segment file, deserialize data to [crate::message::Message]
    /// - Compare the "event_time" of the Message with the oldest time
    /// - If event_time is <= oldest_time, skip it, otherwise write it into the Compaction Append WAL.
    /// - Send the Rotate message to the Compaction Append WAL after each Segment file has been processed.
    /// - Delete the Segment File after the Rotate is complete.
    async fn compact_aligned(&self) -> WalResult<()> {
        todo!()
    }

    async fn compact_unaligned(&self) -> WalResult<()> {
        todo!()
    }

    /// Builds the oldest time below which all data has been processed. For Aligned WAL, all we need
    /// to track is the oldest timestamp. We do not have to worry about the keys.
    /// It returns the list of GC files scanned.
    async fn build_aligned_compaction(&self) -> WalResult<(DateTime<Utc>, Vec<PathBuf>)> {
        // the oldest time across all GC Segments.
        let mut oldest_time = DateTime::from(UNIX_EPOCH);

        // list of GC Segments scanned
        let mut scanned_files = vec![];

        let gc = ReplayWal::new(self.gc.clone(), self.path.clone());

        let (mut rx, handle) = gc.streaming_read()?;

        while let Some(entry) = rx.next().await {
            match entry {
                SegmentEntry::DataEntry { data, .. } => {
                    let gc: GcEvent = prost::Message::decode(data)
                        .map_err(|e| format!("prost decoding failed, {e}"))?;

                    let gc: GcEventEntry = gc.into();

                    if gc.end_time > oldest_time {
                        oldest_time = gc.end_time
                    }
                }
                SegmentEntry::DataFooter { .. } => {
                    unimplemented!()
                }
                SegmentEntry::CmdFileSwitch { filename } => {
                    scanned_files.push(filename);
                }
            }
        }

        handle.await.map_err(|e| format!("Join Failed, {e}"))??;

        Ok((oldest_time, scanned_files))
    }

    /// Builds the oldest time below which all data has been processed. For Unaligned WAL, we need
    /// to track is the oldest timestamp for the given keys.
    /// It returns the list of GC files scanned.
    async fn build_unaligned_compaction(
        &self,
    ) -> WalResult<(HashMap<String, DateTime<Utc>>, Vec<PathBuf>)> {
        // list of keys to oldest time mapping
        let mut oldest_time_map = HashMap::new();
        // list of GC Segments scanned
        let mut scanned_files = vec![];

        let gc = ReplayWal::new(self.gc.clone(), self.path.clone());

        let (mut rx, handle) = gc.streaming_read()?;

        while let Some(entry) = rx.next().await {
            match entry {
                SegmentEntry::DataEntry { data, .. } => {
                    let gc: GcEvent = prost::Message::decode(data)
                        .map_err(|e| format!("prost decoding failed, {e}"))?;

                    let gc: GcEventEntry = gc.into();

                    // insert only if the gc entry has higher end time and if entry does not exist,
                    // insert the entry.
                    oldest_time_map
                        .entry(gc.keys.unwrap_or(vec![]).join(WAL_KEY_SEPERATOR))
                        .and_modify(|dt| {
                            if &gc.end_time > dt {
                                *dt = gc.end_time
                            }
                        })
                        .or_insert(gc.end_time);
                }
                SegmentEntry::DataFooter { .. } => {
                    unimplemented!()
                }
                SegmentEntry::CmdFileSwitch { filename } => {
                    scanned_files.push(filename);
                }
            }
        }

        handle.await.map_err(|e| format!("Join Failed, {e}"))??;

        Ok((oldest_time_map, scanned_files))
    }
}
