//! Compactor has business logic. It knows what kind of WALs have been created and will
//! compact based on the type. WAL inherently is agnostic to data. The compactor will be given
//! multiple WAL types (data, gc, etc.) and it decides how to purge (aka compact).
//!
//! Compaction is done by combing the [WalType::Gc] + [WalType::Data] + [WalType::Compact]
//!
//! ### Compaction Logic
//! #### Aligned kind
//! 1. Replay all the GC events and store the max end time
//! 2. Replay all the data events and only retain the messages with event time > max end time
//!
//! #### Unaligned kind
//! 1. Replay all the GC events and store the max end time for every key combination(map[key] = max end time)
//! 2. Replay all the data events and only retain the messages with event time > max end time for that key

use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::GcEventEntry;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use crate::reduce::wal::segment::replay::{
    ReplayWal, SegmentEntry, list_files_with_active, parse_segment_create_micros, sort_filenames,
};
use crate::shared::grpc::utc_from_timestamp;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::isb;
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// WALs can represent two Kinds of Windows and data is different for each Kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowKind {
    /// Aligned represents Fixed and Sliding Windows.
    Aligned,
    /// Unaligned represents Session Windows and Accumulators (almost like Global Windows).
    Unaligned,
}

/// A Compactor that compacts based on the GC and Segment WAL files in the given path. It can
/// compact both [WindowKind] of WALs.
pub(crate) struct Compactor {
    /// GC WAL
    gc_wal: ReplayWal,
    /// Segment WAL
    segment_wal: ReplayWal,
    /// Compaction WAL for Read Only
    compaction_ro_wal: ReplayWal,
    /// Compaction Append WAL tx for writing compacted data
    compaction_ao_tx: Sender<SegmentWriteMessage>,
    /// Kind of Window, Aligned or Unaligned
    kind: WindowKind,
    /// join handle for the compaction writer task
    writer_task_handle: JoinHandle<WalResult<()>>,
    /// Base path where the WAL segments are persisted.
    base_path: PathBuf,
}

const WAL_KEY_SEPERATOR: &str = ":";

impl Compactor {
    /// Creates a new Compactor.
    pub(crate) async fn new(
        path: PathBuf,
        kind: WindowKind,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        max_segment_age_secs: u64,
    ) -> WalResult<Self> {
        let segment_wal = ReplayWal::new(WalType::Data, path.clone());
        let gc_wal = ReplayWal::new(WalType::Gc, path.clone());
        let compaction_ro_wal = ReplayWal::new(WalType::Compact, path.clone());
        let compaction_ao_wal = AppendOnlyWal::new(
            WalType::Compact,
            path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            max_segment_age_secs,
        )
        .await?;

        let (compaction_ao_tx, compaction_ao_rx) = mpsc::channel(500);
        let handle = compaction_ao_wal
            .streaming_write(ReceiverStream::new(compaction_ao_rx))
            .await?;

        Ok(Self {
            gc_wal,
            segment_wal,
            compaction_ro_wal,
            compaction_ao_tx,
            kind,
            writer_task_handle: handle,
            base_path: path,
        })
    }

    /// Starts the compaction process with replay and periodic execution.
    ///
    /// # Parameters
    /// * `replay_tx` - Channel to replay compacted data. Compaction runs once and replays data first.
    /// * `interval` - Interval for periodic compaction. Compaction runs periodically after initial replay.
    /// * `cln_token` - Cancellation token for periodic compaction.
    pub(crate) async fn start_compaction_with_replay(
        self,
        replay_tx: Sender<Bytes>,
        interval: Duration,
        cln_token: CancellationToken,
    ) -> WalResult<JoinHandle<WalResult<()>>> {
        Ok(tokio::spawn(async move {
            // First do a one-time boot compaction with replay. The retention gate is never
            // applied at boot: `newest_data_micros` would be stale (reflecting only the
            // previous run's segments) and boot only reads pre-existing `.frozen` data, so no
            // GC segment is deleted here.
            self.compact(Some(replay_tx), false).await?;

            // Then start periodic compaction, with the retention gate applied.
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        self.compact(None, true).await?;
                    }
                    _ = cln_token.cancelled() => {
                        break;
                    }
                }
            }

            info!("Cancellation token received, stopping compaction task...");
            // drop the compactor append wal tx to signal the writer task to shut down
            drop(self.compaction_ao_tx);

            // Wait for the writer task to complete and return the result
            let result = self.writer_task_handle.await.expect("task failed");
            info!("Compaction task completed");

            result
        }))
    }

    /// Compact first needs to get all the GC files and build a compaction map. This map will have
    /// the oldest data before which all can be deleted.
    ///
    /// `apply_retention_gate` controls whether frozen GC segments are eligible
    /// for deletion this cycle:
    /// - `false` during the one-time boot compaction (retain all GC segments)
    /// - `true` on every periodic tick.
    async fn compact(
        &self,
        replay_tx: Option<Sender<Bytes>>,
        apply_retention_gate: bool,
    ) -> WalResult<()> {
        match self.kind {
            WindowKind::Aligned => {
                self.compact_aligned(replay_tx.clone(), apply_retention_gate)
                    .await?
            }
            WindowKind::Unaligned => {
                self.compact_unaligned(replay_tx, apply_retention_gate)
                    .await?
            }
        }
        Ok(())
    }

    /// Compacts Aligned Segments.
    /// ## Logic
    /// - Get the oldest time by calling Build Aligned Compaction
    /// - Get all the Segment files
    /// - Open Append WAL for Compaction
    /// - For each Segment file, deserialize data to [crate::message::Message]
    /// - Compare the "event_time" of the Message with the oldest time
    /// - If event_time is <= oldest_time, skip it, otherwise write it into the Compaction Append WAL.
    /// - Send the Rotate message to the Compaction Append WAL after each Segment file has been processed.
    /// - Delete the Segment File after the Rotate is complete, subject to the retention gate (see
    ///   [Compactor::compact]).
    async fn compact_aligned(
        &self,
        replay_tx: Option<mpsc::Sender<Bytes>>,
        apply_retention_gate: bool,
    ) -> WalResult<()> {
        // Snapshot the newest data-segment creation time once, before any WAL is read.
        let newest_data_micros = self.newest_data_segment_micros();

        // Get the oldest time and scanned GC files
        let (oldest_time, gc_files) = self.build_aligned_compaction().await?;

        debug!(oldest_time = ?oldest_time.timestamp_millis(), "Event time till which the data has been processed");

        let compact = AlignedCompaction(oldest_time);

        // Compact the compaction_ro_wal
        self.process_wal_stream(&self.compaction_ro_wal, &compact, replay_tx.clone())
            .await?;

        // Compact the segment_wal
        self.process_wal_stream(&self.segment_wal, &compact, replay_tx)
            .await?;

        // Delete the GC files, gated on retention (see [Compactor::compact]).
        self.delete_gc_files(gc_files, apply_retention_gate, newest_data_micros)
            .await?;

        Ok(())
    }

    /// Compacts Unaligned Segments.
    /// ## Logic
    /// - Get the oldest time by calling Build Unaligned Compaction
    /// - Get all the Segment files
    /// - Open Append WAL for Compaction
    /// - For each Segment file, deserialize data to [crate::message::Message]
    /// - Compare the "event_time" of the Message with the compaction hashmap. The key is the joined keys
    ///   are the key from the Message::Header.
    /// - If event_time is <= oldest_time, skip it, otherwise write it into the Compaction Append WAL.
    /// - Send the Rotate message to the Compaction Append WAL after each Segment file has been processed.
    /// - Delete the Segment File after the Rotate is complete, subject to the retention gate (see
    ///   [Compactor::compact]).
    async fn compact_unaligned(
        &self,
        replay_tx: Option<mpsc::Sender<Bytes>>,
        apply_retention_gate: bool,
    ) -> WalResult<()> {
        // Snapshot the newest data-segment creation time once, before any WAL is read.
        let newest_data_micros = self.newest_data_segment_micros();

        // Get the oldest time map and scanned GC files
        let (oldest_time_map, gc_files) = self.build_unaligned_compaction().await?;

        info!(oldest_time_map = ?oldest_time_map, "Event time map till which the data has been processed");

        let compact = UnalignedCompaction(oldest_time_map);

        // Compact the compaction_ro_wal
        self.process_wal_stream(&self.compaction_ro_wal, &compact, replay_tx.clone())
            .await?;

        // Compact the segment_wal
        self.process_wal_stream(&self.segment_wal, &compact, replay_tx)
            .await?;

        // Delete the GC files, gated on retention (see [Compactor::compact]).
        self.delete_gc_files(gc_files, apply_retention_gate, newest_data_micros)
            .await?;

        Ok(())
    }

    /// Deletes scanned GC segment files subject to the retention gate.
    ///
    /// A GC segment `g` is only deleted when:
    /// - `apply_retention_gate` is `true` AND
    /// - `newest_data_micros` is strictly greater than `g`'s **successor's** create-time.
    ///
    /// If `g` has no successor (possible during shutdown), it is retained (fail-safe).
    /// If above two conditions aren't satisfied, `g` is retained so it keeps contributing to next
    /// cycle's `oldest_time_map`/`oldest_time` (built fresh from whatever GC files are still present).
    async fn delete_gc_files(
        &self,
        gc_files: Vec<PathBuf>,
        apply_retention_gate: bool,
        newest_data_micros: Option<u64>,
    ) -> WalResult<()> {
        if !apply_retention_gate {
            debug!("retention gate disabled (boot compaction), retaining all GC segments");
            return Ok(());
        }

        // Single sorted snapshot of ALL GC segments (including the active `.wal`), used to look
        // up each candidate's successor. Taking this at the delete loop (rather than at cycle
        // start) is fine: successor create-times are immutable on-disk facts once a segment is
        // frozen, only their *existence* matters.
        let all_gc_sorted =
            sort_filenames(list_files_with_active(&WalType::Gc, self.base_path.clone()));

        for gc_file in gc_files {
            let successor_create_micros = successor_create_micros(&all_gc_sorted, &gc_file);

            let should_delete = newest_data_micros
                .zip(successor_create_micros)
                .is_some_and(|(newest, successor)| newest > successor);

            if should_delete {
                debug!(gc_file = %gc_file.display(), "removing segment file");
                tokio::fs::remove_file(&gc_file).await?;
            } else {
                debug!(gc_file = %gc_file.display(), "retaining segment file, not yet safe to delete");
            }
        }

        Ok(())
    }

    /// Returns the max create-time (unix micros, parsed from the filename) over all `data_*`
    /// segment files on disk, including the currently-active `.wal` segment. Returns `None` if
    /// there are no data segments at all.
    fn newest_data_segment_micros(&self) -> Option<u64> {
        list_files_with_active(&WalType::Data, self.base_path.clone())
            .into_iter()
            .filter_map(|path| {
                path.file_name()
                    .and_then(|n| n.to_str())
                    .map(parse_segment_create_micros)
            })
            .max()
    }

    /// Process the WAL stream (Readonly) and write the compacted data to the compaction WAL.
    /// Every message is checked against the should_retain function and if it returns true, it is
    /// written to the compaction WAL.
    async fn process_wal_stream<T: ShouldRetain>(
        &self,
        wal: &ReplayWal,
        should_retain: &T,
        replay_tx: Option<mpsc::Sender<Bytes>>,
    ) -> WalResult<()> {
        // Get a streaming reader for the WAL
        let (mut rx, handle) = wal.clone().streaming_read()?;

        let wal_tx = self.compaction_ao_tx.clone();

        // Process each WAL entry
        while let Some(entry) = rx.next().await {
            match entry {
                SegmentEntry::DataEntry { data, .. } => {
                    // Deserialize the message
                    let msg: isb::ReadMessage = prost::Message::decode(data.clone())
                        .map_err(|e| format!("Failed to decode message: {e}"))?;

                    if should_retain
                        .should_retain_message(&msg.message.expect("Message should be present"))?
                    {
                        // Send the message to the compaction WAL
                        // No message handle needed for compaction writes
                        wal_tx
                            .send(SegmentWriteMessage::WriteGcEvent { data: data.clone() })
                            .await
                            .map_err(|e| {
                                format!("Failed to send message to compaction WAL: {e}")
                            })?;

                        // if replay_tx is provided, send the message to it.
                        // This is used to replay the compacted data during boot up
                        if let Some(tx) = &replay_tx {
                            tx.send(data).await.map_err(|e| {
                                format!("Failed to send message to replay channel: {e}")
                            })?;
                        }
                    }
                }

                SegmentEntry::DataFooter { .. } => {
                    unimplemented!()
                }

                SegmentEntry::CmdFileSwitch { filename } => {
                    // Send rotate message after processing each file
                    wal_tx
                        .send(SegmentWriteMessage::Rotate { on_size: false })
                        .await
                        .map_err(|e| format!("Failed to send rotate command: {e}"))?;

                    // Delete the processed segment file
                    tokio::fs::remove_file(&filename).await.map_err(|e| {
                        format!(
                            "Failed to delete segment file {}: {}",
                            filename.display(),
                            e
                        )
                    })?;

                    debug!(filename = %filename.display(), "removing segment file");
                }
            }
        }

        // Wait for the WAL reader to complete
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {e}"))??;

        // Drop the sender to close the channel
        drop(wal_tx);

        Ok(())
    }

    /// Builds the oldest time below which all data has been processed. For Aligned WAL, all we need
    /// to track is the oldest timestamp. We do not have to worry about the keys.
    /// It returns the list of GC files scanned.
    async fn build_aligned_compaction(&self) -> WalResult<(DateTime<Utc>, Vec<PathBuf>)> {
        // The oldest time across all GC Segments.
        let mut oldest_time = DateTime::from(UNIX_EPOCH);

        // list of GC Segments scanned
        let mut scanned_files = vec![];

        // Get a streaming reader for the GC WAL.
        let (mut rx, handle) = self.gc_wal.clone().streaming_read()?;

        while let Some(entry) = rx.next().await {
            match entry {
                SegmentEntry::DataEntry { data, .. } => {
                    // Decode the GC event.
                    let gc: GcEvent = prost::Message::decode(data)
                        .map_err(|e| format!("Failed to decode GC event: {e}"))?;
                    let gc: GcEventEntry = gc.into();

                    // Update the oldest time if the current GC event's end time is newer.
                    oldest_time = oldest_time.max(gc.end_time);
                }
                SegmentEntry::DataFooter { .. } => {
                    unimplemented!()
                }
                SegmentEntry::CmdFileSwitch { filename } => {
                    // Add the scanned file to the list.
                    scanned_files.push(filename);
                }
            }
        }

        // Wait for the WAL reader to complete.
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {e}"))??;

        Ok((oldest_time, scanned_files))
    }

    /// Builds the oldest time below which all data has been processed. For Unaligned WAL, we need
    /// to track the oldest timestamp for the given keys.
    /// It returns the list of GC files scanned.
    async fn build_unaligned_compaction(
        &self,
    ) -> WalResult<(HashMap<String, DateTime<Utc>>, Vec<PathBuf>)> {
        // Map of keys to their oldest time
        let mut oldest_time_map = HashMap::new();
        // List of GC segment files scanned
        let mut scanned_files = Vec::new();

        // Get a streaming reader for the GC WAL
        let (mut rx, handle) = self.gc_wal.clone().streaming_read()?;

        while let Some(entry) = rx.next().await {
            match entry {
                SegmentEntry::DataEntry { data, .. } => {
                    // Decode the GC event
                    let gc: GcEvent = prost::Message::decode(data)
                        .map_err(|e| format!("Failed to decode GC event: {e}"))?;
                    let gc: GcEventEntry = gc.into();

                    // Update the oldest time map for the given keys
                    if let Some(keys) = gc.keys {
                        let key = keys.join(WAL_KEY_SEPERATOR);
                        oldest_time_map
                            .entry(key)
                            .and_modify(|dt| {
                                if gc.end_time > *dt {
                                    *dt = gc.end_time;
                                }
                            })
                            .or_insert(gc.end_time);
                    }
                }
                SegmentEntry::DataFooter { .. } => {
                    unimplemented!()
                }
                SegmentEntry::CmdFileSwitch { filename } => {
                    // Add the scanned file to the list
                    scanned_files.push(filename);
                }
            }
        }

        // Wait for the WAL reader to complete
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {e}"))??;

        Ok((oldest_time_map, scanned_files))
    }
}

/// Looks up `g`'s successor's create-time (unix micros) in a `sort_filenames`-ordered snapshot
/// of GC segments. Returns `None` if `g` cannot be located in the snapshot, or if `g` is the
/// last (newest) segment in the snapshot - in both cases the caller must retain `g`
fn successor_create_micros(sorted_gc_files: &[PathBuf], g: &Path) -> Option<u64> {
    let pos = sorted_gc_files.iter().position(|p| p == g)?;
    let successor = sorted_gc_files.get(pos + 1)?;
    let file_name = successor.file_name().and_then(|n| n.to_str())?;
    Some(parse_segment_create_micros(file_name))
}

/// ShouldRetain trait defines a method to determine whether a message should be retained.
trait ShouldRetain {
    /// Determines whether a message should be retained based on its event time in the message
    fn should_retain_message(&self, msg: &isb::Message) -> WalResult<bool>;
}

/// AlignedCompaction holds the oldest time below which all data has been processed. The event time
/// can be the same for all Keys.
struct AlignedCompaction(DateTime<Utc>);

/// UnalignedCompaction holds the oldest time below which all data has been processed. The event time
/// will be different for each Key.
struct UnalignedCompaction(HashMap<String, DateTime<Utc>>);

impl ShouldRetain for AlignedCompaction {
    /// Determines whether a message should be retained based on its event time.
    fn should_retain_message(&self, msg: &isb::Message) -> WalResult<bool> {
        // Extract the event time from the message
        let event_time = msg
            .header
            .as_ref()
            .and_then(|header| header.message_info.as_ref())
            .map(|info| {
                info.event_time
                    .map(utc_from_timestamp)
                    .expect("event time should be present")
            })
            .expect("Failed to extract event time from message");

        // Retain the message if its event time is greater than the oldest time
        // we should only compact the messages with event time strictly less than
        // the oldest time.
        Ok(event_time >= self.0)
    }
}

impl ShouldRetain for UnalignedCompaction {
    /// Determines whether an unaligned message should be retained based on its event time and keys.
    fn should_retain_message(&self, msg: &isb::Message) -> WalResult<bool> {
        // Extract the event time from the message
        let event_time = msg
            .header
            .as_ref()
            .and_then(|header| header.message_info.as_ref())
            .map(|info| {
                info.event_time
                    .map(utc_from_timestamp)
                    .expect("event time should be present")
            })
            .expect("Failed to extract event time from message");

        // Extract the keys from the message
        let keys = msg
            .header
            .as_ref()
            .map(|header| header.keys.clone())
            .unwrap_or_default();

        // Join the keys with the separator
        let key = keys.join(WAL_KEY_SEPERATOR);

        // Check if the key exists in the map
        if let Some(oldest_time) = self.0.get(&key) {
            // Retain the message if its event time is greater than the oldest time for this key
            // we should only compact the messages with event time strictly less than
            // the oldest time.
            return Ok(event_time >= *oldest_time);
        }

        // If the key is not in the map, retain the message
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use crate::reduce::wal::WalMessage;
    use crate::shared::grpc::prost_timestamp_from_utc;
    use bytes::Bytes;
    use chrono::TimeZone;
    use std::fs;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[tokio::test]
    async fn test_build_aligned_compaction() -> WalResult<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        // Create WAL directories
        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Create a compactor with default test values
        let compactor = Compactor::new(
            path.clone(),
            WindowKind::Aligned,
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create some test GC events with different timestamps
        let gc_events = vec![
            GcEvent {
                start_time: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            },
            GcEvent {
                start_time: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                ..Default::default()
            },
        ];

        // Create a WAL writer for GC events
        let gc_writer = AppendOnlyWal::new(
            WalType::Gc,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Set up streaming write for the GC WAL
        let (tx, rx) = mpsc::channel(100);
        let writer_handle = gc_writer.streaming_write(ReceiverStream::new(rx)).await?;

        // Write GC events to the WAL
        for event in gc_events {
            let mut buf = Vec::new();
            prost::Message::encode(&event, &mut buf)
                .map_err(|e| format!("Failed to encode GC event: {e}"))?;
            tx.send(SegmentWriteMessage::WriteGcEvent {
                data: Bytes::from(buf),
            })
            .await
            .map_err(|e| format!("Failed to send data: {e}"))?;
        }

        // Drop the sender to close the channel
        drop(tx);

        // Wait for the writer to complete
        writer_handle
            .await
            .map_err(|e| format!("Writer failed: {e}"))??;

        // Run build_aligned_compaction
        let (oldest_time, scanned_files) = compactor.build_aligned_compaction().await?;

        // Verify the results
        assert_eq!(oldest_time, Utc.timestamp_opt(2000, 0).unwrap());
        assert!(!scanned_files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_build_unaligned_compaction() -> WalResult<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        // Create WAL directories
        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Create a compactor with default test values
        let compactor = Compactor::new(
            path.clone(),
            WindowKind::Unaligned,
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create some test GC events with different timestamps and keys
        let gc_events = vec![
            GcEvent {
                start_time: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                keys: vec!["key1".to_string(), "key2".to_string()],
            },
            GcEvent {
                start_time: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                keys: vec!["key1".to_string(), "key2".to_string()],
            },
            GcEvent {
                start_time: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp {
                    seconds: 1500,
                    nanos: 0,
                }),
                keys: vec!["key3".to_string(), "key4".to_string()],
            },
        ];

        // Create a WAL writer for GC events
        let gc_writer = AppendOnlyWal::new(
            WalType::Gc,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Set up streaming write for the GC WAL
        let (tx, rx) = mpsc::channel(100);
        let writer_handle = gc_writer.streaming_write(ReceiverStream::new(rx)).await?;

        // Write GC events to the WAL
        for event in gc_events {
            let mut buf = Vec::new();
            prost::Message::encode(&event, &mut buf)
                .map_err(|e| format!("Failed to encode GC event: {e}"))?;
            tx.send(SegmentWriteMessage::WriteGcEvent {
                data: Bytes::from(buf),
            })
            .await
            .map_err(|e| format!("Failed to send data: {e}"))?;
        }

        // Drop the sender to close the channel
        drop(tx);

        // Wait for the writer to complete
        writer_handle
            .await
            .map_err(|e| format!("Writer failed: {e}"))??;

        // Run build_unaligned_compaction
        let (oldest_time_map, scanned_files) = compactor.build_unaligned_compaction().await?;

        // Verify the results
        assert!(!scanned_files.is_empty());
        assert_eq!(oldest_time_map.len(), 2); // We expect two different key combinations

        // Check the timestamps for each key combination
        let key1_2 = "key1:key2".to_string();
        let key3_4 = "key3:key4".to_string();

        assert_eq!(
            oldest_time_map.get(&key1_2),
            Some(&Utc.timestamp_opt(2000, 0).unwrap())
        );
        assert_eq!(
            oldest_time_map.get(&key3_4),
            Some(&Utc.timestamp_opt(1500, 0).unwrap())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_gc_wal_and_compaction_with_multiple_files() {
        let test_path = tempdir().unwrap().keep();

        // Create GC WAL
        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            test_path.clone(),
            1,    // 1MB
            1000, // 1s flush interval
            300,  // max_segment_age_secs
        )
        .await
        .unwrap();

        // Create and write 2 GC events
        let gc_start_1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 5).unwrap();
        let gc_end_1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 10).unwrap();
        let gc_event_1 = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start_1)),
            end_time: Some(prost_timestamp_from_utc(gc_end_1)),
            keys: vec![],
        };

        let gc_start_2 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 15).unwrap();
        let gc_end_2 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 20).unwrap();
        let gc_event_2 = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start_2)),
            end_time: Some(prost_timestamp_from_utc(gc_end_2)),
            keys: vec![],
        };

        let (tx, rx) = mpsc::channel(10);
        let handle = gc_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        // Write GC events as raw data
        tx.send(SegmentWriteMessage::WriteGcEvent {
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event_1)),
        })
        .await
        .unwrap();

        tx.send(SegmentWriteMessage::WriteGcEvent {
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event_2)),
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create segment WAL
        let segment_wal = AppendOnlyWal::new(
            WalType::Data,
            test_path.clone(),
            1,    // 1MB
            1000, // 1s flush interval
            300,  // max_segment_age_secs
        )
        .await
        .unwrap();

        // Write 1000 segment entries across 10 files
        let (tx, rx) = mpsc::channel(100);
        let handle = segment_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        let start_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let time_increment = chrono::Duration::seconds(1);

        for i in 1..=1000 {
            let message = Message {
                event_time: start_time + (time_increment * i),
                keys: Arc::from(vec!["test-key".to_string()]),
                value: bytes::Bytes::from(vec![1, 2, 3]),
                offset: Offset::Int(IntOffset::new(i as i64, 0)),
                id: MessageID {
                    vertex_name: "test-vertex".to_string().into(),
                    offset: i.to_string().into(),
                    index: 0,
                },
                ..Default::default()
            };

            // Send message - conversion to bytes happens internally
            tx.send(SegmentWriteMessage::WriteMessage {
                read_message: message.into(),
            })
            .await
            .unwrap();

            // Rotate every 100 messages to create 10 files
            if i % 100 == 0 {
                tx.send(SegmentWriteMessage::Rotate { on_size: false })
                    .await
                    .unwrap();
            }
        }
        drop(tx);
        handle.await.unwrap().unwrap();

        // Create and run compactor
        let compactor = Compactor::new(
            test_path.clone(),
            WindowKind::Aligned,
            1,    // 1MB
            1000, // 1s flush interval
            300,  // max_segment_age_secs
        )
        .await
        .unwrap();

        compactor.compact(None, true).await.unwrap();

        drop(compactor.compaction_ao_tx);
        compactor.writer_task_handle.await.unwrap().unwrap();

        // Verify compacted data
        let compaction_wal = ReplayWal::new(WalType::Compact, test_path);
        let (mut rx, handle) = compaction_wal.streaming_read().unwrap();
        let mut replayed_event_times = vec![];

        let mut remaining_message_count = 0;
        while let Some(entry) = rx.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let msg: isb::ReadMessage = prost::Message::decode(data).unwrap();
                if let Some(header) = msg.message.unwrap().header
                    && let Some(message_info) = header.message_info
                {
                    let event_time = message_info
                        .event_time
                        .map(utc_from_timestamp)
                        .expect("event time should not be empty");
                    assert!(
                        event_time >= gc_end_2,
                        "Found message with event_time < gc_end_2"
                    );
                    replayed_event_times.push(event_time);
                }
                remaining_message_count += 1;
            }
        }
        handle.await.unwrap().unwrap();

        // Verify the number of remaining messages
        // with event_time <= gc_end_2 should be removed
        assert_eq!(
            remaining_message_count, 981,
            "Expected 981 messages to remain after compaction"
        );

        // Verify that the event times are in increasing order
        for i in 1..replayed_event_times.len() {
            let prev = replayed_event_times
                .get(i - 1)
                .expect("Previous event time should exist");
            let curr = replayed_event_times
                .get(i)
                .expect("Current event time should exist");
            assert_eq!(
                *prev + chrono::Duration::seconds(1),
                *curr,
                "Event times are not in increasing order"
            );
        }
    }

    #[tokio::test]
    async fn test_compact_with_replay_aligned() -> WalResult<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        // Create WAL directories
        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Create GC WAL with test events
        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create a GC event with a specific end time
        let gc_start = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let gc_end = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 10).unwrap();
        let gc_event = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start)),
            end_time: Some(prost_timestamp_from_utc(gc_end)),
            ..Default::default()
        };

        // Write the GC event to the WAL
        let (tx, rx) = mpsc::channel(100);
        let writer_handle = gc_wal.streaming_write(ReceiverStream::new(rx)).await?;

        let mut buf = Vec::new();
        prost::Message::encode(&gc_event, &mut buf)
            .map_err(|e| format!("Failed to encode GC event: {e}"))?;
        tx.send(SegmentWriteMessage::WriteGcEvent {
            data: Bytes::from(buf),
        })
        .await
        .map_err(|e| format!("Failed to send data: {e}"))?;

        // Drop the sender to close the channel
        drop(tx);
        writer_handle
            .await
            .map_err(|e| format!("Writer failed: {e}"))??;

        // Create segment WAL with test messages
        let segment_wal = AppendOnlyWal::new(
            WalType::Data,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create messages with different event times
        let (tx, rx) = mpsc::channel(100);
        let writer_handle = segment_wal.streaming_write(ReceiverStream::new(rx)).await?;

        // Message with event time before the GC end time (should be filtered out)
        let before_time = Utc.with_ymd_and_hms(2025, 4, 1, 0, 59, 0).unwrap();
        let before_message = Message {
            event_time: before_time,
            keys: Arc::from(vec!["test-key".to_string()]),
            value: bytes::Bytes::from(vec![1, 2, 3]),
            offset: Offset::Int(IntOffset::new(1, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "1".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        // Message with event time after the GC end time (should be retained)
        let after_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 30, 0).unwrap();
        let after_message = Message {
            event_time: after_time,
            keys: Arc::from(vec!["test-key".to_string()]),
            value: bytes::Bytes::from(vec![4, 5, 6]),
            offset: Offset::Int(IntOffset::new(2, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "2".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        // Write the messages to the WAL - conversion to bytes happens internally
        tx.send(SegmentWriteMessage::WriteMessage {
            read_message: before_message.into(),
        })
        .await
        .map_err(|e| format!("Failed to send data: {e}"))?;

        tx.send(SegmentWriteMessage::WriteMessage {
            read_message: after_message.into(),
        })
        .await
        .map_err(|e| format!("Failed to send data: {e}"))?;

        // Drop the sender to close the channel
        drop(tx);
        writer_handle
            .await
            .map_err(|e| format!("Writer failed: {e}"))??;

        // Create a compactor with aligned window kind
        let compactor = Compactor::new(
            path.clone(),
            WindowKind::Aligned,
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create a channel to receive replayed messages
        let (replay_tx, mut replay_rx) = mpsc::channel(100);

        let cln_token = CancellationToken::new();

        // Call start_compaction with replay
        let handle = compactor
            .start_compaction_with_replay(replay_tx, Duration::from_secs(60), cln_token.clone())
            .await?;

        cln_token.cancel();
        // Wait for compaction to complete
        handle
            .await
            .map_err(|e| format!("Compaction failed: {e}"))??;

        // Count the number of messages received through the replay channel
        let mut replayed_count = 0;
        let mut replayed_event_times = Vec::new();

        while let Ok(data) = replay_rx.try_recv() {
            let msg: isb::ReadMessage = prost::Message::decode(data)
                .map_err(|e| format!("Failed to decode message: {e}"))?;

            // Verify that the message has an event time after the GC end time
            if let Some(header) = msg.message.unwrap().header
                && let Some(message_info) = header.message_info
            {
                let event_time = message_info.event_time.map(utc_from_timestamp).unwrap();
                assert!(
                    event_time > gc_end,
                    "Found message with event_time <= gc_end"
                );

                // Store the event time for later verification
                replayed_event_times.push(event_time);
            }
            replayed_count += 1;
        }

        // Verify that only one message was replayed (the one with event time after GC end time)
        assert_eq!(
            replayed_count, 1,
            "Expected only one message to be replayed"
        );

        // Verify that the event time is as expected
        assert_eq!(
            replayed_event_times.len(),
            1,
            "Expected one event time to be recorded"
        );

        assert_eq!(
            *replayed_event_times
                .first()
                .expect("Expected at least one event time"),
            after_time,
            "Event time of replayed message does not match the original message"
        );

        // check the order of the replayed event by checking if their event time is increasing by 1
        for i in 1..replayed_event_times.len() {
            let prev = replayed_event_times
                .get(i - 1)
                .expect("Previous event time should exist");
            let curr = replayed_event_times
                .get(i)
                .expect("Current event time should exist");
            assert_eq!(
                *prev + chrono::Duration::seconds(1),
                *curr,
                "Event times are not in increasing order"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_compact_with_replay_unaligned() -> WalResult<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        // Create WAL directories
        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Create GC WAL with test events
        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create GC events with different keys and end times
        let (tx, rx) = mpsc::channel(100);
        let writer_handle = gc_wal.streaming_write(ReceiverStream::new(rx)).await?;

        // GC event for key1:key2 with end time
        let gc_start_1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let gc_end_1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 10).unwrap();
        let gc_event_1 = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start_1)),
            end_time: Some(prost_timestamp_from_utc(gc_end_1)),
            keys: vec!["key1".to_string(), "key2".to_string()],
        };

        // GC event for key3:key4 with a different end time
        let gc_start_2 = Utc.with_ymd_and_hms(2025, 4, 1, 2, 0, 0).unwrap();
        let gc_end_2 = Utc.with_ymd_and_hms(2025, 4, 1, 2, 0, 0).unwrap();
        let gc_event_2 = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start_2)),
            end_time: Some(prost_timestamp_from_utc(gc_end_2)),
            keys: vec!["key3".to_string(), "key4".to_string()],
        };

        // Write the GC events to the WAL
        for event in [gc_event_1, gc_event_2] {
            let mut buf = Vec::new();
            prost::Message::encode(&event, &mut buf)
                .map_err(|e| format!("Failed to encode GC event: {e}"))?;
            tx.send(SegmentWriteMessage::WriteGcEvent {
                data: Bytes::from(buf),
            })
            .await
            .map_err(|e| format!("Failed to send data: {e}"))?;
        }

        // Drop the sender to close the channel
        drop(tx);
        writer_handle
            .await
            .map_err(|e| format!("Writer failed: {e}"))??;

        // Create segment WAL with test messages
        let segment_wal = AppendOnlyWal::new(
            WalType::Data,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create messages with different keys and event times
        let (tx, rx) = mpsc::channel(100);
        let writer_handle = segment_wal.streaming_write(ReceiverStream::new(rx)).await?;

        // Message 1: key1:key2 with event time before gc_end_1 (should be filtered out)
        let before_time_1 = Utc.with_ymd_and_hms(2025, 4, 1, 0, 59, 0).unwrap();
        let message_1 = Message {
            event_time: before_time_1,
            keys: Arc::from(vec!["key1".to_string(), "key2".to_string()]),
            value: bytes::Bytes::from(vec![1, 2, 3]),
            offset: Offset::Int(IntOffset::new(1, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "1".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        let wal_message_1: WalMessage = message_1.clone().into();

        // Message 2: key1:key2 with event time after gc_end_1 (should be retained)
        let after_time_1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 30, 0).unwrap();
        let message_2 = Message {
            event_time: after_time_1,
            keys: Arc::from(vec!["key1".to_string(), "key2".to_string()]),
            value: bytes::Bytes::from(vec![4, 5, 6]),
            offset: Offset::Int(IntOffset::new(2, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "2".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        let wal_message_2: WalMessage = message_2.clone().into();

        // Message 3: key3:key4 with event time before gc_end_2 (should be filtered out)
        let before_time_2 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 30, 0).unwrap();
        let message_3 = Message {
            event_time: before_time_2,
            keys: Arc::from(vec!["key3".to_string(), "key4".to_string()]),
            value: bytes::Bytes::from(vec![7, 8, 9]),
            offset: Offset::Int(IntOffset::new(3, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "3".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        let wal_message_3: WalMessage = message_3.clone().into();

        // Message 4: key3:key4 with event time after gc_end_2 (should be retained)
        let after_time_2 = Utc.with_ymd_and_hms(2025, 4, 1, 2, 30, 0).unwrap();
        let message_4 = Message {
            event_time: after_time_2,
            keys: Arc::from(vec!["key3".to_string(), "key4".to_string()]),
            value: bytes::Bytes::from(vec![10, 11, 12]),
            offset: Offset::Int(IntOffset::new(4, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "4".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        let wal_message_4: WalMessage = message_4.clone().into();

        // Write the messages to the WAL
        for (message, _wal_message) in [
            (message_1, wal_message_1),
            (message_2, wal_message_2),
            (message_3, wal_message_3),
            (message_4, wal_message_4),
        ]
        .iter()
        {
            // Send message - conversion to bytes happens internally
            tx.send(SegmentWriteMessage::WriteMessage {
                read_message: message.clone().into(),
            })
            .await
            .map_err(|e| format!("Failed to send data: {e}"))?;
        }

        // Drop the sender to close the channel
        drop(tx);
        writer_handle
            .await
            .map_err(|e| format!("Writer failed: {e}"))??;

        // Create a compactor with unaligned window kind
        let compactor = Compactor::new(
            path.clone(),
            WindowKind::Unaligned,
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            300,  // max_segment_age_secs
        )
        .await?;

        // Create a channel to receive replayed messages
        let (replay_tx, mut replay_rx) = mpsc::channel(100);

        let cln_token = CancellationToken::new();

        // Call start_compaction with replay
        let handle = compactor
            .start_compaction_with_replay(replay_tx, Duration::from_secs(60), cln_token.clone())
            .await?;

        cln_token.cancel();
        // Wait for compaction to complete
        handle
            .await
            .map_err(|e| format!("Compaction failed: {e}"))??;

        // Count the number of messages received through the replay channel
        let mut replayed_count = 0;
        let mut key1_key2_count = 0;
        let mut key3_key4_count = 0;

        while let Ok(data) = replay_rx.try_recv() {
            let msg: isb::ReadMessage = prost::Message::decode(data)
                .map_err(|e| format!("Failed to decode message: {e}"))?;

            // Verify that the message has an event time after the appropriate GC end time
            if let Some(header) = msg.message.unwrap().header {
                let key_str = header.keys.join(WAL_KEY_SEPERATOR);

                if key_str == "key1:key2" {
                    key1_key2_count += 1;
                    if let Some(message_info) = header.message_info.as_ref() {
                        let event_time = message_info.event_time.map(utc_from_timestamp).unwrap();
                        assert!(
                            event_time > gc_end_1,
                            "Found key1:key2 message with event_time <= gc_end_1"
                        );
                    }
                } else if key_str == "key3:key4" {
                    key3_key4_count += 1;
                    if let Some(message_info) = header.message_info.as_ref() {
                        let event_time = message_info.event_time.map(utc_from_timestamp).unwrap();
                        assert!(
                            event_time > gc_end_2,
                            "Found key3:key4 message with event_time <= gc_end_2"
                        );
                    }
                }
            }
            replayed_count += 1;
        }

        // Verify that only two messages were replayed (one for each key combination)
        assert_eq!(replayed_count, 2, "Expected two messages to be replayed");
        assert_eq!(key1_key2_count, 1, "Expected one key1:key2 message");
        assert_eq!(key3_key4_count, 1, "Expected one key3:key4 message");

        Ok(())
    }

    /// Counts files directly under `base_path` whose name starts with `prefix` and whose
    /// extension is exactly `extension` (e.g. `("gc", "frozen")`).
    fn count_segment_files(base_path: &std::path::Path, prefix: &str, extension: &str) -> usize {
        fs::read_dir(base_path)
            .expect("directory should exist")
            .map(|entry| entry.expect("dir entry should be valid").path())
            .filter(|path| path.is_file())
            .filter(|path| {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default();
                name.starts_with(prefix) && path.extension().is_some_and(|ext| ext == extension)
            })
            .count()
    }

    /// Regression test for the reduce WAL GC-segment-deletion gate.
    ///
    /// Simulates a retired key whose `GcEvent` is scanned in compaction cycle 1, but whose
    /// matching tail data segment only freezes in cycle 2 (data and GC WALs freeze
    /// independently). Before the fix, cycle 1 would unconditionally delete the GC segment, so
    /// by cycle 2 the key would be absent from `oldest_time_map` and the tail would be retained
    /// forever (the leak). After the fix, the GC segment is retained through cycle 1 (the gate
    /// does not fire yet, since no data segment has proven the tail was already scanned), so
    /// cycle 2 still has the key mapped and correctly drops the tail.
    #[tokio::test]
    async fn test_retention_gate_retains_gc_until_tail_data_freezes() -> WalResult<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        let retired_key = "retired-key".to_string();
        let event_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        // GC end time is after the tail's event time, so the tail should be dropped once matched.
        let gc_end = event_time + chrono::Duration::seconds(10);

        // Open the data WAL *before* the GC WAL and write the tail message first - matching the
        // real pipeline's causal order (data is ingested before its `GcEvent` is written, see
        // `reducer.rs:352`), which the successor-predicate's correctness proof relies on. Leave
        // the segment active (not frozen yet) - it only freezes in the next cycle.
        let data_wal = AppendOnlyWal::new(WalType::Data, path.clone(), 100, 1000, 300).await?;
        let (data_tx, data_rx) = mpsc::channel(10);
        let data_handle = data_wal
            .streaming_write(ReceiverStream::new(data_rx))
            .await?;

        let tail_message = Message {
            event_time,
            keys: Arc::from(vec![retired_key.clone()]),
            value: Bytes::from_static(b"tail"),
            offset: Offset::Int(IntOffset::new(1, 0)),
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "1".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        data_tx
            .send(SegmentWriteMessage::WriteMessage {
                read_message: tail_message.into(),
            })
            .await
            .map_err(|e| format!("Failed to send tail message: {e}"))?;

        // Give the writer a moment to persist the write before the GC WAL is created.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now write and freeze the GC segment for the retired key. Its successor (the next GC
        // segment, opened immediately by the rotate below) gets a create-time strictly after the
        // still-active data segment's create-time.
        let gc_wal = AppendOnlyWal::new(WalType::Gc, path.clone(), 100, 1000, 300).await?;
        let (gc_tx, gc_rx) = mpsc::channel(10);
        let gc_handle = gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;

        let gc_event = GcEvent {
            start_time: Some(prost_timestamp_from_utc(event_time)),
            end_time: Some(prost_timestamp_from_utc(gc_end)),
            keys: vec![retired_key.clone()],
        };
        gc_tx
            .send(SegmentWriteMessage::WriteGcEvent {
                data: Bytes::from(prost::Message::encode_to_vec(&gc_event)),
            })
            .await
            .map_err(|e| format!("Failed to send GC event: {e}"))?;
        gc_tx
            .send(SegmentWriteMessage::Rotate { on_size: false })
            .await
            .map_err(|e| format!("Failed to send rotate: {e}"))?;
        drop(gc_tx);
        gc_handle
            .await
            .map_err(|e| format!("GC writer failed: {e}"))??;

        let gc_dir = path.clone();
        assert_eq!(
            count_segment_files(&gc_dir, "gc", "frozen"),
            1,
            "expected exactly one frozen GC segment before compaction"
        );

        let compactor = Compactor::new(path.clone(), WindowKind::Unaligned, 100, 1000, 300).await?;

        // Cycle 1: the tail data segment is still active (not frozen), so the compactor doesn't
        // even see it yet. The GC segment must be retained: the gate must not fire since no data
        // segment on disk yet proves the tail was scanned with this GC segment present.
        compactor.compact(None, true).await?;
        assert_eq!(
            count_segment_files(&gc_dir, "gc", "frozen"),
            1,
            "GC segment must be retained after cycle 1 (tail data not yet frozen)"
        );

        // Now freeze the tail data segment - simulating the data WAL's independent freeze
        // schedule catching up in a later cycle.
        data_tx
            .send(SegmentWriteMessage::Rotate { on_size: false })
            .await
            .map_err(|e| format!("Failed to send rotate: {e}"))?;
        drop(data_tx);
        data_handle
            .await
            .map_err(|e| format!("Data writer failed: {e}"))??;

        // Cycle 2: the tail data segment is now frozen. Because the GC segment was retained
        // through cycle 1, `oldest_time_map` still has the retired key mapped, so the tail is
        // correctly dropped (not leaked into the compaction WAL).
        compactor.compact(None, true).await?;

        drop(compactor.compaction_ao_tx);
        compactor
            .writer_task_handle
            .await
            .expect("writer task should not panic")?;

        let compaction_wal = ReplayWal::new(WalType::Compact, path);
        let (mut rx, handle) = compaction_wal.streaming_read()?;
        let mut retained_count = 0;
        while let Some(entry) = rx.next().await {
            if matches!(entry, SegmentEntry::DataEntry { .. }) {
                retained_count += 1;
            }
        }
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {e}"))??;

        assert_eq!(
            retained_count, 0,
            "tail data for the retired key must be dropped, not leaked, once its segment freezes"
        );

        Ok(())
    }

    /// Hand-crafts a raw WAL segment file containing the given length-prefixed entries. Mirrors
    /// the on-disk wire format `AppendOnlyWal` writes and `ReplayWal::read_segment` reads
    /// (`<u64_le data_len><data_len bytes>`, repeated). Used so tests can control a segment's
    /// `create_micros` (embedded in its filename) independent of wall-clock time.
    fn write_raw_segment_file(path: &Path, entries: &[Vec<u8>]) {
        use std::io::Write;
        let mut file = fs::File::create(path).expect("create raw segment file");
        for entry in entries {
            file.write_all(&(entry.len() as u64).to_le_bytes())
                .expect("write entry length prefix");
            file.write_all(entry).expect("write entry data");
        }
    }

    /// Encodes a minimal `isb::ReadMessage` matching the wire format `AppendOnlyWal` produces for
    /// `SegmentWriteMessage::WriteMessage` (see `crate::reduce::wal::WalMessage`'s `TryFrom`
    /// impl) - just enough fields for `UnalignedCompaction::should_retain_message` to evaluate.
    fn encode_tail_message(event_time: DateTime<Utc>, keys: Vec<String>) -> Vec<u8> {
        let msg = isb::ReadMessage {
            message: Some(isb::Message {
                header: Some(isb::Header {
                    message_info: Some(isb::MessageInfo {
                        event_time: Some(prost_timestamp_from_utc(event_time)),
                        is_late: false,
                    }),
                    kind: 0,
                    id: Some(isb::MessageId {
                        vertex_name: "test-vertex".to_string(),
                        offset: "1".to_string(),
                        index: 0,
                    }),
                    keys,
                    headers: Default::default(),
                    metadata: None,
                }),
                body: Some(isb::Body {
                    payload: b"sparse-tail".to_vec(),
                }),
            }),
            read_offset: 1,
            watermark: None,
            metadata: None,
        };
        prost::Message::encode_to_vec(&msg)
    }

    /// **Required regression test (the reviewer's blocking finding).** Discriminates the
    /// successor-based predicate from the original (unsound) `create(g) + max_segment_age`
    /// predicate for a sparse-GC / stale-empty-segment workload.
    ///
    /// Crafts the on-disk layout directly: a frozen GC segment `g` whose *own* `create_micros`
    /// is stale (as if it sat open+empty well past any realistic `max_segment_age` before
    /// finally receiving its one `GcEvent` - an empty segment never age-rotates, see
    /// `append.rs:198,271-273`), whose successor (the next GC segment) has a much later
    /// create-time `S` (= `g`'s true freeze-time). Key K's tail data (event time before K's GC
    /// end-time, i.e. it should be dropped once matched) starts out in an *active* data segment
    /// created before `S`.
    ///
    /// Cycle 1 (`newest_data(=T0) < S`, but `T0 - T_STALE` is deliberately **larger** than this
    /// test's own `max_segment_age_secs = 300`): the new successor predicate correctly
    /// **retains** `g` here (`T0 < S`), but the old `create(g)+A` predicate would have already
    /// fired at this very cycle (`T0 > T_STALE + A`, since the stale gap exceeds `A`) and
    /// deleted `g` prematurely - exactly the bug the reviewer flagged. This is what makes the
    /// `count == 1` assertion below discriminate the two predicates at cycle 1, not via a
    /// boundary/equality fluke.
    /// Cycle 2 (after freezing the tail's data segment and opening a new one with create `> S`):
    /// `g` is now safe to delete under the successor predicate, and the tail is correctly
    /// **dropped** (matched via `g`, which survived into this cycle) rather than leaked into the
    /// compaction WAL.
    #[tokio::test]
    async fn test_retention_gate_sparse_gc_uses_successor_not_own_create_time() -> WalResult<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Fully synthetic (unix-micros) timestamps, chosen only relative to one another - the
        // fix's predicate never compares them to wall-clock "now". `Compactor::new` below is
        // constructed with `max_segment_age_secs = 300` (300_000_000 micros): `T0 - T_STALE` is
        // deliberately > that, so the OLD `create(g)+A` predicate would already consider `g`
        // deletable at cycle 1 (`T0 > T_STALE + 300_000_000`), while the NEW successor predicate
        // correctly retains it (`T0 < S`) - this is what discriminates the two at cycle 1.
        const T_STALE: u64 = 1_000_000; // g's own (stale, untrustworthy) create time
        const T0: u64 = T_STALE + 400_000_000; // data segment opens 400s later (> the 300s max_segment_age)
        const S: u64 = T0 + 50_000_000; // g's successor's create time = g's true freeze time (> T0)
        const T1: u64 = S + 50_000_000; // new data segment opens after g freezes (> S)

        let retired_key = "sparse-retired-key".to_string();
        let event_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        // GC end time is after the tail's event time, so the tail should be dropped once matched.
        let gc_end = event_time + chrono::Duration::seconds(10);

        // `g`: a frozen GC segment with a single retiring `GcEvent`, stamped with the stale
        // create time.
        let gc_event = GcEvent {
            start_time: Some(prost_timestamp_from_utc(event_time)),
            end_time: Some(prost_timestamp_from_utc(gc_end)),
            keys: vec![retired_key.clone()],
        };
        write_raw_segment_file(
            &path.join(format!("gc_0_{T_STALE}.frozen")),
            &[prost::Message::encode_to_vec(&gc_event)],
        );

        // `g`'s successor: the next GC segment, still active - only its create-time matters.
        write_raw_segment_file(&path.join(format!("gc_1_{S}.wal")), &[]);

        // The tail: an active (not yet frozen) data segment containing the one message that
        // should be dropped once matched against `g`.
        write_raw_segment_file(
            &path.join(format!("data_0_{T0}.wal")),
            &[encode_tail_message(event_time, vec![retired_key.clone()])],
        );

        let compactor = Compactor::new(path.clone(), WindowKind::Unaligned, 100, 1000, 300).await?;

        // Cycle 1: `newest_data_micros` (= T0) < S ⇒ the gate must not fire; `g` is retained.
        compactor.compact(None, true).await?;
        assert_eq!(
            count_segment_files(&path, "gc", "frozen"),
            1,
            "stale-but-not-yet-safe GC segment must be retained (old create(g)+A predicate would have deleted it here)"
        );

        // Freeze the tail's data segment (rename, preserving T0) and open a new active data
        // segment with a create-time strictly after `S`.
        tokio::fs::rename(
            path.join(format!("data_0_{T0}.wal")),
            path.join(format!("data_0_{T0}.frozen")),
        )
        .await?;
        write_raw_segment_file(&path.join(format!("data_1_{T1}.wal")), &[]);

        // Cycle 2: `newest_data_micros` (= T1) > S ⇒ the tail (now frozen) is scanned and
        // correctly dropped (matched via `g`, still present in `oldest_time_map`), and only then
        // is `g` finally safe to delete.
        compactor.compact(None, true).await?;
        assert_eq!(
            count_segment_files(&path, "gc", "frozen"),
            0,
            "GC segment should be reclaimed once a data segment newer than its successor exists"
        );

        drop(compactor.compaction_ao_tx);
        compactor
            .writer_task_handle
            .await
            .expect("writer task should not panic")?;

        let compaction_wal = ReplayWal::new(WalType::Compact, path);
        let (mut rx, handle) = compaction_wal.streaming_read()?;
        let mut retained_count = 0;
        while let Some(entry) = rx.next().await {
            if matches!(entry, SegmentEntry::DataEntry { .. }) {
                retained_count += 1;
            }
        }
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {e}"))??;

        assert_eq!(
            retained_count, 0,
            "the tail must be dropped, not leaked, into the compaction WAL"
        );

        Ok(())
    }

    /// Bounded-retention test: churns many keys across several "rounds". Each round writes
    /// several retired keys' `GcEvent`s (each frozen into its own GC segment immediately) while
    /// their tail data accumulates in a single still-active data segment; only at the end of the
    /// round does the data segment freeze and get compacted. Asserts that the number of retained
    /// (not-yet-deletable) GC segments plateaus at the round size instead of growing with the
    /// total number of churned keys, and that no tail data ever leaks into the compaction WAL.
    #[tokio::test]
    async fn test_retention_gate_bounded_retention_under_key_churn() -> WalResult<()> {
        const ROUNDS: usize = 4;
        const KEYS_PER_ROUND: usize = 5;

        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Open the data WAL (and its initial active segment) *before* any GC segment is created,
        // so the initial active data segment is always older than the first round's GC segments
        // - otherwise the gate could fire on round 0's GC segments as soon as they freeze, before
        // any round has had a chance to flush its data.
        let data_wal = AppendOnlyWal::new(WalType::Data, path.clone(), 100, 1000, 300).await?;
        let (data_tx, data_rx) = mpsc::channel(100);
        let data_handle = data_wal
            .streaming_write(ReceiverStream::new(data_rx))
            .await?;

        // max_segment_age=0 means the gate fires as soon as ANY strictly newer data segment
        // exists on disk, which lets this test run fast while still exercising the real gate.
        let compactor = Compactor::new(path.clone(), WindowKind::Unaligned, 100, 1000, 0).await?;

        let base_time = Utc.with_ymd_and_hms(2025, 4, 1, 0, 0, 0).unwrap();
        let gc_dir = path.clone();

        for round in 0..ROUNDS {
            for k in 0..KEYS_PER_ROUND {
                let key_idx = round * KEYS_PER_ROUND + k;
                let key = format!("churn-key-{key_idx}");
                let event_time = base_time + chrono::Duration::seconds(key_idx as i64);
                let gc_end = event_time + chrono::Duration::seconds(1);

                // Write the tail message into the round's still-active data segment.
                let message = Message {
                    event_time,
                    keys: Arc::from(vec![key.clone()]),
                    value: Bytes::from_static(b"churn"),
                    offset: Offset::Int(IntOffset::new(key_idx as i64 + 1, 0)),
                    id: MessageID {
                        vertex_name: "test-vertex".to_string().into(),
                        offset: key_idx.to_string().into(),
                        index: 0,
                    },
                    ..Default::default()
                };
                data_tx
                    .send(SegmentWriteMessage::WriteMessage {
                        read_message: message.into(),
                    })
                    .await
                    .map_err(|e| format!("Failed to send message: {e}"))?;

                // Write and freeze this key's own GC segment. Each key gets a dedicated,
                // freshly-opened WAL (fully awaited before moving on) rather than sharing one
                // long-lived GC WAL across keys: reusing one would leave an empty active segment
                // (opened by the previous key's rotate) idle across round boundaries, and its
                // stale creation time would make it eligible for the gate the instant it is
                // finally written to and frozen - an artifact of how this test drives the WAL,
                // not something the fix needs to tolerate (see `append.rs::write_data`'s
                // size-gated age check, which the design's retention-gate proof assumes is
                // always what keeps a GC segment's create-time close to its events).
                let gc_wal = AppendOnlyWal::new(WalType::Gc, path.clone(), 100, 1000, 300).await?;
                let (gc_tx, gc_rx) = mpsc::channel(1);
                let gc_handle = gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;

                let gc_event = GcEvent {
                    start_time: Some(prost_timestamp_from_utc(event_time)),
                    end_time: Some(prost_timestamp_from_utc(gc_end)),
                    keys: vec![key.clone()],
                };
                gc_tx
                    .send(SegmentWriteMessage::WriteGcEvent {
                        data: Bytes::from(prost::Message::encode_to_vec(&gc_event)),
                    })
                    .await
                    .map_err(|e| format!("Failed to send GC event: {e}"))?;
                gc_tx
                    .send(SegmentWriteMessage::Rotate { on_size: false })
                    .await
                    .map_err(|e| format!("Failed to send rotate: {e}"))?;
                drop(gc_tx);
                gc_handle
                    .await
                    .map_err(|e| format!("GC writer failed: {e}"))??;

                // Per-key compaction cycle: the round's data segment is still active, so nothing
                // yet proves it is safe to reclaim this key's GC segment.
                compactor.compact(None, true).await?;
            }

            // Peak: right before the round's flush, every retired key from this round must still
            // have its GC segment retained (none reclaimed prematurely).
            let peak = count_segment_files(&gc_dir, "gc", "frozen");
            assert_eq!(
                peak, KEYS_PER_ROUND,
                "round {round}: expected {KEYS_PER_ROUND} retained GC segments before flush, got {peak}"
            );

            // Flush: freeze the round's data segment and run one more compaction cycle. This
            // proves the round's tail data has been scanned (and, since each message matches a
            // retired key, dropped), so the round's GC segments can finally be reclaimed.
            data_tx
                .send(SegmentWriteMessage::Rotate { on_size: false })
                .await
                .map_err(|e| format!("Failed to send rotate: {e}"))?;
            tokio::time::sleep(Duration::from_millis(30)).await;
            compactor.compact(None, true).await?;

            let after_flush = count_segment_files(&gc_dir, "gc", "frozen");
            assert_eq!(
                after_flush, 0,
                "round {round}: GC segments should be fully reclaimed after flush, got {after_flush}"
            );
        }

        drop(data_tx);
        data_handle
            .await
            .map_err(|e| format!("Data writer failed: {e}"))??;

        drop(compactor.compaction_ao_tx);
        compactor
            .writer_task_handle
            .await
            .expect("writer task should not panic")?;

        // No tail data ever leaked into the compaction WAL, regardless of the total number of
        // churned keys (ROUNDS * KEYS_PER_ROUND).
        let compaction_wal = ReplayWal::new(WalType::Compact, path);
        let (mut rx, handle) = compaction_wal.streaming_read()?;
        let mut retained_count = 0;
        while let Some(entry) = rx.next().await {
            if matches!(entry, SegmentEntry::DataEntry { .. }) {
                retained_count += 1;
            }
        }
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {e}"))??;

        assert_eq!(
            retained_count, 0,
            "no churned key's tail data should leak into the compaction WAL"
        );

        Ok(())
    }

    /// The boot compaction (`compact(Some(replay_tx), false)`) must never apply the retention
    /// gate: even when a strictly newer data segment already exists on disk (a condition that
    /// would satisfy the gate on a periodic cycle), boot must retain every GC segment.
    #[tokio::test]
    async fn test_boot_compaction_retains_all_gc_segments() -> WalResult<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().to_path_buf();

        fs::create_dir_all(path.join(WalType::Gc.to_string()))?;
        fs::create_dir_all(path.join(WalType::Data.to_string()))?;
        fs::create_dir_all(path.join(WalType::Compact.to_string()))?;

        // Write and freeze a GC segment.
        let gc_wal = AppendOnlyWal::new(WalType::Gc, path.clone(), 100, 1000, 300).await?;
        let (gc_tx, gc_rx) = mpsc::channel(10);
        let gc_handle = gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;

        let gc_event = GcEvent {
            start_time: Some(prost_timestamp_from_utc(Utc::now())),
            end_time: Some(prost_timestamp_from_utc(Utc::now())),
            ..Default::default()
        };
        gc_tx
            .send(SegmentWriteMessage::WriteGcEvent {
                data: Bytes::from(prost::Message::encode_to_vec(&gc_event)),
            })
            .await
            .map_err(|e| format!("Failed to send GC event: {e}"))?;
        gc_tx
            .send(SegmentWriteMessage::Rotate { on_size: false })
            .await
            .map_err(|e| format!("Failed to send rotate: {e}"))?;
        drop(gc_tx);
        gc_handle
            .await
            .map_err(|e| format!("GC writer failed: {e}"))??;

        let gc_dir = path.clone();
        assert_eq!(
            count_segment_files(&gc_dir, "gc", "frozen"),
            1,
            "expected one frozen GC segment"
        );

        // Open a data WAL *after* the GC segment has been frozen, so its active segment is
        // strictly newer than the GC segment - the condition that would satisfy the retention
        // gate on a periodic cycle.
        let data_wal = AppendOnlyWal::new(WalType::Data, path.clone(), 100, 1000, 300).await?;
        let (data_tx, data_rx) = mpsc::channel(10);
        let _data_handle = data_wal
            .streaming_write(ReceiverStream::new(data_rx))
            .await?;

        // max_segment_age=0 so, on a periodic cycle, the newer active data segment alone would
        // be enough to satisfy the gate.
        let compactor = Compactor::new(path.clone(), WindowKind::Aligned, 100, 1000, 0).await?;

        let (replay_tx, _replay_rx) = mpsc::channel(10);

        // Boot compaction: must retain the GC segment no matter what.
        compactor.compact(Some(replay_tx), false).await?;
        assert_eq!(
            count_segment_files(&gc_dir, "gc", "frozen"),
            1,
            "boot compaction must never delete GC segments, even with a newer data segment present"
        );

        // Sanity check: a periodic cycle against the same on-disk state *does* reclaim it,
        // proving the gate is otherwise armed and it is specifically the boot path suppressing
        // it above.
        compactor.compact(None, true).await?;
        assert_eq!(
            count_segment_files(&gc_dir, "gc", "frozen"),
            0,
            "periodic compaction should reclaim the GC segment once it is safe"
        );

        drop(data_tx);
        drop(compactor.compaction_ao_tx);
        compactor
            .writer_task_handle
            .await
            .expect("writer task should not panic")?;

        Ok(())
    }
}
