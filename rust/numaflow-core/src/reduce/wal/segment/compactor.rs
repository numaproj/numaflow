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
use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
use crate::shared::grpc::utc_from_timestamp;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::isb;
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
use std::path::PathBuf;
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
            // First do a one-time compaction with replay
            self.compact(Some(replay_tx)).await?;

            // Then start periodic compaction
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        self.compact(None).await?;
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
    async fn compact(&self, replay_tx: Option<Sender<Bytes>>) -> WalResult<()> {
        match self.kind {
            WindowKind::Aligned => self.compact_aligned(replay_tx.clone()).await?,
            WindowKind::Unaligned => self.compact_unaligned(replay_tx).await?,
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
    /// - Delete the Segment File after the Rotate is complete.
    async fn compact_aligned(&self, replay_tx: Option<mpsc::Sender<Bytes>>) -> WalResult<()> {
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

        // Delete the GC files
        for gc_file in gc_files {
            debug!(gc_file = %gc_file.display(), "removing segment file");
            tokio::fs::remove_file(gc_file).await?;
        }

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
    /// - Delete the Segment File after the Rotate is complete.
    async fn compact_unaligned(&self, replay_tx: Option<mpsc::Sender<Bytes>>) -> WalResult<()> {
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

        // Delete the GC files
        for gc_file in gc_files {
            info!(gc_file = %gc_file.display(), "removing segment file");
            tokio::fs::remove_file(gc_file).await?;
        }

        Ok(())
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
            let mut message = Message::default();
            message.event_time = start_time + (time_increment * i);
            message.keys = Arc::from(vec!["test-key".to_string()]);
            message.value = bytes::Bytes::from(vec![1, 2, 3]);
            message.offset = Offset::Int(IntOffset::new(i as i64, 0));
            message.id = MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: i.to_string().into(),
                index: 0,
            };

            // Send message - conversion to bytes happens internally
            tx.send(SegmentWriteMessage::WriteMessage { message })
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

        compactor.compact(None).await.unwrap();

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
            assert_eq!(
                replayed_event_times[i - 1] + chrono::Duration::seconds(1),
                replayed_event_times[i],
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
        let mut before_message = Message::default();
        before_message.event_time = before_time;
        before_message.keys = Arc::from(vec!["test-key".to_string()]);
        before_message.value = bytes::Bytes::from(vec![1, 2, 3]);
        before_message.offset = Offset::Int(IntOffset::new(1, 0));
        before_message.id = MessageID {
            vertex_name: "test-vertex".to_string().into(),
            offset: "1".to_string().into(),
            index: 0,
        };

        // Message with event time after the GC end time (should be retained)
        let after_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 30, 0).unwrap();
        let mut after_message = Message::default();
        after_message.event_time = after_time;
        after_message.keys = Arc::from(vec!["test-key".to_string()]);
        after_message.value = bytes::Bytes::from(vec![4, 5, 6]);
        after_message.offset = Offset::Int(IntOffset::new(2, 0));
        after_message.id = MessageID {
            vertex_name: "test-vertex".to_string().into(),
            offset: "2".to_string().into(),
            index: 0,
        };

        // Write the messages to the WAL - conversion to bytes happens internally
        tx.send(SegmentWriteMessage::WriteMessage {
            message: before_message,
        })
        .await
        .map_err(|e| format!("Failed to send data: {e}"))?;

        tx.send(SegmentWriteMessage::WriteMessage {
            message: after_message,
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
            replayed_event_times[0], after_time,
            "Event time of replayed message does not match the original message"
        );

        // check the order of the replayed event by checking if their event time is increasing by 1
        for i in 1..replayed_event_times.len() {
            assert_eq!(
                replayed_event_times[i - 1] + chrono::Duration::seconds(1),
                replayed_event_times[i],
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
        let mut message_1 = Message::default();
        message_1.event_time = before_time_1;
        message_1.keys = Arc::from(vec!["key1".to_string(), "key2".to_string()]);
        message_1.value = bytes::Bytes::from(vec![1, 2, 3]);
        message_1.offset = Offset::Int(IntOffset::new(1, 0));
        message_1.id = MessageID {
            vertex_name: "test-vertex".to_string().into(),
            offset: "1".to_string().into(),
            index: 0,
        };
        let wal_message_1: WalMessage = message_1.clone().into();

        // Message 2: key1:key2 with event time after gc_end_1 (should be retained)
        let after_time_1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 30, 0).unwrap();
        let mut message_2 = Message::default();
        message_2.event_time = after_time_1;
        message_2.keys = Arc::from(vec!["key1".to_string(), "key2".to_string()]);
        message_2.value = bytes::Bytes::from(vec![4, 5, 6]);
        message_2.offset = Offset::Int(IntOffset::new(2, 0));
        message_2.id = MessageID {
            vertex_name: "test-vertex".to_string().into(),
            offset: "2".to_string().into(),
            index: 0,
        };
        let wal_message_2: WalMessage = message_2.clone().into();

        // Message 3: key3:key4 with event time before gc_end_2 (should be filtered out)
        let before_time_2 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 30, 0).unwrap();
        let mut message_3 = Message::default();
        message_3.event_time = before_time_2;
        message_3.keys = Arc::from(vec!["key3".to_string(), "key4".to_string()]);
        message_3.value = bytes::Bytes::from(vec![7, 8, 9]);
        message_3.offset = Offset::Int(IntOffset::new(3, 0));
        message_3.id = MessageID {
            vertex_name: "test-vertex".to_string().into(),
            offset: "3".to_string().into(),
            index: 0,
        };
        let wal_message_3: WalMessage = message_3.clone().into();

        // Message 4: key3:key4 with event time after gc_end_2 (should be retained)
        let after_time_2 = Utc.with_ymd_and_hms(2025, 4, 1, 2, 30, 0).unwrap();
        let mut message_4 = Message::default();
        message_4.event_time = after_time_2;
        message_4.keys = Arc::from(vec!["key3".to_string(), "key4".to_string()]);
        message_4.value = bytes::Bytes::from(vec![10, 11, 12]);
        message_4.offset = Offset::Int(IntOffset::new(4, 0));
        message_4.id = MessageID {
            vertex_name: "test-vertex".to_string().into(),
            offset: "4".to_string().into(),
            index: 0,
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
                message: message.clone(),
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
}
