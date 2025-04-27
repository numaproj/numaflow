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
//!

use crate::reduce::wal::error::WalResult;
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
use crate::reduce::wal::segment::GcEventEntry;
use crate::reduce::wal::segment::WalType;
use crate::shared::grpc::utc_from_timestamp;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::isb;
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    /// GC WAL
    gc_wal: ReplayWal,
    /// Segment WAL
    segment_wal: ReplayWal,
    /// Compaction WAL for Read Only
    compaction_ro_wal: ReplayWal,
    /// New Compaction WAL for writing compacted data
    compaction_ao_wal: AppendOnlyWal,
    /// Kind of Window, Aligned or Unaligned
    kind: WindowKind,
}

const WAL_KEY_SEPERATOR: &'static str = ":";

impl Compactor {
    /// Creates a new Compactor.
    pub(crate) async fn new(
        path: PathBuf,
        kind: WindowKind,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        channel_buffer_size: usize,
    ) -> WalResult<Self> {
        let segment_wal = ReplayWal::new(WalType::Data, path.clone());
        let gc_wal = ReplayWal::new(WalType::Gc, path.clone());
        let compaction_ro_wal = ReplayWal::new(WalType::Compact, path.clone());
        let compaction_ao_wal = AppendOnlyWal::new(
            WalType::Compact,
            path.clone(),
            max_file_size_mb,
            flush_interval_ms,
            channel_buffer_size,
        )
        .await?;

        Ok(Self {
            gc_wal,
            segment_wal,
            compaction_ro_wal,
            compaction_ao_wal,
            kind,
        })
    }

    /// Starts the compaction process every `interval_ms` milliseconds.
    pub(crate) async fn start_compaction(
        self,
        duration: Duration,
        cln_token: CancellationToken,
    ) -> WalResult<JoinHandle<WalResult<()>>> {
        let mut tick = tokio::time::interval(duration);
        let result: JoinHandle<WalResult<()>> = tokio::spawn(async move {
            loop {
                tokio::select! {
                _ = tick.tick() => {
                        self.compact().await?;
                     }
                _ = cln_token.cancelled() => {
                        break;
                    }
                }
            }
            Ok(())
        });
        Ok(result)
    }

    /// Compact first needs to get all the GC files and build a compaction map. This map will have
    /// the oldest data before which all can be deleted.
    pub(crate) async fn compact(&self) -> WalResult<()> {
        match self.kind {
            WindowKind::Aligned => self.compact_aligned().await?,
            WindowKind::Unaligned => self.compact_unaligned().await?,
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
    async fn compact_aligned(&self) -> WalResult<()> {
        // Get the oldest time and scanned GC files
        let (oldest_time, gc_files) = self.build_aligned_compaction().await?;

        let compact = AlignedCompaction(oldest_time);

        // Compact the compaction_ro_wal
        self.process_wal_stream(&self.compaction_ro_wal, &compact)
            .await?;

        // Compact the segment_wal
        self.process_wal_stream(&self.segment_wal, &compact).await?;

        // Delete the GC files
        for gc_file in gc_files {
            info!(gc_file = %gc_file.display(), "removing segment file");
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
    async fn compact_unaligned(&self) -> WalResult<()> {
        // Get the oldest time map and scanned GC files
        let (oldest_time_map, gc_files) = self.build_unaligned_compaction().await?;

        let compact = UnalignedCompaction(oldest_time_map);

        // Compact the compaction_ro_wal
        self.process_wal_stream(&self.compaction_ro_wal, &compact)
            .await?;

        // Compact the segment_wal
        self.process_wal_stream(&self.segment_wal, &compact).await?;

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
    ) -> WalResult<()> {
        // Get a streaming reader for the WAL
        let (mut rx, handle) = wal.clone().streaming_read()?;

        let (wal_tx, wal_rx) = mpsc::channel(100);
        let (_result_rx, writer_handle) = self
            .compaction_ao_wal
            .clone()
            .streaming_write(ReceiverStream::new(wal_rx))
            .await
            .map_err(|e| format!("Failed to start compaction WAL writer: {}", e))?;

        // Process each WAL entry
        while let Some(entry) = rx.next().await {
            match entry {
                SegmentEntry::DataEntry { data, .. } => {
                    // Deserialize the message
                    let msg: isb::Message = prost::Message::decode(data.clone())
                        .map_err(|e| format!("Failed to decode message: {}", e))?;

                    if should_retain.should_retain_message(&msg)? {
                        // Send the message to the compaction WAL
                        wal_tx
                            .send(SegmentWriteMessage::WriteData { id: None, data })
                            .await
                            .map_err(|e| {
                                format!("Failed to send message to compaction WAL: {}", e)
                            })?;
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
                        .map_err(|e| format!("Failed to send rotate command: {}", e))?;

                    // Delete the processed segment file
                    tokio::fs::remove_file(&filename).await.map_err(|e| {
                        format!(
                            "Failed to delete segment file {}: {}",
                            filename.display(),
                            e
                        )
                    })?;

                    info!(filename = %filename.display(), "removing segment file");
                }
            }
        }

        // Wait for the WAL reader to complete
        handle
            .await
            .map_err(|e| format!("WAL reader failed: {}", e))??;

        // Drop the sender to close the channel
        drop(wal_tx);

        // Wait for the writer to complete
        writer_handle
            .await
            .map_err(|e| format!("Compaction writer failed: {}", e))??;

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
            .map(|info| utc_from_timestamp(info.event_time))
            .expect("Failed to extract event time from message");

        // Retain the message if its event time is greater than the oldest time
        Ok(event_time > self.0)
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
            .map(|info| utc_from_timestamp(info.event_time))
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
            return Ok(event_time > *oldest_time);
        }

        // If the key is not in the map, retain the message
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, MessageID};
    use crate::shared::grpc::prost_timestamp_from_utc;
    use bytes::Bytes;
    use chrono::TimeZone;
    use prost_types;
    use std::fs;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[tokio::test]
    async fn test_build_aligned_compaction() -> WalResult<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create WAL directories
        fs::create_dir_all(path.join(WalType::Gc.to_string())).unwrap();
        fs::create_dir_all(path.join(WalType::Data.to_string())).unwrap();
        fs::create_dir_all(path.join(WalType::Compact.to_string())).unwrap();

        // Create a compactor with default test values
        let compactor = Compactor::new(
            path.clone(),
            WindowKind::Aligned,
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            1000, // channel_buffer_size
        )
        .await?;

        // Create some test GC events with different timestamps
        let gc_events = vec![
            GcEvent {
                end_time: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            },
            GcEvent {
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
            100,  // channel_buffer_size
        )
        .await?;

        // Set up streaming write for the GC WAL
        let (tx, rx) = mpsc::channel(100);
        let (_result_rx, writer_handle) =
            gc_writer.streaming_write(ReceiverStream::new(rx)).await?;

        // Write GC events to the WAL
        for event in gc_events {
            let mut buf = Vec::new();
            prost::Message::encode(&event, &mut buf)
                .map_err(|e| format!("Failed to encode GC event: {e}"))?;
            tx.send(SegmentWriteMessage::WriteData {
                id: None,
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
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create WAL directories
        fs::create_dir_all(path.join(WalType::Gc.to_string())).unwrap();
        fs::create_dir_all(path.join(WalType::Data.to_string())).unwrap();
        fs::create_dir_all(path.join(WalType::Compact.to_string())).unwrap();

        // Create a compactor with default test values
        let compactor = Compactor::new(
            path.clone(),
            WindowKind::Unaligned,
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            1000, // channel_buffer_size
        )
        .await?;

        // Create some test GC events with different timestamps and keys
        let gc_events = vec![
            GcEvent {
                end_time: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                keys: vec!["key1".to_string(), "key2".to_string()],
                ..Default::default()
            },
            GcEvent {
                end_time: Some(prost_types::Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                keys: vec!["key1".to_string(), "key2".to_string()],
                ..Default::default()
            },
            GcEvent {
                end_time: Some(prost_types::Timestamp {
                    seconds: 1500,
                    nanos: 0,
                }),
                keys: vec!["key3".to_string(), "key4".to_string()],
                ..Default::default()
            },
        ];

        // Create a WAL writer for GC events
        let gc_writer = AppendOnlyWal::new(
            WalType::Gc,
            path.clone(),
            100,  // max_file_size_mb
            1000, // flush_interval_ms
            100,  // channel_buffer_size
        )
        .await?;

        // Set up streaming write for the GC WAL
        let (tx, rx) = mpsc::channel(100);
        let (_result_rx, writer_handle) =
            gc_writer.streaming_write(ReceiverStream::new(rx)).await?;

        // Write GC events to the WAL
        for event in gc_events {
            let mut buf = Vec::new();
            prost::Message::encode(&event, &mut buf)
                .map_err(|e| format!("Failed to encode GC event: {e}"))?;
            tx.send(SegmentWriteMessage::WriteData {
                id: None,
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

    // FIXME
    #[tokio::test]
    async fn test_gc_wal_and_compaction_with_multiple_files() {
        let test_path = tempfile::tempdir().unwrap().into_path();

        // Create GC WAL
        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            test_path.clone(),
            1,    // 1MB
            1000, // 1s flush interval
            500,  // channel buffer
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let (_offset_stream, handle) = gc_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        tx.send(SegmentWriteMessage::WriteData {
            id: Some("gc1".to_string()),
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event_1)),
        })
        .await
        .unwrap();

        tx.send(SegmentWriteMessage::WriteData {
            id: Some("gc2".to_string()),
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
            500,  // channel buffer
        )
        .await
        .unwrap();

        // Write 1000 segment entries across 10 files
        let (tx, rx) = mpsc::channel(100);
        let (mut offset_stream, handle) = segment_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        let write_result_cnt = tokio::spawn(async move {
            let mut counter = 0;
            while let Some(msg) = offset_stream.next().await {
                counter += 1;
            }
            return counter;
        });

        let start_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let time_increment = chrono::Duration::seconds(1);

        for i in 1..=1000 {
            let mut message = Message::default();
            message.event_time = start_time + (time_increment * i);
            message.keys = Arc::from(vec!["test-key".to_string()]);
            message.value = bytes::Bytes::from(vec![1, 2, 3]);
            message.id = MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: i.to_string().into(),
                index: 0,
            };

            let proto_message: Bytes = message.try_into().unwrap();
            tx.send(SegmentWriteMessage::WriteData {
                id: Some(format!("msg-{}", i)),
                data: proto_message,
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

        assert_eq!(write_result_cnt.await.unwrap(), 1000);

        // Create and run compactor
        let compactor = Compactor::new(
            test_path.clone(),
            WindowKind::Aligned,
            1,    // 1MB
            1000, // 1s flush interval
            500,  // channel buffer
        )
        .await
        .unwrap();

        compactor.compact().await.unwrap();

        // Verify compacted data
        let compaction_wal = ReplayWal::new(WalType::Compact, test_path);
        let (mut rx, handle) = compaction_wal.streaming_read().unwrap();

        let mut remaining_message_count = 0;
        while let Some(entry) = rx.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let msg: numaflow_pb::objects::isb::Message = prost::Message::decode(data).unwrap();
                if let Some(header) = msg.header {
                    if let Some(message_info) = header.message_info {
                        let event_time = utc_from_timestamp(message_info.event_time);
                        assert!(
                            event_time > gc_end_2,
                            "Found message with event_time <= gc_end_2"
                        );
                    }
                }
                remaining_message_count += 1;
            }
        }
        handle.await.unwrap().unwrap();

        // Verify the number of remaining messages
        // Messages with event_time <= gc_end_2 should be removed
        assert_eq!(
            remaining_message_count, 980,
            "Expected 980 messages to remain after compaction"
        );
    }
}
