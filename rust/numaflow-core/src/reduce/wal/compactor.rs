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
use crate::reduce::wal::WalType;
use crate::reduce::wal::{GcEventEntry, Wal};
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
    gc_wal: ReplayWal,
    segment_wal: ReplayWal,
    compaction_ro_wal: ReplayWal,
    compaction_ao_wal: AppendOnlyWal,
    path: PathBuf,
    kind: WindowKind,
}

const WAL_KEY_SEPERATOR: &'static str = ":";

impl Compactor {
    pub(crate) async fn new(
        path: PathBuf,
        kind: WindowKind,
        max_file_size_mb: u64,
        flush_interval_ms: u64,
        channel_buffer_size: usize,
    ) -> WalResult<Self> {
        let segment_wal = ReplayWal::new(Wal::new(WalType::Data), path.clone());
        let gc_wal = ReplayWal::new(Wal::new(WalType::Gc), path.clone());
        let compaction_ro_wal = ReplayWal::new(Wal::new(WalType::Compact), path.clone());
        let compaction_ao_wal = AppendOnlyWal::new(
            Wal::new(WalType::Compact),
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
            path,
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
            WindowKind::Unaligned => {
                unimplemented!()
            }
        }
        Ok(())
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
        // Get the oldest time and scanned GC files
        let (oldest_time, gc_files) = self.build_aligned_compaction().await?;

        // Compact the compaction_ro_wal
        self.process_wal_stream(&self.compaction_ro_wal, oldest_time)
            .await?;

        // Compact the segment_wal
        self.process_wal_stream(&self.segment_wal, oldest_time)
            .await?;

        // Delete the GC files
        for gc_file in gc_files {
            info!(gc_file = %gc_file.display(), "removing segment file");
            tokio::fs::remove_file(gc_file).await?;
        }

        Ok(())
    }

    async fn process_wal_stream(
        &self,
        wal: &ReplayWal,
        oldest_time: DateTime<Utc>,
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
                    // Check if the message should be retained
                    if Self::should_retain_message(&data, oldest_time)? {
                        // Send the data to the compaction WAL
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

    /// Determines whether a message should be retained based on its event time.
    fn should_retain_message(data: &[u8], oldest_time: DateTime<Utc>) -> WalResult<bool> {
        // Deserialize the message
        let msg: isb::Message =
            prost::Message::decode(data).map_err(|e| format!("Failed to decode message: {}", e))?;

        // Extract the event time from the message
        let event_time = msg
            .header
            .as_ref()
            .and_then(|header| header.message_info.as_ref())
            .map(|info| utc_from_timestamp(info.event_time))
            .expect("Failed to extract event time from message");

        // Retain the message if its event time is greater than the oldest time
        Ok(event_time > oldest_time)
    }

    async fn compact_unaligned(&self) -> WalResult<()> {
        unimplemented!()
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
