use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::wal::segment::append::{AppendOnlyWal, FileWriterMessage};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// A WAL Segment.
pub(crate) mod segment;

/// A compactor to truncate processed WAL segments. It can be both Data WAL and GC WAL.
pub(crate) mod compactor;

/// All the errors WAL could face.
pub(crate) mod error;

// GC - RO, Segment - RO, Compact - RO,WA
//

// GC Event - (start, end, optional keys)
//
// GC Events WAL contract is that any GC event that is written means all the messages < the window
// end time can be deleted from the segment WAL(if keys are present only the messages with same
// keys will be deleted)
//
// Fixed window of 60s duration GC Events - (60, 120), (120, 180), (180, 240)
// Sliding window of 60s length and slide 10s GC Events - (60, 70), (70, 80), (80, 90)
// Session window with 10s timeout GC Events - (60, 100, [key1, key2]), (120, 1000, [key3])
//
//
// Compaction logic for Aligned kind
// 1. Replay all the GC events and store the max end time
// 2. Replay all the data events and only retain the messages with event time > max end time
//
// Compaction logic for Unaligned kind
// 1. Replay all the GC events and store the max end time for every key combination(map[key] = max end time)
// 2. Replay all the data events and only retain the messages with event time > max end time for that key
//
// NOTE: subsequent compactions should also consider already compacted files and order is very important
// during compaction we should make sure the order of the messages does not change.

/// WAL is made of three parts, the Segment, GC WAL, and Compaction WAL.
#[derive(Debug, Clone)]
pub(crate) enum WalType {
    /// Segment WAL contains the data.
    Segment,
    /// GC WAL contains the completed-processing events.
    Gc,
    /// Compaction WAL is a Segment WAL, but it has the compacted Segment data.
    Compaction,
}

impl std::fmt::Display for WalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalType::Segment => write!(f, "Segment"),
            WalType::Gc => write!(f, "GC"),
            WalType::Compaction => write!(f, "Compaction"),
        }
    }
}

impl WalType {
    fn new(segment: &'static str) -> Self {
        match segment {
            "gc" => WalType::Gc,
            "segment" => WalType::Segment,
            "compaction" => WalType::Compaction,
            _ => {
                unimplemented!("supported types are 'segment', 'gc', 'compaction'")
            }
        }
    }

    /// Some WALs have footers and some does not. It is mostly for optimizations.
    fn has_footer(&self) -> bool {
        // TODO: set footer to true for Segment and Compaction for optimizations.
        match self {
            WalType::Segment => false,
            WalType::Gc => false,
            WalType::Compaction => false,
        }
    }

    /// Prefix of the WAL Segment as stored in the disk.
    fn segment_prefix(&self) -> String {
        match self {
            WalType::Segment => "segment".to_string(),
            WalType::Gc => "gc".to_string(),
            WalType::Compaction => "compaction".to_string(),
        }
    }

    /// Suffix of the WAL Segment as stored in the disk. Not all WAL Segments have prefix,
    /// it is used to filter our work-in-progress WAL Segments that are derived from other WALs.
    fn segment_suffix(&self) -> String {
        match self {
            WalType::Segment => "".to_string(),
            WalType::Gc => "".to_string(),
            WalType::Compaction => "".to_string(),
        }
    }
}

/// An entry in the GC WAL about the GC action.
pub(crate) struct GcEventEntry {
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    keys: Option<Vec<String>>,
}

impl From<GcEvent> for GcEventEntry {
    fn from(value: GcEvent) -> Self {
        Self {
            start_time: utc_from_timestamp(value.start_time),
            end_time: utc_from_timestamp(value.end_time),
            keys: value.keys.into(),
        }
    }
}

impl From<GcEventEntry> for GcEvent {
    fn from(value: GcEventEntry) -> Self {
        Self {
            start_time: Some(prost_timestamp_from_utc(value.start_time)),
            end_time: Some(prost_timestamp_from_utc(value.end_time)),
            keys: value.keys.unwrap_or(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reduce::wal::compactor::{Compactor, WindowKind};
    use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
    use chrono::{TimeZone, Utc};
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_gc_wal_and_compaction() {
        let test_path = tempfile::tempdir().unwrap().into_path();

        // Create GC WAL
        let gc_wal = AppendOnlyWal::new(
            WalType::new("gc"),
            test_path.clone(),
            1,    // 1MB
            1000, // 1s flush interval
            500,  // channel buffer
        )
        .await
        .unwrap();

        // Create and write GC events
        let gc_start = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 5).unwrap();
        let gc_end = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 6).unwrap();

        let gc_event = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start)),
            end_time: Some(prost_timestamp_from_utc(gc_end)),
            keys: vec![],
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let (_offset_stream, handle) = gc_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        tx.send(FileWriterMessage::WriteData {
            id: Some("gc".to_string()),
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event)),
        })
        .await
        .unwrap();

        // Send rotate command to GC WAL
        tx.send(FileWriterMessage::Rotate { on_size: false })
            .await
            .unwrap();
        drop(tx);
        handle.await.unwrap().unwrap();

        // Create segment WAL
        let segment_wal = AppendOnlyWal::new(
            WalType::new("segment"),
            test_path.clone(),
            1,    // 20MB
            1000, // 1s flush interval
            500,  // channel buffer
        )
        .await
        .unwrap();

        // Write 100 segment entries
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let (_offset_stream, handle) = segment_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        let start_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let time_increment = chrono::Duration::seconds(36); // (3600s / 100 entries)

        for i in 0..100 {
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
            tx.send(FileWriterMessage::WriteData {
                id: Some(format!("msg-{}", i)),
                data: proto_message,
            })
            .await
            .unwrap();
        }

        // Send rotate command to segment WAL
        tx.send(FileWriterMessage::Rotate { on_size: false })
            .await
            .unwrap();
        drop(tx);
        handle.await.unwrap().unwrap();

        // Create and run compactor
        let compactor = Compactor::new(
            WalType::new("gc"),
            WalType::new("segment"),
            test_path.clone(),
            WindowKind::Aligned,
        );

        compactor.compact().await.unwrap();

        // Verify compacted data
        let compaction_wal = ReplayWal::new(WalType::new("compaction"), test_path);
        let (mut rx, handle) = compaction_wal.streaming_read().unwrap();

        while let Some(entry) = rx.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let msg: numaflow_pb::objects::isb::Message = prost::Message::decode(data).unwrap();
                if let Some(header) = msg.header {
                    if let Some(message_info) = header.message_info {
                        let event_time = utc_from_timestamp(message_info.event_time);
                        assert!(
                            event_time > gc_end,
                            "Found message with event_time <= gc_end"
                        );
                    }
                }
            }
        }
        handle.await.unwrap().unwrap();
    }
}
