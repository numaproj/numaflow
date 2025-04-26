//! Write Ahead Log for both Aligned and Unaligned Reduce Operation. The WAL is to persist the data
//! we read from the ISB and store it until the processing is complete. There are three types of data
//! that goes into the WAL.
//!
//! ### Data
//! The Data WAL contains the raw data [crate::message::Message] that is streamed from the ISB.
//!
//! ### GC Events
//! The GC Events contract is that any GC event that is written means all the messages < the window
//! end time can be deleted from the segment WAL(if keys are present only the messages with same
//! keys will be deleted).
//! E.g., of GC Events,
//! Fixed window of 60s duration GC Events - (60, 120), (120, 180), (180, 240)
//! Sliding window of 60s length and slide 10s GC Events - (60, 70), (70, 80), (80, 90)
//! Session window with 10s timeout GC Events - (60, 100, `[key1, key2]`), (120, 1000, `[key3]`)
//!
//! ### Compaction
//! While compacting, a new data segment is created called Compaction Segments. These contain the
//! data elements that cannot be deleted because the reduction is not complete yet.
//! Subsequent compactions should also consider already compacted files and order is very important
//! during compaction we should make sure the order of the messages does not change.

use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;

/// A WAL Segment.
pub(crate) mod segment;

/// A compactor to truncate processed WAL segments. It can be both Data WAL and GC WAL.
pub(crate) mod compactor;

/// All the errors WAL could face.
pub(crate) mod error;

/// WAL is made of three types, the Data, GC, and Compaction WAL.
#[derive(Debug, Clone)]
pub(crate) enum WalType {
    /// Data WAL contains the raw data.
    Data,
    /// GC WAL contains the completed-processing events.
    Gc,
    /// Compaction WAL is a Data WAL, but it has the compacted data after purging based on GC Events.
    Compact,
}

impl WalType {
    /// Some WALs have footers and some does not. It is mostly for optimizations.
    pub(crate) fn has_footer(&self) -> bool {
        // TODO: Set footer to true for Segment and Compaction for optimizations if needed.
        match self {
            WalType::Data => false,
            WalType::Gc => false,
            WalType::Compact => false,
        }
    }

    /// Prefix of the WAL Segment as stored in the disk.
    pub(crate) fn segment_prefix(&self) -> &'static str {
        match self {
            WalType::Data => "data",
            WalType::Gc => "gc",
            WalType::Compact => "compaction",
        }
    }

    /// Suffix of the WAL Segment as stored in the disk. Not all WAL Segments have prefix,
    /// it is used to filter our work-in-progress WAL Segments that are derived from other WALs.
    pub(crate) fn segment_suffix(&self) -> &'static str {
        match self {
            WalType::Data => "",
            WalType::Gc => "",
            WalType::Compact => "",
        }
    }
}

impl std::fmt::Display for WalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalType::Data => write!(f, "Data"),
            WalType::Gc => write!(f, "GC"),
            WalType::Compact => write!(f, "Compact"),
        }
    }
}

/// An entry in the GC WAL about the GC action.
#[derive(Debug)]
pub(crate) struct GcEventEntry {
    /// Start time of the Window
    start_time: DateTime<Utc>,
    /// The end time of the Window. Anything after this timestamp has not been processed.
    end_time: DateTime<Utc>,
    /// Keys are used only for Unaligned window since sessions are defined not purely on [crate::watermark]
    /// but also on Keys.
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
            keys: value.keys.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, MessageID};
    use crate::reduce::wal::compactor::{Compactor, WindowKind};
    use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
    use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use std::sync::Arc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_gc_wal_and_aligned_compaction() {
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

        // Create and write GC events
        let gc_start = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 5).unwrap();
        let gc_end = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 10).unwrap();

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

        tx.send(SegmentWriteMessage::WriteData {
            id: Some("gc".to_string()),
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event)),
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create segment WAL
        let segment_wal = AppendOnlyWal::new(
            WalType::Data,
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
        let time_increment = chrono::Duration::seconds(1);

        for i in 1..=100 {
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
        }

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create and run compactor
        let compactor = Compactor::new(
            test_path.clone(),
            WindowKind::Aligned,
            1,    // 20MB
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
                            event_time > gc_end,
                            "Found message with event_time <= gc_end"
                        );
                    }
                }
                remaining_message_count += 1
            }
        }
        handle.await.unwrap().unwrap();

        // we send 100 messages out of which 10 messages have event times less than the window end
        // time so the remaining message cound should be 90
        assert_eq!(
            remaining_message_count, 90,
            "Expected 90 messages to remain after compaction"
        );
    }

    #[tokio::test]
    async fn test_gc_wal_and_unaligned_compaction() {
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

        // Create and write GC events with different keys
        let gc_start = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let gc_end_key1 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 10).unwrap();
        let gc_end_key2 = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 15).unwrap();

        let gc_event1 = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start)),
            end_time: Some(prost_timestamp_from_utc(gc_end_key1)),
            keys: vec!["key1".to_string()],
        };

        let gc_event2 = GcEvent {
            start_time: Some(prost_timestamp_from_utc(gc_start)),
            end_time: Some(prost_timestamp_from_utc(gc_end_key2)),
            keys: vec!["key2".to_string()],
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let (_offset_stream, handle) = gc_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        // Send both GC events
        tx.send(SegmentWriteMessage::WriteData {
            id: Some("gc1".to_string()),
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event1)),
        })
        .await
        .unwrap();

        tx.send(SegmentWriteMessage::WriteData {
            id: Some("gc2".to_string()),
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event2)),
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create segment WAL
        let segment_wal = AppendOnlyWal::new(
            WalType::Data,
            test_path.clone(),
            1,    // 20MB
            1000, // 1s flush interval
            500,  // channel buffer
        )
        .await
        .unwrap();

        // Write segment entries with different keys
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let (_offset_stream, handle) = segment_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        let start_time = Utc.with_ymd_and_hms(2025, 4, 1, 1, 0, 0).unwrap();
        let time_increment = chrono::Duration::seconds(1);

        // Create 50 messages with key1 and 50 messages with key2
        for i in 1..=100 {
            let mut message = Message::default();
            message.event_time = start_time + (time_increment * i);

            // Alternate between key1 and key2
            let key = if i % 2 == 0 { "key1" } else { "key2" };
            message.keys = Arc::from(vec![key.to_string()]);

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
        }

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create and run compactor with unaligned window kind
        let compactor = Compactor::new(
            test_path.clone(),
            WindowKind::Unaligned,
            1,    // 20MB
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
        let mut key1_count = 0;
        let mut key2_count = 0;

        while let Some(entry) = rx.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let msg: numaflow_pb::objects::isb::Message = prost::Message::decode(data).unwrap();
                if let Some(header) = msg.header {
                    // Check event time based on key
                    if let (Some(message_info), keys) = (header.message_info.as_ref(), header.keys)
                    {
                        let event_time = utc_from_timestamp(message_info.event_time);

                        if keys.contains(&"key1".to_string()) {
                            key1_count += 1;
                            assert!(
                                event_time > gc_end_key1,
                                "Found key1 message with event_time <= gc_end_key1"
                            );
                        } else if keys.contains(&"key2".to_string()) {
                            key2_count += 1;
                            assert!(
                                event_time > gc_end_key2,
                                "Found key2 message with event_time <= gc_end_key2"
                            );
                        }
                    }
                }
                remaining_message_count += 1
            }
        }
        handle.await.unwrap().unwrap();

        // For key1, we should have removed messages with event time <= 10s (10 messages)
        // For key2, we should have removed messages with event time <= 15s (15 messages)
        // Total messages: 100 - (5 key1 messages + 8 key2 messages) = 87
        // Note: Since we alternate keys and start from 1, key1 is at odd indices and key2 at even indices
        assert_eq!(
            remaining_message_count, 87,
            "Expected 87 messages to remain after compaction"
        );

        // We expect 45 key1 messages (50 - 5) and 40 key2 messages (50 - 8)
        assert_eq!(key1_count, 45, "Expected 45 key1 messages to remain");
        assert_eq!(key2_count, 42, "Expected 42 key2 messages to remain");
    }
}
