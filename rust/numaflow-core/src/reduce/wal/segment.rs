//! There are three types of Segments that goes into the WAL.
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

/// Reading from WAL for Replays.
pub(crate) mod replay;

/// Writing/Append to the WAL.
pub(crate) mod append;

/// A compactor to truncate processed WAL segments. It can be both Data WAL and GC WAL.
pub(crate) mod compactor;

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
    pub(crate) start_time: DateTime<Utc>,
    /// The end time of the Window. Anything after this timestamp has not been processed.
    pub(crate) end_time: DateTime<Utc>,
    /// Keys are used only for Unaligned window since sessions are defined not purely on [crate::watermark]
    /// but also on Keys.
    pub(crate) keys: Option<Vec<String>>,
}

impl From<GcEvent> for GcEventEntry {
    fn from(value: GcEvent) -> Self {
        Self {
            start_time: value
                .start_time
                .map(utc_from_timestamp)
                .expect("start time should be present"),
            end_time: value
                .end_time
                .map(utc_from_timestamp)
                .expect("end time should be present"),
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
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use crate::reduce::wal::segment::WalType;
    use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
    use crate::reduce::wal::segment::compactor::{Compactor, WindowKind};
    use crate::reduce::wal::segment::replay::{ReplayWal, SegmentEntry};
    use chrono::{TimeZone, Utc};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_gc_wal_and_aligned_compaction() {
        let test_path = tempfile::tempdir().unwrap().keep();

        // Create GC WAL
        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            test_path.clone(),
            1,    // 1MB
            1000, // 1s flush interval
            300,  // 5 minutes
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
        let handle = gc_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        tx.send(SegmentWriteMessage::WriteGcEvent {
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
            300,  // 5 minutes
        )
        .await
        .unwrap();

        // Write 100 segment entries
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let handle = segment_wal
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
        }

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create and run compactor
        let compactor = Compactor::new(test_path.clone(), WindowKind::Aligned, 1, 1000, 300)
            .await
            .unwrap();

        let (replay_tx, _replay_rx) = tokio::sync::mpsc::channel(1000);
        let cln_token = CancellationToken::new();

        let handle = compactor
            .start_compaction_with_replay(replay_tx, Duration::from_secs(60), cln_token.clone())
            .await
            .unwrap();

        // signal the compaction task to stop and wait for it to complete
        cln_token.cancel();
        handle.await.unwrap().unwrap();

        // Verify compacted data
        let compaction_replay_wal = ReplayWal::new(WalType::Compact, test_path);
        let (mut rx, handle) = compaction_replay_wal.streaming_read().unwrap();

        let mut remaining_message_count = 0;
        while let Some(entry) = rx.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let msg: numaflow_pb::objects::isb::ReadMessage =
                    prost::Message::decode(data).unwrap();
                if let Some(header) = msg.message.unwrap().header
                    && let Some(message_info) = header.message_info
                {
                    let event_time = message_info.event_time.map(utc_from_timestamp).unwrap();
                    assert!(
                        event_time >= gc_end,
                        "Found message with event_time < gc_end"
                    );
                }
                remaining_message_count += 1
            }
        }
        handle.await.unwrap().unwrap();

        // we send 100 messages out of which 9 messages have event times less than the window end
        // time so the remaining message cound should be 91
        assert_eq!(
            remaining_message_count, 91,
            "Expected 91 messages to remain after compaction"
        );
    }

    #[tokio::test]
    async fn test_gc_wal_and_unaligned_compaction() {
        let test_path = tempfile::tempdir().unwrap().keep();

        // Create GC WAL
        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            test_path.clone(),
            1,    // 1MB
            1000, // 1s flush interval
            300,  // 5 minutes
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
        let handle = gc_wal
            .streaming_write(ReceiverStream::new(rx))
            .await
            .unwrap();

        // Send both GC events
        tx.send(SegmentWriteMessage::WriteGcEvent {
            data: bytes::Bytes::from(prost::Message::encode_to_vec(&gc_event1)),
        })
        .await
        .unwrap();

        tx.send(SegmentWriteMessage::WriteGcEvent {
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
            300,  // 5 minutes
        )
        .await
        .unwrap();

        // Write segment entries with different keys
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let handle = segment_wal
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
        }

        drop(tx);
        handle.await.unwrap().unwrap();

        // Create and run compactor with unaligned window kind
        let compactor = Compactor::new(
            test_path.clone(),
            WindowKind::Unaligned,
            1,    // 20MB
            1000, // 1s flush interval
            300,  // max_segment_age_secs
        )
        .await
        .unwrap();

        let cln_token = CancellationToken::new();
        let (replay_tx, _replay_rx) = tokio::sync::mpsc::channel(1000);

        let handle = compactor
            .start_compaction_with_replay(replay_tx, Duration::from_secs(60), cln_token.clone())
            .await
            .unwrap();

        // signal the compaction task to stop and wait for it to complete
        cln_token.cancel();
        handle.await.unwrap().unwrap();

        // Verify compacted data
        let compaction_wal = ReplayWal::new(WalType::Compact, test_path);
        let (mut rx, handle) = compaction_wal.streaming_read().unwrap();

        let mut remaining_message_count = 0;
        let mut key1_count = 0;
        let mut key2_count = 0;

        while let Some(entry) = rx.next().await {
            if let SegmentEntry::DataEntry { data, .. } = entry {
                let msg: numaflow_pb::objects::isb::ReadMessage =
                    prost::Message::decode(data).unwrap();
                // Check event time based on key
                if let Some(header) = msg.message.unwrap().header
                    && let (Some(message_info), keys) = (header.message_info.as_ref(), header.keys)
                {
                    let event_time = message_info.event_time.map(utc_from_timestamp).unwrap();
                    if keys.contains(&"key1".to_string()) {
                        key1_count += 1;
                        assert!(
                            event_time >= gc_end_key1,
                            "Found key1 message with event_time < gc_end_key1"
                        );
                    } else if keys.contains(&"key2".to_string()) {
                        key2_count += 1;
                        assert!(
                            event_time >= gc_end_key2,
                            "Found key2 message with event_time < gc_end_key2"
                        );
                    }
                }
                remaining_message_count += 1
            }
        }
        handle.await.unwrap().unwrap();

        // For key1, we should have removed messages with event time < 10s (9 messages)
        // For key2, we should have removed messages with event time < 15s (15 messages)
        // Total messages: 100 - (4 key1 messages + 7 key2 messages) = 87
        // Note: Since we alternate keys and start from 1, key1 is at odd indices and key2 at even indices
        assert_eq!(
            remaining_message_count, 89,
            "Expected 89 messages to remain after compaction"
        );

        // We expect 45 key1 messages (50 - 4) and 40 key2 messages (50 - 7)
        assert_eq!(key1_count, 46, "Expected 46 key1 messages to remain");
        assert_eq!(key2_count, 43, "Expected 43 key2 messages to remain");
    }
}
