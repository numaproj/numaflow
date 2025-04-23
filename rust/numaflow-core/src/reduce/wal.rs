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
            WalType::Compaction => ".wip".to_string(),
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

async fn simple_data_wal_writer() {
    let data_wal = AppendOnlyWal::new(
        WalType::new("segment"),
        "var/run/numaflow".into(),
        20 * 1024 * 1024,
        1000,
        500,
    )
    .await
    .unwrap();
    let messages: Vec<Message> = (0..10)
        .map(|i| {
            let headers = HashMap::new();
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["serve".to_string()]),
                tags: None,
                value: vec![1, 2, 3].into(),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Default::default(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "123".to_string().into(),
                    index: i as i32,
                },
                headers,
                metadata: None,
            }
        })
        .collect();

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (offset_stream, handle) = data_wal
        .streaming_write(ReceiverStream::new(rx))
        .await
        .unwrap();
    // write messages
    for message in messages {
        let _ = tx
            .send(FileWriterMessage::WriteData {
                id: message.id.to_string(),
                data: message.try_into().unwrap(), // impl from for bytes
            })
            .await;
    }

    let gc_wal = AppendOnlyWal::new(
        WalType::new("gc"),
        "var/run/numaflow".into(),
        1 * 1024 * 1024,
        1000,
        500,
    )
    .await
    .unwrap();
    let gc_event = GcEvent {
        start_time: Some(prost_timestamp_from_utc(Utc::now())),
        end_time: Some(prost_timestamp_from_utc(Utc::now())),
        keys: vec![],
    };

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let (offset_stream, handle) = gc_wal
        .streaming_write(ReceiverStream::new(rx))
        .await
        .unwrap();
    // write gc event
    let _ = tx
        .send(FileWriterMessage::WriteData {
            id: "gc".to_string(),
            data: Bytes::from(prost::Message::encode_to_vec(&gc_event)), // impl from for bytes
        })
        .await;
}
