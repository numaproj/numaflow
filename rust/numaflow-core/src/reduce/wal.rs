//! Write Ahead Log for both Aligned and Unaligned Reduce Operation. The WAL is to persist the data
//! we read from the ISB and store it until the processing is complete.

use crate::config::components::reduce::StorageConfig;
use crate::message::{IntOffset, Message, Offset};
use crate::reduce::error::Error;
use crate::reduce::pbq::WAL;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::append::AppendOnlyWal;
use crate::reduce::wal::segment::compactor::{Compactor, WindowKind};
use crate::reduce::wal::segment::wal::{FsStore, WalStore};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use bytes::Bytes;
use std::sync::Arc;

/// A WAL Segment.
pub(crate) mod segment;

/// All the errors WAL could face.
pub(crate) mod error;

#[derive(Debug, Clone)]
pub(crate) struct WalMessage {
    pub(crate) message: Message,
}

// Convert Message to Bytes used to persist in WAL
impl TryFrom<WalMessage> for Bytes {
    type Error = Error;

    fn try_from(wal_message: WalMessage) -> Result<Self, Self::Error> {
        let message = wal_message.message;

        let Offset::Int(int_offset) = message.offset else {
            return Err(Error::Other("Invalid offset".to_string()));
        };

        let proto_message = numaflow_pb::objects::isb::ReadMessage {
            message: Some(numaflow_pb::objects::isb::Message {
                header: Some(numaflow_pb::objects::isb::Header {
                    message_info: Some(numaflow_pb::objects::isb::MessageInfo {
                        event_time: Some(prost_timestamp_from_utc(message.event_time)),
                        is_late: message.is_late,
                    }),
                    kind: message.typ.into(),
                    id: Some(message.id.into()),
                    keys: message.keys.to_vec(),
                    headers: Arc::unwrap_or_clone(message.headers),
                    metadata: message.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
                }),
                body: Some(numaflow_pb::objects::isb::Body {
                    payload: message.value.to_vec(),
                }),
            }),
            read_offset: int_offset.offset,
            watermark: message.watermark.map(prost_timestamp_from_utc),
            metadata: None,
        };

        Ok(Bytes::from(prost::Message::encode_to_vec(&proto_message)))
    }
}

// Convert Bytes to Message, used while reading from WAL
impl TryFrom<Bytes> for WalMessage {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let proto_read_message: numaflow_pb::objects::isb::ReadMessage =
            prost::Message::decode(value.as_ref()).map_err(|e| Error::Other(e.to_string()))?;

        let proto_message = proto_read_message
            .message
            .ok_or_else(|| Error::Other("Missing inner message".to_string()))?;

        let header = proto_message
            .header
            .ok_or_else(|| Error::Other("Missing header".to_string()))?;
        let body = proto_message
            .body
            .ok_or_else(|| Error::Other("Missing body".to_string()))?;

        let message_info = header.message_info.expect("info can't be empty");
        let msg = Message {
            typ: header.kind.into(),
            keys: Arc::from(header.keys),
            tags: None,
            value: Bytes::from(body.payload),
            offset: Offset::Int(IntOffset::new(proto_read_message.read_offset, 0)),
            event_time: message_info
                .event_time
                .map(utc_from_timestamp)
                .expect("event time should be present"),
            watermark: proto_read_message.watermark.map(utc_from_timestamp),
            id: header.id.map(Into::into).unwrap_or_default(),
            is_late: message_info.is_late,
            ..Default::default()
        };

        Ok(WalMessage { message: msg })
    }
}

impl From<WalMessage> for Message {
    fn from(value: WalMessage) -> Self {
        value.message
    }
}

impl From<Message> for WalMessage {
    fn from(value: Message) -> Self {
        WalMessage { message: value }
    }
}

/// Create WAL components for reduce operations.
///
/// This function creates both the main WAL and GC WAL if storage is configured.
/// The only difference between aligned and unaligned reducers is the WindowKind.
pub(crate) async fn create_wal_components(
    storage_config: Option<&StorageConfig>,
    window_kind: WindowKind,
) -> crate::Result<(Option<WAL>, Option<AppendOnlyWal>)> {
    let Some(storage_config) = storage_config else {
        return Ok((None, None));
    };

    // the single store; all WAL types are built from it, so they share the same storage
    tokio::fs::create_dir_all(&storage_config.path)
        .await
        .map_err(|e| crate::error::Error::WAL(e.to_string()))?;

    let store: Arc<dyn WalStore> = Arc::new(FsStore::new(storage_config.path.clone()));

    create_wal_components_with_store(storage_config, window_kind, store).await
}

/// Builds the WAL components from the given store.
async fn create_wal_components_with_store(
    storage_config: &StorageConfig,
    window_kind: WindowKind,
    store: Arc<dyn WalStore>,
) -> crate::Result<(Option<WAL>, Option<AppendOnlyWal>)> {
    let append_only_wal = AppendOnlyWal::new(
        WalType::Data,
        store.writer(WalType::Data).await?,
        storage_config.max_file_size_mb,
        storage_config.flush_interval_ms,
        storage_config.max_segment_age_secs,
    );

    let compactor = Compactor::new(
        Arc::clone(&store),
        window_kind,
        storage_config.max_file_size_mb,
        storage_config.flush_interval_ms,
        storage_config.max_segment_age_secs,
    )
    .await?;

    let gc_wal = AppendOnlyWal::new(
        WalType::Gc,
        store.writer(WalType::Gc).await?,
        storage_config.max_file_size_mb,
        storage_config.flush_interval_ms,
        storage_config.max_segment_age_secs,
    );

    Ok((
        Some(WAL {
            append_only_wal,
            compactor,
        }),
        Some(gc_wal),
    ))
}
