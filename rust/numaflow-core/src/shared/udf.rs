use std::sync::Arc;

use numaflow_udf_client::{KeyValueGroup as SharedKeyValueGroup, UdfDatum, UdfMetadata};

use crate::message::Message;

/// Converts core's internal message into the protocol-neutral datum used by shared UDF sessions.
pub(crate) fn udf_datum_from_message(message: Message) -> UdfDatum {
    UdfDatum {
        id: message.id.to_string(),
        keys: message.keys.to_vec(),
        value: message.value,
        event_time: message.event_time,
        watermark: message.watermark,
        headers: Arc::unwrap_or_clone(message.headers),
        metadata: message.metadata.map(|metadata| {
            let metadata = Arc::unwrap_or_clone(metadata);
            UdfMetadata {
                previous_vertex: metadata.previous_vertex,
                sys_metadata: metadata
                    .sys_metadata
                    .into_iter()
                    .map(|(name, group)| {
                        (
                            name,
                            SharedKeyValueGroup {
                                key_value: group.key_value,
                            },
                        )
                    })
                    .collect(),
                user_metadata: metadata
                    .user_metadata
                    .into_iter()
                    .map(|(name, group)| {
                        (
                            name,
                            SharedKeyValueGroup {
                                key_value: group.key_value,
                            },
                        )
                    })
                    .collect(),
            }
        }),
    }
}
