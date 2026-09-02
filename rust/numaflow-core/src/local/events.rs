//! Public event types and their conversion to/from the crate-internal [`Message`].
//!
//! [`InputEvent`] / [`OutputEvent`] are the public mirror of `Message` (which is `pub(crate)` and
//! not nameable outside the crate). The conversion helpers here are the only place the public
//! shape and the internal shape meet.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::local::config_builder::INPUT_VERTEX;
use crate::message::{IntOffset, Message, MessageID, MessageType, Offset};
use crate::metadata::{KeyValueGroup, Metadata};

/// An event fed into the input buffer of a local run.
#[derive(Debug, Clone)]
pub struct InputEvent {
    pub payload: Bytes,
    pub keys: Vec<String>,
    pub headers: HashMap<String, String>,
    pub event_time: DateTime<Utc>,
    /// Only meaningful for reduce (drives the watermark driver); ignored for other kinds.
    pub watermark: Option<DateTime<Utc>>,
    /// Must be unique per run — it is the ISB dedup key (gotcha G2). The in-memory writer keys
    /// dedup on `MessageID.to_string()`, so two events sharing an id → the second is silently
    /// dropped.
    pub id: String,
    /// User metadata as `group -> key -> value`. Mirrors `Metadata.user_metadata` (whose values
    /// are `Bytes`); we expose `String` for ergonomics and encode to bytes at conversion time.
    pub user_metadata: Option<HashMap<String, HashMap<String, String>>>,
    /// The upstream vertex name the UDF should observe. Note (G13): the ISB proto encode rewrites
    /// `metadata.previous_vertex` to the *writing* vertex's name, so for this to be honored the
    /// config builder must name the input vertex accordingly. The facade currently uses the fixed
    /// input-vertex name, so a per-event override here is recorded in metadata but overwritten on
    /// encode — kept in the API for forward compatibility.
    pub previous_vertex: Option<String>,
}

impl Default for InputEvent {
    fn default() -> Self {
        Self {
            payload: Bytes::new(),
            keys: vec![],
            headers: HashMap::new(),
            event_time: Utc::now(),
            watermark: None,
            id: String::new(),
            user_metadata: None,
            previous_vertex: None,
        }
    }
}

/// An event read back from the output buffer of a local run.
#[derive(Debug, Clone)]
pub struct OutputEvent {
    pub payload: Bytes,
    pub keys: Vec<String>,
    pub headers: HashMap<String, String>,
    pub event_time: DateTime<Utc>,
    /// `MessageID` display form `"{vertex}-{offset}-{index}"`.
    pub id: String,
    /// Broker offset display form.
    pub offset: String,
    /// User metadata rendered as `group.key=value; ...`, if any was present.
    pub metadata_summary: Option<String>,
}

/// Build the internal `Metadata` for an input event, or `None` if there is nothing to carry.
fn build_metadata(ev: &InputEvent) -> Option<Arc<Metadata>> {
    let has_user = ev.user_metadata.as_ref().is_some_and(|m| !m.is_empty());
    let has_prev = ev.previous_vertex.is_some();
    if !has_user && !has_prev {
        return None;
    }

    let user_metadata = ev
        .user_metadata
        .as_ref()
        .map(|groups| {
            groups
                .iter()
                .map(|(group, kvs)| {
                    let key_value = kvs
                        .iter()
                        .map(|(k, v)| (k.clone(), Bytes::from(v.clone().into_bytes())))
                        .collect();
                    (group.clone(), KeyValueGroup { key_value })
                })
                .collect()
        })
        .unwrap_or_default();

    Some(Arc::new(Metadata {
        // See G13: this is overwritten by the ISB encode to the input-vertex name.
        previous_vertex: ev.previous_vertex.clone().unwrap_or_default(),
        sys_metadata: HashMap::new(),
        user_metadata,
    }))
}

/// Convert a public [`InputEvent`] into the internal `Message` written into the input buffer.
///
/// Modeled on the reduce-forwarder template test's write loop and the factory test's
/// `test_message`. The `offset` is a dummy — the in-memory buffer assigns real offsets on write.
pub(crate) fn input_event_to_message(ev: InputEvent) -> Message {
    let metadata = build_metadata(&ev);
    Message {
        typ: MessageType::Data,
        keys: Arc::from(ev.keys),
        tags: None,
        value: ev.payload,
        // Dummy offset; the in-memory buffer assigns the real offset on write.
        offset: Offset::Int(IntOffset::new(0, 0)),
        event_time: ev.event_time,
        // Watermark is consumed by the reduce watermark driver, not persisted through the ISB.
        watermark: None,
        id: MessageID {
            vertex_name: INPUT_VERTEX.into(),
            offset: ev.id.into(),
            index: 0,
        },
        headers: Arc::new(ev.headers),
        metadata,
        is_late: false,
        nack_options: None,
    }
}

/// Render user metadata into a compact `group.key=value; ...` summary for display.
fn render_metadata(metadata: &Metadata) -> Option<String> {
    if metadata.user_metadata.is_empty() {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    for (group, kvg) in &metadata.user_metadata {
        for (k, v) in &kvg.key_value {
            // Values are arbitrary bytes; show UTF-8 when valid, else a byte-length placeholder.
            let value = match std::str::from_utf8(v) {
                Ok(s) => s.to_string(),
                Err(_) => format!("<{} bytes>", v.len()),
            };
            parts.push(format!("{group}.{k}={value}"));
        }
    }
    // Deterministic ordering so output is stable across HashMap iteration order.
    parts.sort();
    Some(parts.join("; "))
}

/// Convert an internal `Message` read from the output buffer into a public [`OutputEvent`].
pub(crate) fn message_to_output_event(msg: Message) -> OutputEvent {
    let metadata_summary = msg.metadata.as_deref().and_then(render_metadata);
    OutputEvent {
        payload: msg.value,
        keys: msg.keys.to_vec(),
        headers: (*msg.headers).clone(),
        event_time: msg.event_time,
        id: msg.id.to_string(),
        offset: msg.offset.to_string(),
        metadata_summary,
    }
}
