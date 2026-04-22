//! OpenTelemetry propagation helpers for Numaflow message metadata.
//!
//! Adapts `KeyValueGroup` (sys_metadata) as a carrier for W3C Trace Context.
//! The propagator uses these getter/setter implementations to read and write
//! `traceparent` and `tracestate` without knowing about our protobuf types.

use std::collections::HashMap;
use std::sync::Arc;

use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};

use crate::metadata::KeyValueGroup;

/// Key under which W3C trace context is stored in `sys_metadata`.
/// Always holds the `platform.process` span context - the shared parent
/// that makes source.read, map, and sink.write siblings.
pub const TRACING_METADATA_KEY: &str = "tracing";

/// Key under which the current stage's span context is stored for UDF consumption.
/// The UDF reads this key to see the platform stage (e.g., map) as its parent.
/// Written before calling the UDF, removed after the UDF returns.
pub const TRACING_UDF_METADATA_KEY: &str = "tracing_udf";

/// Wraps `KeyValueGroup` for **extraction**: the propagator calls `get` / `keys`
/// to read `traceparent` and `tracestate` from the carrier.
pub struct MetadataExtractor<'a>(pub &'a KeyValueGroup);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .key_value
            .get(key)
            .and_then(|b| std::str::from_utf8(b.as_ref()).ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.key_value.keys().map(String::as_str).collect()
    }
}

/// Wraps `KeyValueGroup` for **injection**: the propagator calls `set` to write
/// `traceparent` and `tracestate` into the carrier.
pub struct MetadataInjector<'a>(pub &'a mut KeyValueGroup);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0
            .key_value
            .insert(key.to_string(), bytes::Bytes::from(value.into_bytes()));
    }
}

/// Extracts an OpenTelemetry [`Context`] from a message's sys_metadata.
/// Returns `Context::current()` (root) if the tracing key is absent.
pub(crate) fn extract_trace_context(
    metadata: &crate::metadata::Metadata,
) -> opentelemetry::Context {
    match metadata.sys_metadata.get(TRACING_METADATA_KEY) {
        Some(kvg) => {
            let extractor = MetadataExtractor(kvg);
            global::get_text_map_propagator(|prop| prop.extract(&extractor))
        }
        None => opentelemetry::Context::current(),
    }
}

/// Injects a specific OpenTelemetry [`Context`] into a named sys_metadata key.
///
/// Used to inject `platform.process` context into `"tracing"` (preserving the root)
/// and stage-specific context into `"tracing_udf"` (for UDF parent).
pub(crate) fn inject_context_into_metadata(
    metadata: &mut crate::metadata::Metadata,
    key: &str,
    cx: &opentelemetry::Context,
) {
    let kvg = metadata
        .sys_metadata
        .entry(key.to_string())
        .or_insert_with(|| KeyValueGroup {
            key_value: HashMap::new(),
        });
    let mut injector = MetadataInjector(kvg);
    global::get_text_map_propagator(|prop| {
        prop.inject_context(cx, &mut injector);
    });
}

/// Wraps `HashMap<String, String>` (message headers) for **extraction**.
/// Used to extract W3C trace context from incoming message headers
/// (e.g., Kafka headers passed through by source connectors).
pub struct HeaderExtractor<'a>(pub &'a HashMap<String, String>);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        // Try exact match first, then case-insensitive (Kafka headers are case-sensitive
        // but upstream producers may use varying cases for B3 headers)
        self.0.get(key).map(String::as_str).or_else(|| {
            let lower = key.to_lowercase();
            self.0
                .iter()
                .find(|(k, _)| k.to_lowercase() == lower)
                .map(|(_, v)| v.as_str())
        })
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Extracts an OpenTelemetry [`Context`] from incoming message headers.
///
/// Checks for W3C `traceparent` first, then B3 multi-headers
/// (`X-B3-TraceId`, `X-B3-SpanId`, `X-B3-Sampled`). If B3 headers are found,
/// converts them to W3C `traceparent` format before extraction.
///
/// Returns `Context::current()` (root) if no trace headers are present.
pub(crate) fn extract_trace_context_from_headers(
    headers: &Arc<HashMap<String, String>>,
) -> opentelemetry::Context {
    // Check for W3C traceparent header first
    if get_header_case_insensitive(headers, "traceparent").is_some() {
        let extractor = HeaderExtractor(headers.as_ref());
        return global::get_text_map_propagator(|prop| prop.extract(&extractor));
    }

    // Check for B3 multi-headers and convert to W3C traceparent
    let trace_id = get_header_case_insensitive(headers, "X-B3-TraceId");
    let span_id = get_header_case_insensitive(headers, "X-B3-SpanId");

    if let (Some(trace_id), Some(span_id)) = (trace_id, span_id) {
        let sampled = get_header_case_insensitive(headers, "X-B3-Sampled");
        let traceparent = b3_to_traceparent(trace_id, span_id, sampled);

        let mut synthetic = HashMap::new();
        synthetic.insert("traceparent".to_string(), traceparent);
        let extractor = HeaderExtractor(&synthetic);
        return global::get_text_map_propagator(|prop| prop.extract(&extractor));
    }

    opentelemetry::Context::current()
}

/// Case-insensitive header lookup.
fn get_header_case_insensitive<'a>(
    headers: &'a HashMap<String, String>,
    key: &str,
) -> Option<&'a str> {
    headers.get(key).map(String::as_str).or_else(|| {
        let lower = key.to_lowercase();
        headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == lower)
            .map(|(_, v)| v.as_str())
    })
}

/// Converts B3 multi-header values to W3C traceparent format.
///
/// Format: `{version}-{trace_id}-{span_id}-{trace_flags}`
/// - Pads 64-bit trace IDs to 128-bit (left-pads with zeros)
/// - Maps sampled: "1"/"true" -> "01", "0"/"false" -> "00", default -> "01"
fn b3_to_traceparent(trace_id: &str, span_id: &str, sampled: Option<&str>) -> String {
    let padded_trace_id = if trace_id.len() <= 16 {
        format!("{:0>32}", trace_id)
    } else {
        trace_id.to_string()
    };

    let flags = match sampled {
        Some("1" | "true") => "01",
        Some("0" | "false") => "00",
        Some("d") => "01", // debug = sampled
        _ => "01",         // default to sampled
    };

    format!("00-{}-{}-{}", padded_trace_id, span_id, flags)
}
