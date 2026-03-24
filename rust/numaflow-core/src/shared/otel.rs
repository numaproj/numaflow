//! OpenTelemetry propagation helpers for Numaflow message metadata.
//!
//! Adapts `KeyValueGroup` (sys_metadata["tracing"]) as a carrier for W3C Trace Context.
//! The propagator uses these getter/setter implementations to read and write
//! `traceparent` and `tracestate` without knowing about our protobuf types.

use std::collections::HashMap;
use std::sync::Arc;

use opentelemetry::propagation::{Extractor, Injector};

use crate::metadata::KeyValueGroup;

/// Key under which W3C trace context is stored in `sys_metadata`.
pub const TRACING_METADATA_KEY: &str = "tracing";

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

/// Ensures the global text map propagator is set to W3C Trace Context.
/// Idempotent; safe to call from `run()` so Core can rely on propagation even
/// if the binary did not set it (e.g. in tests).
pub(crate) fn ensure_propagator() {
    use opentelemetry::global;
    let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
    global::set_text_map_propagator(propagator);
    tracing::info!("W3C TraceContext propagator installed");
}

/// Extracts an OpenTelemetry [`Context`] from a message's sys_metadata.
/// Returns `Context::current()` (root) if the tracing key is absent.
pub(crate) fn extract_trace_context(metadata: &crate::metadata::Metadata) -> opentelemetry::Context {
    use opentelemetry::global;
    match metadata.sys_metadata.get(TRACING_METADATA_KEY) {
        Some(kvg) => {
            let extractor = MetadataExtractor(kvg);
            global::get_text_map_propagator(|prop| prop.extract(&extractor))
        }
        None => opentelemetry::Context::current(),
    }
}

/// Wraps `HashMap<String, String>` (message headers) for **extraction**.
/// Used to extract W3C trace context from incoming message headers
/// (e.g., Kafka headers passed through by eventbus-source).
pub struct HeaderExtractor<'a>(pub &'a HashMap<String, String>);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        // Try exact match first, then case-insensitive (Kafka headers are case-sensitive
        // but upstream producers may use varying cases for B3 headers)
        self.0
            .get(key)
            .map(String::as_str)
            .or_else(|| {
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
    use opentelemetry::global;

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
    headers
        .get(key)
        .map(String::as_str)
        .or_else(|| {
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
/// - Maps sampled: "1"/"true" → "01", "0"/"false" → "00", default → "01"
fn b3_to_traceparent(trace_id: &str, span_id: &str, sampled: Option<&str>) -> String {
    // Pad 64-bit trace IDs to 128-bit
    let padded_trace_id = if trace_id.len() <= 16 {
        format!("{:0>32}", trace_id)
    } else {
        trace_id.to_string()
    };

    let flags = match sampled {
        Some("1") | Some("true") => "01",
        Some("0") | Some("false") => "00",
        Some("d") => "01", // debug = sampled
        _ => "01",         // default to sampled
    };

    format!("00-{}-{}-{}", padded_trace_id, span_id, flags)
}

/// Injects the current span's trace context into a message's sys_metadata
/// so downstream vertices / UDFs can continue the trace.
///
/// Uses `tracing::Span::current().context()` (from `OpenTelemetrySpanExt`)
/// because `tracing-opentelemetry` stores the OTel span in the tracing span's
/// extensions, NOT in the thread-local `opentelemetry::Context`.
pub(crate) fn inject_trace_context(metadata: &mut crate::metadata::Metadata) {
    use opentelemetry::global;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let cx = tracing::Span::current().context();

    let kvg = metadata
        .sys_metadata
        .entry(TRACING_METADATA_KEY.to_string())
        .or_insert_with(|| KeyValueGroup {
            key_value: std::collections::HashMap::new(),
        });
    let mut injector = MetadataInjector(kvg);
    global::get_text_map_propagator(|prop| {
        prop.inject_context(&cx, &mut injector);
    });
}
