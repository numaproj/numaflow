//! OpenTelemetry propagation helpers for Numaflow message metadata.
//!
//! Adapts `KeyValueGroup` (sys_metadata["tracing"]) as a carrier for W3C Trace Context.
//! The propagator uses these getter/setter implementations to read and write
//! `traceparent` and `tracestate` without knowing about our protobuf types.

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
