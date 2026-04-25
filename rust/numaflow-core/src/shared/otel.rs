//! OpenTelemetry propagation helpers for Numaflow message metadata.
//!
//! Adapts `KeyValueGroup` (sys_metadata) as a carrier for W3C Trace Context.
//! The propagator uses these getter/setter implementations to read and write
//! `traceparent` and `tracestate` without knowing about our protobuf types.

use std::collections::HashMap;
use std::sync::Arc;

use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{TraceContextExt as _, Tracer};

use crate::metadata::{KeyValueGroup, Metadata};

/// Key under which W3C trace context is stored in `sys_metadata`.
/// Always holds the propagated `vertex.process` context for downstream
/// Numaflow-managed spans (for example source, map, and sink spans).
pub const TRACING_METADATA_KEY: &str = "tracing";

/// Key under which the current stage's span context is stored for UDF consumption.
/// The UDF reads this key to see the current Numaflow stage as its parent.
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
/// Returns a fresh root context if the tracing key is absent.
pub(crate) fn extract_trace_context(
    metadata: &crate::metadata::Metadata,
) -> opentelemetry::Context {
    match metadata.sys_metadata.get(TRACING_METADATA_KEY) {
        Some(kvg) => {
            let extractor = MetadataExtractor(kvg);
            global::get_text_map_propagator(|prop| prop.extract(&extractor))
        }
        // No propagated trace: return a fresh root context rather than
        // `Context::current()`, which would inherit the ambient context of the
        // current task (e.g. a surrounding span) and graft this message onto
        // an unrelated parent.
        None => opentelemetry::Context::new(),
    }
}

/// Injects a specific OpenTelemetry [`Context`] into a named sys_metadata key.
///
/// Used to inject `vertex.process` context into `"tracing"` (preserving the root)
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
/// (`X-B3-TraceId`, `X-B3-SpanId`, `X-B3-Sampled`, `X-B3-Flags`). If B3 headers are found,
/// converts them to W3C `traceparent` format before extraction.
///
/// Returns a fresh root context if no trace headers are present.
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
        // B3 debug is carried in X-B3-Flags: 1 (not in X-B3-Sampled).
        // If present, it forces recording regardless of the sampled header.
        let debug = get_header_case_insensitive(headers, "X-B3-Flags") == Some("1");
        let traceparent = b3_to_traceparent(trace_id, span_id, sampled, debug);

        let mut synthetic = HashMap::new();
        synthetic.insert("traceparent".to_string(), traceparent);
        let extractor = HeaderExtractor(&synthetic);
        return global::get_text_map_propagator(|prop| prop.extract(&extractor));
    }

    // No upstream trace headers: fresh root context, not the ambient one.
    opentelemetry::Context::new()
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
/// - Maps sampled: "1"/"true" -> "01", "0"/"false"/absent -> "00"
/// - Maps debug (`X-B3-Flags: 1`) -> "01"
///
/// B3 treats an absent `X-B3-Sampled` header as "deferred" (downstream decides),
/// which has no equivalent in W3C's two-state trace-flags. We collapse absent to
/// `00` (not sampled) rather than `01` to avoid unexpectedly force-sampling every
/// B3-without-sampled trace and amplifying trace volume past the configured
/// sampler ratio.
fn b3_to_traceparent(trace_id: &str, span_id: &str, sampled: Option<&str>, debug: bool) -> String {
    let padded_trace_id = if trace_id.len() <= 16 {
        format!("{:0>32}", trace_id)
    } else {
        trace_id.to_string()
    };

    let flags = if debug {
        "01" // B3 debug forces recording
    } else {
        match sampled {
            Some("1" | "true") => "01",
            Some("0" | "false") => "00",
            _ => "00", // absent: B3 defers; don't force-sample
        }
    };

    format!("00-{}-{}-{}", padded_trace_id, span_id, flags)
}

// Attribute keys applied to every Numaflow-managed span (OTel messaging semantic
// conventions + Numaflow-specific). Values are set at each call site because
// `tracing::info_span!` requires static attribute expressions and the OTel SDK API
// (for batch/sink per-message spans) uses `KeyValue::new` at the call site.
pub const ATTR_MESSAGING_SYSTEM: &str = "messaging.system";
pub const ATTR_MESSAGING_OPERATION_NAME: &str = "messaging.operation.name";
pub const ATTR_MESSAGING_MESSAGE_ID: &str = "messaging.message.id";
pub const ATTR_NUMAFLOW_TOPOLOGY: &str = "numaflow.topology";
pub const ATTR_NUMAFLOW_PIPELINE_NAME: &str = "numaflow.pipeline.name";
pub const ATTR_NUMAFLOW_VERTEX_NAME: &str = "numaflow.vertex.name";

/// Topology attribute value attached to Numaflow-managed tracing spans.
///
/// This is intentionally separate from `CustomResourceType`: tracing only needs
/// the lightweight topology label, not the full resource configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TraceTopology {
    MonoVertex,
    #[allow(dead_code)]
    Pipeline,
}

impl TraceTopology {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MonoVertex => "monovertex",
            Self::Pipeline => "pipeline",
        }
    }

    pub(crate) fn root_operation_name(self) -> &'static str {
        match self {
            Self::MonoVertex => "monovertex.process",
            Self::Pipeline => "pipeline.process",
        }
    }
}

/// Builds the common OTel attributes used on Numaflow-managed spans.
pub(crate) fn build_platform_attributes(
    topology: TraceTopology,
    operation_name: &'static str,
    message_id: String,
) -> Vec<opentelemetry::KeyValue> {
    vec![
        opentelemetry::KeyValue::new(ATTR_MESSAGING_SYSTEM, "numaflow"),
        opentelemetry::KeyValue::new(ATTR_MESSAGING_OPERATION_NAME, operation_name),
        opentelemetry::KeyValue::new(ATTR_MESSAGING_MESSAGE_ID, message_id),
        opentelemetry::KeyValue::new(ATTR_NUMAFLOW_TOPOLOGY, topology.as_str()),
        opentelemetry::KeyValue::new(
            ATTR_NUMAFLOW_PIPELINE_NAME,
            crate::config::get_pipeline_name(),
        ),
        opentelemetry::KeyValue::new(ATTR_NUMAFLOW_VERTEX_NAME, crate::config::get_vertex_name()),
    ]
}

/// Returns the propagated parent context from metadata, or a fresh root if absent.
pub(crate) fn parent_context_from_metadata(metadata: Option<&Metadata>) -> opentelemetry::Context {
    metadata.map(extract_trace_context).unwrap_or_default()
}

/// RAII guard that ends all contained OTel spans when dropped.
pub(crate) struct ContextSpanGuard(Vec<opentelemetry::Context>);

impl ContextSpanGuard {
    pub(crate) fn new(contexts: Vec<opentelemetry::Context>) -> Self {
        Self(contexts)
    }
}

impl Drop for ContextSpanGuard {
    fn drop(&mut self) {
        for cx in self.0.drain(..) {
            cx.span().end();
        }
    }
}

/// Starts an OTel SDK child span with standard Numaflow attributes.
pub(crate) fn start_platform_child_span(
    span_name: &'static str,
    kind: opentelemetry::trace::SpanKind,
    parent_cx: &opentelemetry::Context,
    topology: TraceTopology,
    operation_name: &'static str,
    message_id: String,
) -> opentelemetry::Context {
    let tracer = opentelemetry::global::tracer("numaflow-core");
    let span = tracer
        .span_builder(span_name)
        .with_kind(kind)
        .with_attributes(build_platform_attributes(
            topology,
            operation_name,
            message_id,
        ))
        .start_with_context(&tracer, parent_cx);
    opentelemetry::Context::current().with_span(span)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use opentelemetry::propagation::{Extractor, Injector};
    use opentelemetry::trace::TraceContextExt;
    use std::sync::Once;

    /// Install the W3C propagator once per test process. Tests that rely on
    /// `extract`/`inject` going through the real propagator must call this.
    fn init_propagator() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            opentelemetry::global::set_text_map_propagator(
                opentelemetry_sdk::propagation::TraceContextPropagator::new(),
            );
        });
    }

    fn kvg_with(pairs: &[(&str, &str)]) -> KeyValueGroup {
        let mut kv = HashMap::new();
        for (k, v) in pairs {
            kv.insert((*k).to_string(), Bytes::from(v.as_bytes().to_vec()));
        }
        KeyValueGroup { key_value: kv }
    }

    fn context_from_traceparent(traceparent: &str) -> opentelemetry::Context {
        let carrier = kvg_with(&[("traceparent", traceparent)]);
        global::get_text_map_propagator(|prop| prop.extract(&MetadataExtractor(&carrier)))
    }

    #[test]
    fn metadata_extractor_reads_utf8_values() {
        let kvg = kvg_with(&[("traceparent", "abc"), ("tracestate", "k=v")]);
        let ex = MetadataExtractor(&kvg);
        assert_eq!(ex.get("traceparent"), Some("abc"));
        assert_eq!(ex.get("tracestate"), Some("k=v"));
        assert_eq!(ex.get("missing"), None);

        let mut keys = ex.keys();
        keys.sort();
        assert_eq!(keys, vec!["traceparent", "tracestate"]);
    }

    #[test]
    fn metadata_extractor_returns_none_for_non_utf8() {
        let mut kv = HashMap::new();
        kv.insert("traceparent".to_string(), Bytes::from(vec![0xff, 0xfe]));
        let kvg = KeyValueGroup { key_value: kv };
        assert_eq!(MetadataExtractor(&kvg).get("traceparent"), None);
    }

    #[test]
    fn metadata_injector_sets_value() {
        let mut kvg = KeyValueGroup {
            key_value: HashMap::new(),
        };
        MetadataInjector(&mut kvg).set("traceparent", "tp-value".to_string());
        assert_eq!(
            kvg.key_value.get("traceparent").map(|b| b.as_ref()),
            Some(b"tp-value".as_slice())
        );
    }

    #[test]
    fn extract_trace_context_absent_key_returns_fresh_root() {
        init_propagator();
        let ambient_cx =
            context_from_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        let ambient_sc = ambient_cx.span().span_context().clone();
        let _ambient_guard = ambient_cx.attach();

        let meta = crate::metadata::Metadata::default();
        let cx = extract_trace_context(&meta);
        let span = cx.span();
        let sc = span.span_context();

        // Fresh root: no valid remote span context and no inheritance from the
        // ambient test context.
        assert!(!sc.is_valid());
        assert_ne!(sc.trace_id(), ambient_sc.trace_id());
        assert_ne!(sc.span_id(), ambient_sc.span_id());
    }

    #[test]
    fn extract_trace_context_roundtrips_with_inject() {
        init_propagator();

        // Build a parent context from a known traceparent so we can assert on specific IDs.
        let parent_tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let parent_kvg = kvg_with(&[("traceparent", parent_tp)]);
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataExtractor(&parent_kvg)));
        let parent_sc = {
            let parent_span = parent_cx.span();
            parent_span.span_context().clone()
        };
        assert!(parent_sc.is_valid(), "parent must parse to valid context");

        // Inject into a fresh Metadata under the "tracing" key, then extract back.
        let mut meta = crate::metadata::Metadata::default();
        inject_context_into_metadata(&mut meta, TRACING_METADATA_KEY, &parent_cx);
        assert!(meta.sys_metadata.contains_key(TRACING_METADATA_KEY));

        let extracted_cx = extract_trace_context(&meta);
        let extracted_span = extracted_cx.span();
        let extracted_sc = extracted_span.span_context();
        assert!(extracted_sc.is_valid());
        assert_eq!(extracted_sc.trace_id(), parent_sc.trace_id());
        assert_eq!(extracted_sc.span_id(), parent_sc.span_id());
    }

    #[test]
    fn header_extractor_case_insensitive_fallback() {
        let mut h = HashMap::new();
        h.insert("X-B3-TraceId".to_string(), "abc".to_string());

        let ex = HeaderExtractor(&h);
        assert_eq!(ex.get("X-B3-TraceId"), Some("abc")); // exact match
        assert_eq!(ex.get("x-b3-traceid"), Some("abc")); // case-insensitive
        assert_eq!(ex.get("missing"), None);
    }

    #[test]
    fn extract_from_headers_no_trace_returns_fresh_root() {
        init_propagator();
        let ambient_cx =
            context_from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let ambient_sc = ambient_cx.span().span_context().clone();
        let _ambient_guard = ambient_cx.attach();

        let headers = Arc::new(HashMap::new());
        let cx = extract_trace_context_from_headers(&headers);
        let span = cx.span();
        let sc = span.span_context();

        assert!(!sc.is_valid());
        assert_ne!(sc.trace_id(), ambient_sc.trace_id());
        assert_ne!(sc.span_id(), ambient_sc.span_id());
    }

    #[test]
    fn extract_from_headers_w3c_traceparent() {
        init_propagator();
        let mut h = HashMap::new();
        h.insert(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );
        let headers = Arc::new(h);

        let cx = extract_trace_context_from_headers(&headers);
        let span = cx.span();
        let sc = span.span_context();
        assert!(sc.is_valid());
        assert_eq!(
            sc.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(sc.span_id().to_string(), "b7ad6b7169203331");
        assert!(sc.is_sampled());
    }

    #[test]
    fn extract_from_headers_b3_multi_header_sampled() {
        init_propagator();
        let mut h = HashMap::new();
        h.insert(
            "X-B3-TraceId".to_string(),
            "0af7651916cd43dd8448eb211c80319c".to_string(),
        );
        h.insert("X-B3-SpanId".to_string(), "b7ad6b7169203331".to_string());
        h.insert("X-B3-Sampled".to_string(), "1".to_string());
        let headers = Arc::new(h);

        let cx = extract_trace_context_from_headers(&headers);
        let span = cx.span();
        let sc = span.span_context();
        assert!(sc.is_valid());
        assert_eq!(
            sc.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(sc.span_id().to_string(), "b7ad6b7169203331");
        assert!(sc.is_sampled());
    }

    #[test]
    fn extract_from_headers_b3_debug_flag_forces_sampled() {
        init_propagator();
        let mut h = HashMap::new();
        h.insert(
            "X-B3-TraceId".to_string(),
            "0af7651916cd43dd8448eb211c80319c".to_string(),
        );
        h.insert("X-B3-SpanId".to_string(), "b7ad6b7169203331".to_string());
        h.insert("X-B3-Sampled".to_string(), "0".to_string());
        h.insert("X-B3-Flags".to_string(), "1".to_string());
        let headers = Arc::new(h);

        let cx = extract_trace_context_from_headers(&headers);
        let span = cx.span();
        let sc = span.span_context();
        assert!(sc.is_valid());
        assert_eq!(
            sc.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(sc.span_id().to_string(), "b7ad6b7169203331");
        assert!(sc.is_sampled());
    }

    #[test]
    fn extract_from_headers_b3_missing_span_id_falls_through() {
        init_propagator();
        let mut h = HashMap::new();
        h.insert("X-B3-TraceId".to_string(), "abc".to_string());
        // No X-B3-SpanId.
        let headers = Arc::new(h);

        let cx = extract_trace_context_from_headers(&headers);
        assert!(!cx.span().span_context().is_valid());
    }

    #[test]
    fn b3_pads_64bit_trace_id_to_128bit() {
        let tp = b3_to_traceparent("b7ad6b7169203331", "b7ad6b7169203331", Some("1"), false);
        assert_eq!(
            tp,
            "00-0000000000000000b7ad6b7169203331-b7ad6b7169203331-01"
        );
    }

    #[test]
    fn b3_preserves_128bit_trace_id() {
        let tp = b3_to_traceparent(
            "0af7651916cd43dd8448eb211c80319c",
            "b7ad6b7169203331",
            Some("1"),
            false,
        );
        assert_eq!(
            tp,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
    }

    #[test]
    fn b3_sampled_flag_mapping() {
        let tid = "0af7651916cd43dd8448eb211c80319c";
        let sid = "b7ad6b7169203331";

        // Sampled=1/true -> 01.
        assert!(b3_to_traceparent(tid, sid, Some("1"), false).ends_with("-01"));
        assert!(b3_to_traceparent(tid, sid, Some("true"), false).ends_with("-01"));

        // Sampled=0/false -> 00.
        assert!(b3_to_traceparent(tid, sid, Some("0"), false).ends_with("-00"));
        assert!(b3_to_traceparent(tid, sid, Some("false"), false).ends_with("-00"));

        // Absent -> 00 (B3 deferral collapsed to not-sampled to avoid volume blowup).
        assert!(b3_to_traceparent(tid, sid, None, false).ends_with("-00"));

        // Unknown value -> 00 (same default as absent).
        assert!(b3_to_traceparent(tid, sid, Some("weird"), false).ends_with("-00"));
    }

    #[test]
    fn b3_debug_flag_forces_sampled_regardless_of_sampled_header() {
        let tid = "0af7651916cd43dd8448eb211c80319c";
        let sid = "b7ad6b7169203331";

        // debug=true beats sampled=0/absent.
        assert!(b3_to_traceparent(tid, sid, Some("0"), true).ends_with("-01"));
        assert!(b3_to_traceparent(tid, sid, None, true).ends_with("-01"));
        // debug=false + sampled=1 still samples.
        assert!(b3_to_traceparent(tid, sid, Some("1"), false).ends_with("-01"));
    }
}
