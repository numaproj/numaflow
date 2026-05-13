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

use crate::message::Message;
use crate::metadata::{KeyValueGroup, Metadata};

/// Key under which W3C trace context is stored in `sys_metadata`.
/// Always holds the shared `vertex.process` parent context for downstream
/// platform spans (for example source, map, and sink spans).
pub const TRACING_METADATA_KEY: &str = "tracing";

/// Key under which the current stage's span context is stored for UDF consumption.
/// The UDF reads this key to see the current platform stage as its parent.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SinkStage {
    Primary,
    Fallback,
    OnSuccess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TraceStage {
    SourceDispatch,
    SourceTransform,
    Map,
    Sink(SinkStage),
}

pub(crate) struct SpanSpec {
    pub topology: TraceTopology,
    pub span_name: &'static str,
    pub operation_name: &'static str,
    pub kind: opentelemetry::trace::SpanKind,
}

impl From<(TraceTopology, TraceStage)> for SpanSpec {
    fn from((topology, stage): (TraceTopology, TraceStage)) -> Self {
        use opentelemetry::trace::SpanKind;
        match (topology, stage) {
            (TraceTopology::MonoVertex, TraceStage::SourceDispatch) => SpanSpec {
                topology,
                span_name: "numaflow.monovertex.source.dispatch",
                operation_name: "source.dispatch",
                kind: SpanKind::Consumer,
            },
            (TraceTopology::Pipeline, TraceStage::SourceDispatch) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.source.dispatch",
                operation_name: "source.dispatch",
                kind: SpanKind::Consumer,
            },
            (TraceTopology::MonoVertex, TraceStage::SourceTransform) => SpanSpec {
                topology,
                span_name: "numaflow.monovertex.source.transform",
                operation_name: "source.transform",
                kind: SpanKind::Client,
            },
            (TraceTopology::Pipeline, TraceStage::SourceTransform) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.source.transform",
                operation_name: "source.transform",
                kind: SpanKind::Client,
            },
            (TraceTopology::MonoVertex, TraceStage::Map) => SpanSpec {
                topology,
                span_name: "numaflow.monovertex.map",
                operation_name: "map",
                kind: SpanKind::Client,
            },
            (TraceTopology::Pipeline, TraceStage::Map) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.map",
                operation_name: "map",
                kind: SpanKind::Client,
            },
            (TraceTopology::MonoVertex, TraceStage::Sink(SinkStage::Primary)) => SpanSpec {
                topology,
                span_name: "numaflow.monovertex.sink.write",
                operation_name: "sink.write",
                kind: SpanKind::Client,
            },
            (TraceTopology::MonoVertex, TraceStage::Sink(SinkStage::Fallback)) => SpanSpec {
                topology,
                span_name: "numaflow.monovertex.sink.fallback",
                operation_name: "sink.fallback",
                kind: SpanKind::Client,
            },
            (TraceTopology::MonoVertex, TraceStage::Sink(SinkStage::OnSuccess)) => SpanSpec {
                topology,
                span_name: "numaflow.monovertex.sink.on_success",
                operation_name: "sink.on_success",
                kind: SpanKind::Client,
            },
            (TraceTopology::Pipeline, TraceStage::Sink(SinkStage::Primary)) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.sink.write",
                operation_name: "sink.write",
                kind: SpanKind::Client,
            },
            (TraceTopology::Pipeline, TraceStage::Sink(SinkStage::Fallback)) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.sink.fallback",
                operation_name: "sink.fallback",
                kind: SpanKind::Client,
            },
            (TraceTopology::Pipeline, TraceStage::Sink(SinkStage::OnSuccess)) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.sink.on_success",
                operation_name: "sink.on_success",
                kind: SpanKind::Client,
            },
        }
    }
}

pub(crate) fn start_child_span_from_spec(
    parent_cx: &opentelemetry::Context,
    message_id: String,
    spec: &SpanSpec,
) -> opentelemetry::Context {
    start_platform_child_span(
        spec.span_name,
        spec.kind.clone(),
        parent_cx,
        spec.topology,
        spec.operation_name,
        message_id,
    )
}

pub(crate) trait MessageTarget {
    fn message_mut(&mut self) -> &mut Message;
}

pub(crate) fn inject_stage_spans<T: MessageTarget>(
    targets: &mut [T],
    topology: TraceTopology,
    stage: TraceStage,
) -> ContextSpanGuard {
    let spec: SpanSpec = (topology, stage).into();
    let mut contexts = Vec::with_capacity(targets.len());
    for target in targets.iter_mut() {
        let message = target.message_mut();
        let parent_cx = parent_context_from_metadata(message.metadata.as_deref());
        let msg_id = message.offset.to_string();
        let cx = start_child_span_from_spec(&parent_cx, msg_id, &spec);
        message.inject_tracing_udf(&cx);
        contexts.push(cx);
    }
    ContextSpanGuard::new(contexts)
}

/// Injects the current `tracing` span context as the UDF parent for a message.
///
/// Used by unary and stream map tasks after their async work has been instrumented with the
/// map span. This keeps the ambient `Span::current()` lookup in the tracing helper module.
pub(crate) fn inject_current_span_as_udf_parent(message: &mut Message) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let cx = tracing::Span::current().context();
    message.inject_tracing_udf(&cx);
}

/// Builds the per-message `numaflow.{topology}.map` `tracing` span used by unary and stream
/// mappers. The returned span is a child of the parent context found in `sys_metadata["tracing"]`
/// (or a fresh root if absent). Returns `None` when tracing is disabled.
///
/// Callers wrap their UDF call in `.instrument(span)`. Topology selection (e.g. mono-vertex only,
/// until pipeline tracing lands) stays at the call site.
pub(crate) fn start_map_tracing_span(message: &Message, topology: TraceTopology) -> tracing::Span {
    let parent_cx = parent_context_from_metadata(message.metadata.as_deref());
    let msg_id = message.offset.to_string();
    let span = match topology {
        TraceTopology::MonoVertex => tracing::info_span!(
            "numaflow.monovertex.map",
            otel.kind = "CLIENT",
            { ATTR_MESSAGING_SYSTEM } = "numaflow",
            { ATTR_MESSAGING_OPERATION_NAME } = "map",
            { ATTR_MESSAGING_MESSAGE_ID } = %msg_id,
            { ATTR_NUMAFLOW_TOPOLOGY } = TraceTopology::MonoVertex.as_str(),
            { ATTR_NUMAFLOW_PIPELINE_NAME } = crate::config::get_pipeline_name(),
            { ATTR_NUMAFLOW_VERTEX_NAME } = crate::config::get_vertex_name(),
        ),
        TraceTopology::Pipeline => tracing::info_span!(
            "numaflow.pipeline.map",
            otel.kind = "CLIENT",
            { ATTR_MESSAGING_SYSTEM } = "numaflow",
            { ATTR_MESSAGING_OPERATION_NAME } = "map",
            { ATTR_MESSAGING_MESSAGE_ID } = %msg_id,
            { ATTR_NUMAFLOW_TOPOLOGY } = TraceTopology::Pipeline.as_str(),
            { ATTR_NUMAFLOW_PIPELINE_NAME } = crate::config::get_pipeline_name(),
            { ATTR_NUMAFLOW_VERTEX_NAME } = crate::config::get_vertex_name(),
        ),
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let _ = span.set_parent(parent_cx);
    span
}

/// Spans created for a single source message.
///
/// `platform_span` is the lifecycle root (`numaflow.vertex.process`) — the source loop hands it to
/// the message's `AckHandle` so it lives until ack.
/// `dispatch_cx` is the OTel SDK `numaflow.{topology}.source.dispatch` child; the source loop owns
/// its lifetime via `SourceDispatchSpans`.
pub(crate) struct SourceMessageSpans {
    pub platform_span: tracing::Span,
    pub dispatch_cx: opentelemetry::Context,
}

/// Builds the per-message platform.process and source.dispatch spans for a single source-read
/// message.
///
/// As a side-effect, this writes the `vertex.process` context into
/// `message.metadata.sys_metadata[TRACING_METADATA_KEY]` so downstream stages (map, sink) can
/// extract it as their parent. Without this side-effect, downstream spans would not be linked
/// to the platform root.
pub(crate) fn start_source_message_spans(
    message: &mut Message,
    topology: TraceTopology,
) -> SourceMessageSpans {
    let upstream_cx = extract_trace_context_from_headers(&message.headers);
    let msg_id = message.offset.to_string();
    let platform_span = match topology {
        TraceTopology::MonoVertex => tracing::info_span!(
            "numaflow.vertex.process",
            otel.kind = "INTERNAL",
            { ATTR_MESSAGING_SYSTEM } = "numaflow",
            { ATTR_MESSAGING_OPERATION_NAME } = TraceTopology::MonoVertex.root_operation_name(),
            { ATTR_MESSAGING_MESSAGE_ID } = %msg_id,
            { ATTR_NUMAFLOW_TOPOLOGY } = TraceTopology::MonoVertex.as_str(),
            { ATTR_NUMAFLOW_PIPELINE_NAME } = crate::config::get_pipeline_name(),
            { ATTR_NUMAFLOW_VERTEX_NAME } = crate::config::get_vertex_name(),
        ),
        TraceTopology::Pipeline => tracing::info_span!(
            "numaflow.vertex.process",
            otel.kind = "INTERNAL",
            { ATTR_MESSAGING_SYSTEM } = "numaflow",
            { ATTR_MESSAGING_OPERATION_NAME } = TraceTopology::Pipeline.root_operation_name(),
            { ATTR_MESSAGING_MESSAGE_ID } = %msg_id,
            { ATTR_NUMAFLOW_TOPOLOGY } = TraceTopology::Pipeline.as_str(),
            { ATTR_NUMAFLOW_PIPELINE_NAME } = crate::config::get_pipeline_name(),
            { ATTR_NUMAFLOW_VERTEX_NAME } = crate::config::get_vertex_name(),
        ),
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let _ = platform_span.set_parent(upstream_cx);
    let platform_cx = platform_span.context();

    // Inject vertex.process context into sys_metadata["tracing"] so map/sink see it as parent.
    let metadata = message
        .metadata
        .get_or_insert_with(|| Arc::new(Metadata::default()));
    inject_context_into_metadata(Arc::make_mut(metadata), TRACING_METADATA_KEY, &platform_cx);

    let dispatch_spec: SpanSpec = (topology, TraceStage::SourceDispatch).into();
    let dispatch_cx = start_child_span_from_spec(&platform_cx, msg_id, &dispatch_spec);

    SourceMessageSpans {
        platform_span,
        dispatch_cx,
    }
}

/// Tracks per-message source dispatch OTel spans keyed by message offset.
///
/// The source creates a topology-specific dispatch span per input message before
/// tracker insert, transform, and downstream send. On the success path, each span
/// is ended either when the last downstream message for that input offset is
/// bypassed/sent, or immediately after transform if that input produced no outputs.
///
/// Any spans that remain in the map at end-of-iteration (for example, due to a
/// transformer error that breaks the outer loop before all messages are dispatched)
/// are closed by the RAII `Drop` impl, ensuring no span is leaked.
pub(crate) struct SourceDispatchSpans {
    spans: HashMap<crate::message::Offset, opentelemetry::Context>,
}

impl SourceDispatchSpans {
    pub(crate) fn new() -> Self {
        Self {
            spans: HashMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, offset: crate::message::Offset, cx: opentelemetry::Context) {
        self.spans.insert(offset, cx);
    }

    pub(crate) fn end_without_outputs(
        &mut self,
        output_counts: &HashMap<crate::message::Offset, usize>,
    ) {
        let offsets_without_outputs: Vec<_> = self
            .spans
            .keys()
            .filter(|offset| !output_counts.contains_key(*offset))
            .cloned()
            .collect();

        for offset in offsets_without_outputs {
            self.end(&offset);
        }
    }

    pub(crate) fn end(&mut self, offset: &crate::message::Offset) {
        if let Some(cx) = self.spans.remove(offset) {
            cx.span().end();
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, offset: &crate::message::Offset) -> bool {
        self.spans.contains_key(offset)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }
}

impl Drop for SourceDispatchSpans {
    fn drop(&mut self) {
        for (_, cx) in self.spans.drain() {
            cx.span().end();
        }
    }
}

/// RAII guard for the per-input `source.transform` span.
///
/// The span is a child of the input message's `source.dispatch` span and measures the transformer
/// UDF round-trip for that specific source message, not the whole batch. It closes on success,
/// error, or cancellation.
pub(crate) struct SourceTransformSpan(Option<opentelemetry::Context>);

impl SourceTransformSpan {
    pub(crate) fn new(parent_cx: Option<opentelemetry::Context>, msg_id: String) -> Self {
        let Some(parent_cx) = parent_cx else {
            return Self(None);
        };

        let spec: SpanSpec = (TraceTopology::MonoVertex, TraceStage::SourceTransform).into();
        Self(Some(start_child_span_from_spec(&parent_cx, msg_id, &spec)))
    }

    pub(crate) fn record_output_count(&self, output_count: usize) {
        if let Some(cx) = &self.0 {
            cx.span().set_attribute(opentelemetry::KeyValue::new(
                "numaflow.source.transform.output_count",
                output_count as i64,
            ));
        }
    }

    #[cfg(test)]
    pub(crate) fn is_active(&self) -> bool {
        self.0.is_some()
    }
}

impl Drop for SourceTransformSpan {
    fn drop(&mut self) {
        if let Some(cx) = self.0.take() {
            cx.span().end();
        }
    }
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
    fn build_platform_attributes_contains_common_keys() {
        let attrs =
            build_platform_attributes(TraceTopology::MonoVertex, "map", "msg-1".to_string());
        let values: HashMap<_, _> = attrs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.to_string()))
            .collect();

        assert_eq!(
            values.get(ATTR_MESSAGING_SYSTEM).map(String::as_str),
            Some("numaflow")
        );
        assert_eq!(
            values
                .get(ATTR_MESSAGING_OPERATION_NAME)
                .map(String::as_str),
            Some("map")
        );
        assert_eq!(
            values.get(ATTR_MESSAGING_MESSAGE_ID).map(String::as_str),
            Some("msg-1")
        );
        assert_eq!(
            values.get(ATTR_NUMAFLOW_TOPOLOGY).map(String::as_str),
            Some("monovertex")
        );
        assert!(values.contains_key(ATTR_NUMAFLOW_PIPELINE_NAME));
        assert!(values.contains_key(ATTR_NUMAFLOW_VERTEX_NAME));
    }

    #[test]
    fn context_span_guard_drop_is_safe_for_empty_and_root_contexts() {
        ContextSpanGuard::new(Vec::new());
        ContextSpanGuard::new(vec![opentelemetry::Context::new()]);
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

    mod span_spec_tests {
        use super::*;
        use opentelemetry::trace::SpanKind;

        fn spec(topology: TraceTopology, stage: TraceStage) -> SpanSpec {
            (topology, stage).into()
        }

        #[test]
        fn monovertex_source_dispatch() {
            let s = spec(TraceTopology::MonoVertex, TraceStage::SourceDispatch);
            assert_eq!(s.span_name, "numaflow.monovertex.source.dispatch");
            assert_eq!(s.operation_name, "source.dispatch");
            assert!(matches!(s.kind, SpanKind::Consumer));
            assert!(matches!(s.topology, TraceTopology::MonoVertex));
        }

        #[test]
        fn pipeline_source_dispatch() {
            let s = spec(TraceTopology::Pipeline, TraceStage::SourceDispatch);
            assert_eq!(s.span_name, "numaflow.pipeline.source.dispatch");
            assert_eq!(s.operation_name, "source.dispatch");
            assert!(matches!(s.kind, SpanKind::Consumer));
            assert!(matches!(s.topology, TraceTopology::Pipeline));
        }

        #[test]
        fn monovertex_source_transform() {
            let s = spec(TraceTopology::MonoVertex, TraceStage::SourceTransform);
            assert_eq!(s.span_name, "numaflow.monovertex.source.transform");
            assert_eq!(s.operation_name, "source.transform");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn pipeline_source_transform() {
            let s = spec(TraceTopology::Pipeline, TraceStage::SourceTransform);
            assert_eq!(s.span_name, "numaflow.pipeline.source.transform");
            assert_eq!(s.operation_name, "source.transform");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn monovertex_map() {
            let s = spec(TraceTopology::MonoVertex, TraceStage::Map);
            assert_eq!(s.span_name, "numaflow.monovertex.map");
            assert_eq!(s.operation_name, "map");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn pipeline_map() {
            let s = spec(TraceTopology::Pipeline, TraceStage::Map);
            assert_eq!(s.span_name, "numaflow.pipeline.map");
            assert_eq!(s.operation_name, "map");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn monovertex_sink_primary() {
            let s = spec(
                TraceTopology::MonoVertex,
                TraceStage::Sink(SinkStage::Primary),
            );
            assert_eq!(s.span_name, "numaflow.monovertex.sink.write");
            assert_eq!(s.operation_name, "sink.write");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn monovertex_sink_fallback() {
            let s = spec(
                TraceTopology::MonoVertex,
                TraceStage::Sink(SinkStage::Fallback),
            );
            assert_eq!(s.span_name, "numaflow.monovertex.sink.fallback");
            assert_eq!(s.operation_name, "sink.fallback");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn monovertex_sink_on_success() {
            let s = spec(
                TraceTopology::MonoVertex,
                TraceStage::Sink(SinkStage::OnSuccess),
            );
            assert_eq!(s.span_name, "numaflow.monovertex.sink.on_success");
            assert_eq!(s.operation_name, "sink.on_success");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn pipeline_sink_primary() {
            let s = spec(
                TraceTopology::Pipeline,
                TraceStage::Sink(SinkStage::Primary),
            );
            assert_eq!(s.span_name, "numaflow.pipeline.sink.write");
            assert_eq!(s.operation_name, "sink.write");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn pipeline_sink_fallback() {
            let s = spec(
                TraceTopology::Pipeline,
                TraceStage::Sink(SinkStage::Fallback),
            );
            assert_eq!(s.span_name, "numaflow.pipeline.sink.fallback");
            assert_eq!(s.operation_name, "sink.fallback");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn pipeline_sink_on_success() {
            let s = spec(
                TraceTopology::Pipeline,
                TraceStage::Sink(SinkStage::OnSuccess),
            );
            assert_eq!(s.span_name, "numaflow.pipeline.sink.on_success");
            assert_eq!(s.operation_name, "sink.on_success");
            assert!(matches!(s.kind, SpanKind::Client));
        }

        #[test]
        fn operation_name_is_topology_independent() {
            let stages = [
                TraceStage::SourceDispatch,
                TraceStage::SourceTransform,
                TraceStage::Map,
                TraceStage::Sink(SinkStage::Primary),
                TraceStage::Sink(SinkStage::Fallback),
                TraceStage::Sink(SinkStage::OnSuccess),
            ];
            for stage in stages {
                let mv = spec(TraceTopology::MonoVertex, stage);
                let pl = spec(TraceTopology::Pipeline, stage);
                assert_eq!(
                    mv.operation_name, pl.operation_name,
                    "operation_name should be the same across topologies for {:?}",
                    mv.operation_name
                );
            }
        }

        #[test]
        fn span_names_contain_topology_prefix() {
            let stages = [
                TraceStage::SourceDispatch,
                TraceStage::SourceTransform,
                TraceStage::Map,
                TraceStage::Sink(SinkStage::Primary),
                TraceStage::Sink(SinkStage::Fallback),
                TraceStage::Sink(SinkStage::OnSuccess),
            ];
            for stage in stages {
                let mv = spec(TraceTopology::MonoVertex, stage);
                let pl = spec(TraceTopology::Pipeline, stage);
                assert!(
                    mv.span_name.contains("monovertex"),
                    "MonoVertex span_name should contain 'monovertex': {}",
                    mv.span_name
                );
                assert!(
                    pl.span_name.contains("pipeline"),
                    "Pipeline span_name should contain 'pipeline': {}",
                    pl.span_name
                );
            }
        }
    }

    mod start_map_tracing_span_tests {
        use super::*;

        #[test]
        fn always_returns_span_with_topology_specific_name() {
            let msg = Message::default();

            let mv = start_map_tracing_span(&msg, TraceTopology::MonoVertex);
            assert_eq!(
                mv.metadata().map(|m| m.name()),
                Some("numaflow.monovertex.map")
            );

            let pl = start_map_tracing_span(&msg, TraceTopology::Pipeline);
            assert_eq!(
                pl.metadata().map(|m| m.name()),
                Some("numaflow.pipeline.map")
            );
        }
    }

    mod start_source_message_spans_tests {
        use super::*;

        #[test]
        fn always_injects_tracing_metadata_key() {
            let mut msg = Message::default();

            let result = start_source_message_spans(&mut msg, TraceTopology::MonoVertex);

            let metadata = msg.metadata.as_ref().expect("metadata should be created");
            assert!(
                metadata.sys_metadata.contains_key(TRACING_METADATA_KEY),
                "TRACING_METADATA_KEY must be written so downstream stages see vertex.process \
                 as parent"
            );

            // Drop the result explicitly to verify Drop on dispatch_cx doesn't panic.
            drop(result);
        }

        #[test]
        fn pipeline_topology_also_writes_tracing_metadata_key() {
            let mut msg = Message::default();

            let result = start_source_message_spans(&mut msg, TraceTopology::Pipeline);

            let metadata = msg.metadata.as_ref().expect("metadata should be created");
            assert!(metadata.sys_metadata.contains_key(TRACING_METADATA_KEY));

            drop(result);
        }
    }
}
