//! OpenTelemetry propagation helpers for Numaflow message metadata.
//!
//! Adapts `KeyValueGroup` (sys_metadata) as a carrier for W3C Trace Context.
//! The propagator uses these getter/setter implementations to read and write
//! `traceparent` and `tracestate` without knowing about our protobuf types.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{Span as _, TraceContextExt as _, Tracer};

use crate::message::Message;
use crate::metadata::{KeyValueGroup, Metadata};

static TRACER: OnceLock<opentelemetry::global::BoxedTracer> = OnceLock::new();
static PLATFORM_SPANS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Eagerly populates the cached `BoxedTracer` from the global provider. Call this from the
/// binary's tracing setup *after* `set_tracer_provider` so the cache binds to the real
/// provider rather than the noop one.
///
/// Lazy fallback in `get_tracer` still works if this is never called — but `OnceLock` is
/// write-once, so whichever caller wins the race fixes the tracer for the process lifetime.
/// Calling this from the deterministic init path removes that race.
pub fn init_tracer() {
    let _ = TRACER.get_or_init(|| global::tracer("numaflow-core"));
}

fn get_tracer() -> &'static opentelemetry::global::BoxedTracer {
    TRACER.get_or_init(|| global::tracer("numaflow-core"))
}

pub(crate) fn platform_spans_enabled() -> bool {
    *PLATFORM_SPANS_ENABLED.get_or_init(|| {
        platform_spans_enabled_from_env(
            std::env::var("OTEL_SDK_DISABLED").ok().as_deref(),
            std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                .ok()
                .as_deref(),
            std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().as_deref(),
        )
    })
}

fn platform_spans_enabled_from_env(
    otel_sdk_disabled: Option<&str>,
    traces_endpoint: Option<&str>,
    endpoint: Option<&str>,
) -> bool {
    if otel_sdk_disabled.is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return false;
    }

    traces_endpoint
        .or(endpoint)
        .is_some_and(|endpoint| !endpoint.trim().is_empty())
}

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
    Pipeline,
}

impl TraceTopology {
    /// Picks the topology label from runtime config.
    pub(crate) fn current() -> Self {
        if crate::config::is_mono_vertex() {
            Self::MonoVertex
        } else {
            Self::Pipeline
        }
    }

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

/// RAII handle for an OTel SDK span. Ends the underlying span when dropped.
pub(crate) struct StageSpan(opentelemetry::Context);

impl StageSpan {
    /// Wraps an existing OTel context as a RAII guard that ends the span on drop.
    /// Use when the span was started by a helper that returned a bare `Context`
    /// (for example, [`start_vertex_process_from_isb`]) and the caller wants to
    /// scope its lifetime to a block.
    pub(crate) fn from_context(cx: opentelemetry::Context) -> Self {
        Self(cx)
    }
}

impl Drop for StageSpan {
    fn drop(&mut self) {
        self.0.span().end();
    }
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
    /// ISB producer span emitted just before publishing a message to JetStream.
    IsbWrite,
    /// ISB consumer span emitted when a downstream vertex pod dequeues a message
    /// from JetStream and hands it off to its forwarder.
    IsbRead,
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
            (TraceTopology::Pipeline, TraceStage::IsbWrite) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.isb.write",
                operation_name: "isb.write",
                kind: SpanKind::Producer,
            },
            (TraceTopology::Pipeline, TraceStage::IsbRead) => SpanSpec {
                topology,
                span_name: "numaflow.pipeline.isb.read",
                operation_name: "isb.read",
                kind: SpanKind::Consumer,
            },
            // MonoVertex has no ISB hop; these combinations are never constructed.
            (TraceTopology::MonoVertex, TraceStage::IsbWrite | TraceStage::IsbRead) => {
                unreachable!("ISB spans are only emitted in Pipeline topology")
            }
        }
    }
}

/// Starts an OTel SDK child span with standard Numaflow attributes.
///
/// When no real tracer provider is registered (no `OTEL_EXPORTER_OTLP_ENDPOINT`),
/// the global tracer is a noop and the returned span is not recording — we skip
/// the per-message attribute allocation in that case.
pub(crate) fn start_child_span_from_spec(
    parent_cx: &opentelemetry::Context,
    message_id: String,
    spec: &SpanSpec,
) -> opentelemetry::Context {
    if !platform_spans_enabled() {
        return opentelemetry::Context::new();
    }

    start_child_span_from_spec_enabled(parent_cx, message_id, spec)
}

fn start_child_span_from_spec_enabled(
    parent_cx: &opentelemetry::Context,
    message_id: String,
    spec: &SpanSpec,
) -> opentelemetry::Context {
    let tracer = get_tracer();
    let mut span = tracer
        .span_builder(spec.span_name)
        .with_kind(spec.kind.clone())
        .start_with_context(tracer, parent_cx);
    if span.is_recording() {
        span.set_attributes(build_platform_attributes(
            spec.topology,
            spec.operation_name,
            message_id,
        ));
    }
    opentelemetry::Context::current().with_span(span)
}

/// Starts a per-message stage span as a child of the message's `vertex.process` parent and
/// injects the new span context into `sys_metadata["tracing_udf"]` so the UDF sees it as parent.
///
/// Returns a `StageSpan` whose `Drop` ends the underlying span. Callers keep it alive across
/// the work they want to measure — typically by collecting into a `Vec<StageSpan>` scoped to the
/// batch UDF call.
///
/// When no OTel layer is registered, the created span is not recording and we skip the
/// metadata copy-on-write — the caller can invoke this unconditionally.
pub(crate) fn inject_stage_span(
    message: &mut Message,
    topology: TraceTopology,
    stage: TraceStage,
) -> StageSpan {
    if !platform_spans_enabled() {
        return StageSpan(opentelemetry::Context::new());
    }

    inject_stage_span_enabled(message, topology, stage)
}

pub(crate) fn inject_stage_span_enabled(
    message: &mut Message,
    topology: TraceTopology,
    stage: TraceStage,
) -> StageSpan {
    let spec: SpanSpec = (topology, stage).into();
    let parent_cx = parent_context_from_metadata(message.metadata.as_deref());
    let msg_id = message.id.to_string();
    let cx = start_child_span_from_spec_enabled(&parent_cx, msg_id, &spec);
    if cx.span().is_recording() {
        message.inject_tracing_udf(&cx);
    }
    StageSpan(cx)
}

/// Collects per-message [`StageSpan`]s from an iterator of `&mut Message`.
///
/// Expands to a `Vec<StageSpan>` whose drop ends every span together at end of scope.
/// Use this in stage entry points to keep the call site to one line.
macro_rules! inject_stage_spans {
    (enabled, $messages:expr, $topology:expr, $stage:expr $(,)?) => {{
        $messages
            .map(|m| $crate::shared::otel::inject_stage_span_enabled(m, $topology, $stage))
            .collect::<Vec<$crate::shared::otel::StageSpan>>()
    }};
    ($messages:expr, $topology:expr, $stage:expr $(,)?) => {{
        if !$crate::shared::otel::platform_spans_enabled() {
            Vec::<$crate::shared::otel::StageSpan>::new()
        } else {
            $crate::shared::otel::inject_stage_spans!(enabled, $messages, $topology, $stage)
        }
    }};
}

pub(crate) use inject_stage_spans;

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
///
/// When no OTel layer is registered, the returned `platform_span` is disabled, the
/// `dispatch_cx` wraps a noop span, and the metadata side-effect is skipped — the call is
/// effectively free for callers to make unconditionally.
pub(crate) fn start_source_message_spans(
    message: &mut Message,
    topology: TraceTopology,
) -> SourceMessageSpans {
    if !platform_spans_enabled() {
        return disabled_source_message_spans();
    }

    start_source_message_spans_enabled(message, topology)
}

fn disabled_source_message_spans() -> SourceMessageSpans {
    SourceMessageSpans {
        platform_span: tracing::Span::none(),
        dispatch_cx: opentelemetry::Context::new(),
    }
}

fn start_source_message_spans_enabled(
    message: &mut Message,
    topology: TraceTopology,
) -> SourceMessageSpans {
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

    // No OTel layer registered: hand back a disabled span and a noop dispatch context.
    // Skip the header extraction and the sys_metadata write — neither would carry a real
    // span context, and downstream stages already handle a missing `TRACING_METADATA_KEY`
    // by treating it as a fresh root.
    if platform_span.is_disabled() {
        let dispatch_spec: SpanSpec = (topology, TraceStage::SourceDispatch).into();
        let dispatch_cx = start_child_span_from_spec_enabled(
            &opentelemetry::Context::new(),
            msg_id,
            &dispatch_spec,
        );
        return SourceMessageSpans {
            platform_span,
            dispatch_cx,
        };
    }

    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let upstream_cx = extract_trace_context_from_headers(&message.headers);
    let _ = platform_span.set_parent(upstream_cx);
    let platform_cx = platform_span.context();

    // Inject vertex.process context into sys_metadata["tracing"] so map/sink see it as parent.
    let metadata = message
        .metadata
        .get_or_insert_with(|| Arc::new(Metadata::default()));
    inject_context_into_metadata(Arc::make_mut(metadata), TRACING_METADATA_KEY, &platform_cx);

    let dispatch_spec: SpanSpec = (topology, TraceStage::SourceDispatch).into();
    let dispatch_cx = start_child_span_from_spec_enabled(&platform_cx, msg_id, &dispatch_spec);

    SourceMessageSpans {
        platform_span,
        dispatch_cx,
    }
}

/// Spans created for a single message received from the ISB on a downstream vertex pod.
///
/// `platform_span` is the lifecycle root (`numaflow.vertex.process`) for this pod; the ISB
/// reader hands it to the message's `AckHandle` so it lives until ack.
/// `isb_read_cx` is the short-lived `numaflow.pipeline.isb.read` span covering the per-message
/// handoff from the broker into the forwarder pipeline.
pub(crate) struct IsbReadSpans {
    pub platform_span: tracing::Span,
    pub isb_read_cx: opentelemetry::Context,
}

/// Builds the per-message `vertex.process` and `isb.read` spans for a message dequeued from
/// the ISB on a downstream (non-source) pipeline vertex pod.
///
/// As a side-effect, this **overwrites** `sys_metadata[TRACING_METADATA_KEY]` with the new
/// `vertex.process` context. The upstream pod wrote its own `vertex.process` context there
/// before publishing, which we use as the parent — but downstream stage spans (map/sink) on
/// this pod must be children of *this* pod's `vertex.process`, not the upstream's.
///
/// When no OTel layer is registered, the returned `platform_span` is disabled, the
/// `isb_read_cx` wraps a noop span, and the metadata side-effect is skipped — the call is
/// effectively free for callers to make unconditionally.
pub(crate) fn start_vertex_process_from_isb(message: &mut Message) -> IsbReadSpans {
    if !platform_spans_enabled() {
        return disabled_isb_read_spans();
    }

    start_vertex_process_from_isb_enabled(message)
}

fn disabled_isb_read_spans() -> IsbReadSpans {
    IsbReadSpans {
        platform_span: tracing::Span::none(),
        isb_read_cx: opentelemetry::Context::new(),
    }
}

fn start_vertex_process_from_isb_enabled(message: &mut Message) -> IsbReadSpans {
    let msg_id = message.id.to_string();
    let platform_span = tracing::info_span!(
        "numaflow.vertex.process",
        otel.kind = "INTERNAL",
        { ATTR_MESSAGING_SYSTEM } = "numaflow",
        { ATTR_MESSAGING_OPERATION_NAME } = TraceTopology::Pipeline.root_operation_name(),
        { ATTR_MESSAGING_MESSAGE_ID } = %msg_id,
        { ATTR_NUMAFLOW_TOPOLOGY } = TraceTopology::Pipeline.as_str(),
        { ATTR_NUMAFLOW_PIPELINE_NAME } = crate::config::get_pipeline_name(),
        { ATTR_NUMAFLOW_VERTEX_NAME } = crate::config::get_vertex_name(),
    );

    let isb_read_spec: SpanSpec = (TraceTopology::Pipeline, TraceStage::IsbRead).into();

    // No OTel layer registered: skip the parent extraction and metadata overwrite — neither
    // would carry a real span context. The returned isb.read context is a noop child.
    if platform_span.is_disabled() {
        let isb_read_cx = start_child_span_from_spec_enabled(
            &opentelemetry::Context::new(),
            msg_id,
            &isb_read_spec,
        );
        return IsbReadSpans {
            platform_span,
            isb_read_cx,
        };
    }

    use tracing_opentelemetry::OpenTelemetrySpanExt;
    // Upstream vertex wrote its vertex.process context here before publishing.
    let upstream_cx = parent_context_from_metadata(message.metadata.as_deref());
    let _ = platform_span.set_parent(upstream_cx);
    let platform_cx = platform_span.context();

    // Overwrite sys_metadata["tracing"] with this pod's vertex.process context so map/sink
    // stage spans on this pod become children of the local vertex.process, not the upstream's.
    let metadata = message
        .metadata
        .get_or_insert_with(|| Arc::new(Metadata::default()));
    inject_context_into_metadata(Arc::make_mut(metadata), TRACING_METADATA_KEY, &platform_cx);

    let isb_read_cx = start_child_span_from_spec_enabled(&platform_cx, msg_id, &isb_read_spec);

    IsbReadSpans {
        platform_span,
        isb_read_cx,
    }
}

/// Starts an `isb.write` Producer span as a child of the writer's current `vertex.process`
/// context.
///
/// The returned [`StageSpan`] ends the underlying span on drop. Callers hold it across the
/// JetStream publish call to measure broker publish latency. Unlike
/// [`start_vertex_process_from_isb`], this helper does **not** overwrite
/// `sys_metadata[TRACING_METADATA_KEY]` — the writer's `vertex.process` stays in metadata so
/// the next pod's `start_vertex_process_from_isb` parents to it (not to the producer span).
///
/// `message_id` lets callers that have already formatted the broker dedup id (e.g.
/// `async_write` which needs it for JetStream's `message_id` header) pass it in to avoid a
/// second `to_string()` on the hot path. Pass `None` if no formatted id is already in hand.
///
/// When platform spans are disabled, returns before extracting parent context or formatting
/// the span message ID — the call is effectively free.
pub(crate) fn start_isb_write_span(message: &Message, message_id: Option<&str>) -> StageSpan {
    if !platform_spans_enabled() {
        return StageSpan(opentelemetry::Context::new());
    }

    start_isb_write_span_enabled(message, message_id)
}

fn start_isb_write_span_enabled(message: &Message, message_id: Option<&str>) -> StageSpan {
    let parent_cx = parent_context_from_metadata(message.metadata.as_deref());
    let message_id = message_id
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| message.id.to_string());
    let spec: SpanSpec = (TraceTopology::Pipeline, TraceStage::IsbWrite).into();
    StageSpan(start_child_span_from_spec_enabled(
        &parent_cx, message_id, &spec,
    ))
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
        if !platform_spans_enabled() {
            return;
        }

        self.insert_enabled(offset, cx);
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(
        &mut self,
        offset: crate::message::Offset,
        cx: opentelemetry::Context,
    ) {
        self.insert_enabled(offset, cx);
    }

    fn insert_enabled(&mut self, offset: crate::message::Offset, cx: opentelemetry::Context) {
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
///
/// `None` only when the caller had no parent context to start with (e.g., the transformer ran
/// without a preceding source.dispatch span). When no OTel layer is registered, this still wraps
/// a noop context whose `Drop` is a no-op.
pub(crate) struct SourceTransformSpan(Option<opentelemetry::Context>);

impl SourceTransformSpan {
    pub(crate) fn none() -> Self {
        Self(None)
    }

    pub(crate) fn new(
        parent_cx: Option<opentelemetry::Context>,
        msg_id: String,
        topology: TraceTopology,
    ) -> Self {
        if !platform_spans_enabled() {
            return Self(None);
        }

        let Some(parent_cx) = parent_cx else {
            return Self(None);
        };

        let spec: SpanSpec = (topology, TraceStage::SourceTransform).into();
        Self(Some(start_child_span_from_spec_enabled(
            &parent_cx, msg_id, &spec,
        )))
    }

    pub(crate) fn record_output_count(&self, output_count: usize) {
        if let Some(cx) = &self.0
            && cx.span().is_recording()
        {
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
    use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
    use std::sync::Once;
    use tracing_subscriber::layer::SubscriberExt as _;

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

    fn with_otel_test_subscriber<T>(f: impl FnOnce() -> T) -> T {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("numaflow-core-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::with_default(subscriber, f)
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
    fn stage_span_drop_is_safe_for_root_context() {
        let _ = StageSpan(opentelemetry::Context::new());
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
    fn platform_spans_enabled_from_env_uses_otel_configuration() {
        assert!(!platform_spans_enabled_from_env(None, None, None));
        assert!(!platform_spans_enabled_from_env(
            Some("true"),
            Some("http://collector:4317"),
            None
        ));
        assert!(platform_spans_enabled_from_env(
            None,
            Some("http://collector:4317"),
            None
        ));
        assert!(platform_spans_enabled_from_env(
            None,
            None,
            Some("http://collector:4317")
        ));
        assert!(!platform_spans_enabled_from_env(None, Some("  "), None));
    }

    mod span_spec_tests {
        use super::*;
        use opentelemetry::trace::SpanKind;

        fn spec(topology: TraceTopology, stage: TraceStage) -> SpanSpec {
            (topology, stage).into()
        }

        /// Drives every `(topology, stage) -> SpanSpec` arm in one place. Each row
        /// asserts the three fields the production code reads (`span_name`,
        /// `operation_name`, `kind`).
        #[test]
        fn span_spec_from_topology_and_stage_covers_all_arms() {
            let cases: &[(TraceTopology, TraceStage, &str, &str, SpanKind)] = &[
                (
                    TraceTopology::MonoVertex,
                    TraceStage::SourceDispatch,
                    "numaflow.monovertex.source.dispatch",
                    "source.dispatch",
                    SpanKind::Consumer,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::SourceDispatch,
                    "numaflow.pipeline.source.dispatch",
                    "source.dispatch",
                    SpanKind::Consumer,
                ),
                (
                    TraceTopology::MonoVertex,
                    TraceStage::SourceTransform,
                    "numaflow.monovertex.source.transform",
                    "source.transform",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::SourceTransform,
                    "numaflow.pipeline.source.transform",
                    "source.transform",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::MonoVertex,
                    TraceStage::Map,
                    "numaflow.monovertex.map",
                    "map",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::Map,
                    "numaflow.pipeline.map",
                    "map",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::MonoVertex,
                    TraceStage::Sink(SinkStage::Primary),
                    "numaflow.monovertex.sink.write",
                    "sink.write",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::MonoVertex,
                    TraceStage::Sink(SinkStage::Fallback),
                    "numaflow.monovertex.sink.fallback",
                    "sink.fallback",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::MonoVertex,
                    TraceStage::Sink(SinkStage::OnSuccess),
                    "numaflow.monovertex.sink.on_success",
                    "sink.on_success",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::Sink(SinkStage::Primary),
                    "numaflow.pipeline.sink.write",
                    "sink.write",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::Sink(SinkStage::Fallback),
                    "numaflow.pipeline.sink.fallback",
                    "sink.fallback",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::Sink(SinkStage::OnSuccess),
                    "numaflow.pipeline.sink.on_success",
                    "sink.on_success",
                    SpanKind::Client,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::IsbWrite,
                    "numaflow.pipeline.isb.write",
                    "isb.write",
                    SpanKind::Producer,
                ),
                (
                    TraceTopology::Pipeline,
                    TraceStage::IsbRead,
                    "numaflow.pipeline.isb.read",
                    "isb.read",
                    SpanKind::Consumer,
                ),
            ];

            for (topology, stage, span_name, operation_name, kind) in cases {
                let s = spec(*topology, *stage);
                assert_eq!(s.span_name, *span_name, "span_name for {stage:?}");
                assert_eq!(
                    s.operation_name, *operation_name,
                    "operation_name for {stage:?}"
                );
                assert_eq!(
                    std::mem::discriminant(&s.kind),
                    std::mem::discriminant(kind),
                    "kind for {stage:?}"
                );
            }
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

    /// Tests run without an OTel layer registered, so the platform span is disabled
    /// and the sys_metadata write is skipped. The returned span still carries the
    /// right name so callers can `.instrument(...)` it unconditionally.
    #[test]
    fn start_source_message_spans_skips_work_when_platform_spans_disabled() {
        let mut msg = Message::default();
        assert!(msg.metadata.is_none());

        let result = start_source_message_spans(&mut msg, TraceTopology::MonoVertex);
        assert!(result.platform_span.is_disabled());
        assert!(msg.metadata.is_none());
        drop(result); // Drop must not panic on the noop dispatch_cx.
    }

    /// Without an OTel layer registered the helper must not allocate and must not
    /// mutate sys_metadata. The disabled early-return covers that.
    #[test]
    fn start_vertex_process_from_isb_skips_work_when_platform_spans_disabled() {
        let mut msg = Message::default();
        assert!(msg.metadata.is_none());

        let result = start_vertex_process_from_isb(&mut msg);
        assert!(result.platform_span.is_disabled());
        assert!(
            msg.metadata.is_none(),
            "sys_metadata must not be touched on the disabled path"
        );
        drop(result); // Drop must not panic on the noop isb_read_cx.
    }

    #[test]
    fn start_vertex_process_from_isb_enabled_overwrites_tracing_metadata() {
        init_propagator();
        let upstream_cx =
            context_from_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        let mut metadata = Metadata::default();
        inject_context_into_metadata(&mut metadata, TRACING_METADATA_KEY, &upstream_cx);
        let upstream_traceparent = metadata
            .sys_metadata
            .get(TRACING_METADATA_KEY)
            .and_then(|kvg| kvg.key_value.get("traceparent"))
            .cloned();
        let mut msg = Message {
            metadata: Some(Arc::new(metadata)),
            ..Default::default()
        };

        with_otel_test_subscriber(|| {
            let result = start_vertex_process_from_isb_enabled(&mut msg);
            assert!(!result.platform_span.is_disabled());
            drop(StageSpan::from_context(result.isb_read_cx));
            drop(result.platform_span);
        });

        let local_traceparent = msg
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.sys_metadata.get(TRACING_METADATA_KEY))
            .and_then(|kvg| kvg.key_value.get("traceparent"))
            .cloned();
        assert!(local_traceparent.is_some());
        assert_ne!(upstream_traceparent, local_traceparent);

        let extracted_cx = parent_context_from_metadata(msg.metadata.as_deref());
        let extracted_span = extracted_cx.span();
        assert!(extracted_span.span_context().is_valid());
    }

    /// `start_isb_write_span` is called once per ISB publish; when tracing is
    /// disabled it should return an inert `StageSpan` whose Drop is a no-op.
    #[test]
    fn start_isb_write_span_is_inert_when_platform_spans_disabled() {
        let msg = Message::default();
        let guard = start_isb_write_span(&msg, None);
        drop(guard); // No panic, no work.
    }

    #[test]
    fn start_isb_write_span_enabled_preserves_tracing_metadata() {
        init_propagator();
        let parent_cx =
            context_from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let mut metadata = Metadata::default();
        inject_context_into_metadata(&mut metadata, TRACING_METADATA_KEY, &parent_cx);
        let original_tracing = metadata
            .sys_metadata
            .get(TRACING_METADATA_KEY)
            .expect("parent tracing metadata should be present")
            .key_value
            .clone();
        let msg = Message {
            metadata: Some(Arc::new(metadata)),
            ..Default::default()
        };

        with_otel_test_subscriber(|| {
            let guard = start_isb_write_span_enabled(&msg, Some("already-formatted"));
            drop(guard);
            let guard = start_isb_write_span_enabled(&msg, None);
            drop(guard);
        });

        let current_tracing = &msg
            .metadata
            .as_ref()
            .expect("metadata should still be present")
            .sys_metadata
            .get(TRACING_METADATA_KEY)
            .expect("tracing metadata should still be present")
            .key_value;
        assert_eq!(current_tracing, &original_tracing);
    }
}
