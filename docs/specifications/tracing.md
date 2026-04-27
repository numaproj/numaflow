# OpenTelemetry Distributed Tracing for Numaflow

## 1. Objective

Enable distributed tracing in Numaflow to track each message's journey from Source to Sink. This provides visibility into:
- The path a message takes through the pipeline.
- Latency introduced by each component (Source, Map UDF, Sink UDF, platform overhead).
- Whether latency is attributable to the Numaflow platform or user-defined functions (UDFs).

## 2. Span Hierarchy: Sequential Siblings

Each message produces a single trace with the following span hierarchy:

```
upstream producer (optional — carries W3C traceparent or B3 headers)
└── pipeline.process                         (platform, root of Numaflow journey)
    ├── source.read                          (platform, child of pipeline.process)
    ├── map                                  (platform, child of pipeline.process)
    │   └── udf.map.process                  (UDF, child of map)
    │       ├── db.lookup                    (UDF child — user-created spans)
    │       └── cache.lookup                 (UDF child — user-created spans)
    └── sink.write                           (platform, child of pipeline.process)
        └── udf.sink.process                 (UDF, child of sink.write)
            ├── serialize                    (UDF child — user-created spans)
            └── kafka.produce                (UDF child — user-created spans)
```

**Key properties:**
- `pipeline.process` is the root span covering the full message lifecycle (source read → map → sink → ack). Its duration = total pipeline latency for that message.
- `source.read`, `map`, and `sink.write` are **siblings** under `pipeline.process`. Each span's duration is accurate and independent — no inflation.
- UDF spans (`udf.map.process`, `udf.sink.process`) are children of their corresponding platform span, not cross-linked.
- Per-stage latency isolation is clean: `map` duration - `udf.map.process` duration = platform overhead for the map stage.

### 2.1 Span Naming Convention

| Span Name | Service | Kind | Created By |
|---|---|---|---|
| `numaflow.platform.pipeline.process` | numaflow-core | INTERNAL | Platform (source.rs) |
| `numaflow.platform.source.read` | numaflow-core | PRODUCER | Platform (source.rs) |
| `numaflow.platform.map` | numaflow-core | INTERNAL | Platform (mapper) |
| `numaflow.platform.sink.write` | numaflow-core | CLIENT | Platform (sinker) |
| `udf.map.process` | numaflow-udf-{name} | CONSUMER | UDF (Java/Go/Python SDK) |
| `udf.sink.process` | numaflow-udf-{name} | CONSUMER | UDF (Java/Go/Python SDK) |

### 2.2 Span Attributes (OTel Semantic Conventions)

All spans carry [OTel Messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/):

| Attribute | Value | Description |
|---|---|---|
| `messaging.system` | `numaflow` | Identifies the messaging system |
| `messaging.operation.type` | `process` / `publish` | Operation type |
| `messaging.operation.name` | `pipeline.process` / `source.read` / `map` / `sink.write` | Stage name |
| `messaging.message.id` | Message offset | Per-message identifier |

## 3. Key Architectural Decisions

### 3.1 Format: W3C Trace Context

We use the **W3C Trace Context** standard (`traceparent`, `tracestate`) for context propagation.
- **Rationale**: Industry standard, natively supported by OpenTelemetry, compact (1-2 headers), vendor-neutral.

### 3.2 Carrier: System Metadata (`sys_metadata`)

Trace context is stored in the `sys_metadata` field of the Numaflow `Message` protobuf. **Not** in gRPC headers — because gRPC BiDi streaming sets headers once per stream, not per message.

- **Location**: `message.metadata.sys_metadata`
- **Key**: `"tracing"` — carries the shared parent context (`pipeline.process`)
- **Value**: A `KeyValueGroup` containing W3C `traceparent` and `tracestate`

```protobuf
message Metadata {
  map<string, KeyValueGroup> sys_metadata = 2;
}
```

```json
{
  "sys_metadata": {
    "tracing": {
      "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      "tracestate": "pqrs=00f067aa0ba902b7"
    }
  }
}
```

### 3.3 Dual-Key sys_metadata for UDF Context

Two sys_metadata keys are used for trace propagation:

| Key | Contents | Written By | Read By |
|---|---|---|---|
| `sys_metadata["tracing"]` | `pipeline.process` context | Platform (source, once) | Map, Sink (to become siblings) |
| `sys_metadata["tracing_udf"]` | Stage-specific context (e.g., `map` or `sink.write`) | Platform (before UDF call) | UDF (to see platform stage as parent) |

**Why two keys?** The `"tracing"` key holds `pipeline.process` context and is never overwritten — this is what makes source.read, map, and sink.write siblings. The `"tracing_udf"` key is temporarily set before each UDF call so the UDF sees the correct platform stage as its parent. It is removed from result messages after the UDF returns.

### 3.4 pipeline.process Span Lifecycle

The `pipeline.process` span must live for the entire message lifecycle (source read → map → sink → ack) to have accurate duration. It is stored in the message's `AckHandle` — a reference-counted handle that is dropped when the message is fully processed (acked). When the `AckHandle` drops, the span ends automatically.

### 3.5 Upstream Trace Stitching

If the source message carries trace headers, the platform extracts them and makes `pipeline.process` a child of the upstream span. This stitches the Numaflow trace into the upstream producer's trace. Supported formats:

- **W3C Trace Context** (`traceparent`, `tracestate`) — extracted directly.
- **B3 Multi-Header** (`X-B3-TraceId`, `X-B3-SpanId`, `X-B3-Sampled`) — converted to W3C `traceparent` format. 64-bit trace IDs are left-padded to 128-bit.

If no trace headers are present, `pipeline.process` starts a new root trace.

## 4. Implementation: Numaflow Core (Rust)

### 4.1 Dependencies

```toml
opentelemetry = "0.28"
opentelemetry_sdk = "0.28"
opentelemetry-otlp = "0.28"
tracing-opentelemetry = "0.29"
```

### 4.2 Context Propagation Helper (`shared/otel.rs`)

```rust
pub const TRACING_METADATA_KEY: &str = "tracing";
pub const TRACING_UDF_METADATA_KEY: &str = "tracing_udf";

pub struct MetadataExtractor<'a>(pub &'a KeyValueGroup);
// Implements Extractor — reads traceparent/tracestate from KeyValueGroup

pub struct MetadataInjector<'a>(pub &'a mut KeyValueGroup);
// Implements Injector — writes traceparent/tracestate to KeyValueGroup

pub fn extract_trace_context(metadata: &Metadata) -> opentelemetry::Context;
pub fn inject_context_into_metadata(metadata: &mut Metadata, key: &str, cx: &Context);
pub fn extract_trace_context_from_headers(headers: &HashMap<String, String>) -> Context;
```

### 4.3 Component Instrumentation

#### Source
1. Extract upstream context from message headers (W3C traceparent or B3 multi-headers).
2. Create `pipeline.process` span (parent: upstream context, or new root).
3. Create `source.read` span (parent: `pipeline.process`). Short-lived marker.
4. Inject `pipeline.process` context into `sys_metadata["tracing"]`.
5. Store `pipeline.process` span in `AckHandle` for accurate duration.

#### Map Vertex
1. Extract `pipeline.process` context from `sys_metadata["tracing"]`.
2. Create `map` span (parent: `pipeline.process`). Duration covers the full UDF call.
3. Before UDF call: inject `map` context into `sys_metadata["tracing_udf"]`.
4. After UDF returns: remove `tracing_udf` from result messages. Leave `tracing` (pipeline.process) untouched.

#### Sink Vertex
1. Extract `pipeline.process` context from `sys_metadata["tracing"]`.
2. Create per-message `sink.write` spans. Duration covers the batch UDF call.
3. Before UDF call: inject `sink.write` context into `sys_metadata["tracing_udf"]`.

### 4.4 Handling Aggregation (Reduce Vertex)

Reduce vertices ingest multiple messages (Fan-In) and produce a single result. Strategy:

- **Primary Parent**: Select one context (e.g., first message in the window) as the direct parent of the Reduce span.
- **Span Links**: Add contexts of other input messages as OTel Span Links. These appear as "Related Traces" in visualization tools.
- **Limit**: Max 50 links to prevent span bloat. If the window has more messages, sample representative links (first 10, last 10, evenly spaced).

### 4.5 Handling FlatMap (Fan-Out)

When a Map vertex produces multiple output messages from a single input (1 → N):
- The platform injects the **same** parent span context into `sys_metadata` for every output message.
- Downstream vertices create N separate spans all pointing to the same parent.

## 5. Implementation: Numaflow SDKs (Go/Java/Python/Rust)

### 5.1 Initialization

The SDK must initialize a tracer to participate in the trace. It reads:
- `OTEL_EXPORTER_OTLP_ENDPOINT` — OTLP gRPC endpoint (e.g., `http://otel-collector:4317`)
- `OTEL_SERVICE_NAME` — Service name for spans (e.g., `numaflow-udf-map`)

### 5.2 Instrumentation

In the UDF handler:
1. **Extract**: Read parent context from `sys_metadata["tracing_udf"]` (stage-specific context). Falls back to `sys_metadata["tracing"]` (pipeline root) if `tracing_udf` is absent.
2. **Start Span**: Create span (e.g., `udf.map.process`, kind=CONSUMER) as child of extracted context.
3. **Execute**: Pass context to user function. Users can create child spans using standard OTel APIs.
4. **End Span**: Close the span when the function returns.

### 5.3 Example: Java Map UDF

```java
public MessageList processMessage(String[] keys, Datum datum) {
    // Extract parent context from sys_metadata
    Context parentCtx = NumaflowTracing.extractUdfContext(datum.getSystemMetadata());
    Span span = NumaflowTracing.startSpan("udf.map.process", parentCtx, "map", null);

    try (Scope scope = span.makeCurrent()) {
        // User business logic — child spans auto-attach to udf.map.process
        doWork();
        return MessageList.newBuilder()
                .addMessage(new Message(datum.getValue(), keys))
                .build();
    } finally {
        span.end();
    }
}
```

## 6. End-to-End Trace Flow

### MonoVertex (Source → Map → Sink)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ Message arrives with trace headers (W3C traceparent or B3 multi-headers)    │
│ If no headers present, a new root trace is started.                         │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │
               ▼
┌─── Source (platform) ────────────────────────────────────────────────────────┐
│  1. Extract trace context from message headers (W3C or B3 → W3C conversion) │
│  2. Create pipeline.process span (parent: upstream, or new root)            │
│  3. Create source.read span (parent: pipeline.process)                      │
│  4. Inject pipeline.process context → sys_metadata["tracing"]               │
│  5. Store pipeline.process span in AckHandle                                │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  sys_metadata["tracing"] = pipeline.process
               ▼
┌─── Map (platform) ───────────────────────────────────────────────────────────┐
│  1. Extract pipeline.process from sys_metadata["tracing"]                   │
│  2. Create map span (parent: pipeline.process) → sibling of source.read     │
│  3. Inject map context → sys_metadata["tracing_udf"]                        │
│  4. Call Map UDF via gRPC                                                   │
│     └─ UDF reads sys_metadata["tracing_udf"] → creates udf.map.process     │
│  5. Remove tracing_udf from result. Leave tracing (pipeline.process) intact │
└──────────────┬───────────────────────────────────────────────────────────────┘
               │  sys_metadata["tracing"] = pipeline.process (unchanged)
               ▼
┌─── Sink (platform) ──────────────────────────────────────────────────────────┐
│  1. Extract pipeline.process from sys_metadata["tracing"]                   │
│  2. Create sink.write span (parent: pipeline.process) → sibling of map      │
│  3. Inject sink.write context → sys_metadata["tracing_udf"]                 │
│  4. Call Sink UDF via gRPC                                                  │
│     └─ UDF reads sys_metadata["tracing_udf"] → creates udf.sink.process    │
│  5. Ack message → AckHandle drops → pipeline.process span ends              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Pipeline (Source → Vertex₁ → ISB → Vertex₂ → ... → Sink)

For multi-vertex Pipelines, the same pattern applies at each vertex. The `pipeline.process` context in `sys_metadata["tracing"]` is carried through the ISB (JetStream/Kafka) between vertices, maintaining the trace across process boundaries.

## 7. Configuration & Deployment

### 7.1 Enabling Tracing

Add environment variables to the pipeline/monovertex spec:

**Platform container** (`containerTemplate`):
```yaml
spec:
  containerTemplate:
    env:
      - name: OTEL_EXPORTER_OTLP_ENDPOINT
        value: "http://otel-collector.observability:4317"
      - name: OTEL_SERVICE_NAME
        value: "numaflow-core"
```

**UDF containers** (source, udf, sink):
```yaml
spec:
  udf:
    container:
      env:
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: "http://otel-collector.observability:4317"
        - name: OTEL_SERVICE_NAME
          value: "numaflow-udf-map"
```

### 7.2 Infrastructure

The user is responsible for deploying the observability stack:
1. **OTel Collector**: Receives traces from Numaflow pods.
2. **Backend**: Jaeger, Grafana Tempo, Datadog, etc.

## 8. Reliability and Failure Handling

### 8.1 OTLP Endpoint Down

The tracing infrastructure is separate from the pipeline. If the OTel Collector becomes unreachable:
- The OpenTelemetry SDK buffers spans in memory (default ~2048 spans).
- It retries export with exponential backoff.
- If the buffer fills, new spans are dropped. This is intentional to prevent unbounded memory growth.
- **Pipeline impact**: None. Message processing continues normally.

### 8.2 Sampling

For high-throughput pipelines, recording every trace can be expensive. Head-based sampling at the Source decides "should I record this trace?" — all downstream components respect the sampled flag.

- `OTEL_TRACES_SAMPLER`: Strategy (e.g., `parentbased_traceidratio`).
- `OTEL_TRACES_SAMPLER_ARG`: Probability (e.g., `0.1` for 10% sampling).
