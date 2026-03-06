# OpenTelemetry Tracing Design for Numaflow [Proposal]

## 1. Objective
The goal is to enable distributed tracing in Numaflow to track message propagation from Source to Sink. This will provide visibility into:
- The path a message takes through the pipeline.
- Latency introduced by each component (Source, ISB, UDFs, Sink).
- Bottlenecks in the system.

## 2. Key Architectural Decisions

### 2.1 Format: W3C Trace Context
We will use the **W3C Trace Context** standard (`traceparent`, `tracestate`) for context propagation.
- **Rationale**: It is the industry standard, natively supported by OpenTelemetry, compact (1-2 headers), and vendor-neutral.

### 2.2 Carrier: System Metadata (`sys_metadata`)
Trace context will be stored in the `sys_metadata` field of the Numaflow `Message` protobuf.

- **Location**: `message.metadata.sys_metadata`
- **Key**: `"tracing"`
- **Value**: A `KeyValueGroup` containing the W3C headers.

**Message Structure:**
```protobuf
message Message {
  // User headers (Immutable)
  map<string, string> headers = 5; 
  
  // System metadata (Mutable carrier for tracing)
  metadata.Metadata metadata = 6;
}

message Metadata {
  map<string, KeyValueGroup> sys_metadata = 2;
}
```

**Conceptual Data:**
```json
{
  "sys_metadata": {
    "tracing": {
      "key_value": {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "tracestate": "pqrs=00f067aa0ba902b7,congo=t61rcWkgMzE"
      }
    }
  }
}
```

## 3. Implementation Plan: Numaflow Core
The Rust data plane (`rust/numaflow-core`) handles the heavy lifting of reading/writing to ISB and invoking UDFs.

### 3.1 Dependencies
Update `Cargo.toml` with:
- `opentelemetry`
- `opentelemetry_sdk`
- `opentelemetry-otlp`
- `tracing-opentelemetry`

### 3.2 Context Propagation Helper
We need a helper module (`numaflow-core/src/shared/otel.rs`) that implements `opentelemetry::propagation::Extractor` and `Injector` for the `KeyValueGroup` struct.

```rust
pub const TRACING_METADATA_KEY: &str = "tracing";

pub struct MetadataExtractor<'a>(pub &'a KeyValueGroup);
// Implements Extractor trait to read "traceparent" from KeyValueGroup

pub struct MetadataInjector<'a>(pub &'a mut KeyValueGroup);
// Implements Injector trait to write "traceparent" to KeyValueGroup
```

### 3.3 Initialization (Tracer Setup)
The tracer MUST be initialized at startup in `main`. It should be configured with a **Service Name** (typically `pipeline_name-vertex_name`) and use the **OTLP Exporter** for production.

**Initialization Snippet:**
```rust
use opentelemetry::sdk::{Resource, trace as sdktrace};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;

pub fn init_tracing(service_name: &str) -> Result<sdktrace::Tracer, Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Check if tracing is enabled via custom env var
    let enabled = std::env::var("NUMAFLOW_TRACING_ENABLED")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    if !enabled {
        // Return a no-op tracer
        return Ok(sdktrace::TracerProvider::default().tracer("noop"));
    }

    //  Configure Resource (Service Name is critical for filtering in UI)
    let resource = Resource::new(vec![
        KeyValue::new("service.name", service_name.to_string()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    //  Configure OTLP Exporter
    // - Automatically reads OTEL_EXPORTER_OTLP_ENDPOINT from env
    // - Defaults to http://localhost:4317 if not set
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic(); // Uses gRPC

    // Build Pipeline
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            sdktrace::config().with_resource(resource)
        )
        .install_batch(opentelemetry::runtime::Tokio)?;
        
    Ok(tracer)
}
```

### 3.4 Component Instrumentation

#### A. Source (Producer)
- **Action**: Starts the trace.
- **Logic**:
  1. Create a new "Root" Span (e.g., `kind=Producer`).
  2. Inject the span context into the outgoing message's `sys_metadata["tracing"]`.
  3. Write message to ISB.

#### B. Vertices (Map/Reduce/Sink)
- **Action**: Continues the trace.
- **Logic**:
  1. **Read**: Read message from ISB.
  2. **Extract**: Look for `sys_metadata["tracing"]`. Use `MetadataExtractor` to get the parent context.
  3. **Start Span**: Create a new span (Span Kind to be decided) linked to the parent context.
     * **Available Span Kinds**:
       - `Server`: Covers the server-side handling of an RPC or other remote request.
       - `Client`: Covers the client-side of an RPC or other remote request.
       - `Producer`: Describes a child of an asynchronous request (e.g., sending to a queue).
       - `Consumer`: Describes a child of an asynchronous request (e.g., receiving from a queue).
       - `Internal`: Default. Represents an internal operation within an application.
  4. **Process**:
     - **Before UDF**: Create a child span (e.g., `kind=Client`) for the UDF call. Inject this context into a *copy* of the metadata passed to the UDF.
     - **After UDF**: Receive results.
  5. **Write**:
     - Inject the current context into the outgoing message's `sys_metadata`.
     - Write to next ISB.

### 3.5 Handling Aggregation (Reduce Vertex)
Reduce vertices ingest multiple messages (Fan-In) and produce a single result. This requires a strategy for linking multiple parent contexts (one from each input message) to a single resulting span.

#### Option: Trace Links
This approach preserves the relationship between all input messages and the aggregated result without creating a messy parent-child graph.
- **Ingest Phase**: As messages arrive in the window, we extract their trace contexts.
- **Aggregate Phase**: When the window closes and we create the span for the Reduce UDF execution:
  1. **Primary Parent**: We select **one** context (e.g., from the first message) to be the direct `Parent` of the new span. This maintains the main timeline.
  2. **Links**: We add the contexts of all *other* messages as **Span Links**. In distributed tracing visualization tools, these appear as "Related Traces" or "Links", allowing users to jump from any input message trace to the aggregated reduce trace.
- **Limit**: To prevent span bloat in massive windows, limit the number of links (e.g., max 50). If more, sample representative links (e.g., first 10, last 10).

### 3.6 Handling FlatMap (Fan-Out)
When a Map vertex produces multiple output messages from a single input (1 -> N):
- **Forking**: The trace context "forks" automatically.
- **Implementation**: The Core injects the **same** current span context (parent) into `sys_metadata` for **every** output message.
- **Result**: Downstream vertices will create N separate spans, all pointing back to the same parent span from the Map vertex.

## 4. Implementation Plan: Numaflow SDKs (e.g., Go/Rust/Python)

The SDKs run user code (UDFs) in a separate container. They **MUST** also initialize a tracer to participate in the distributed trace.

### 4.1 Initialization
The SDK must run similar initialization code as the Core (see section 3.3). 
- It should read `NUMAFLOW_TRACING_ENABLED` to decide whether to start.
- It should use `NUMAFLOW_PIPELINE_NAME` + `NUMAFLOW_VERTEX_NAME` (or similar) to construct `service.name`.
- It connects to the **same** OTLP endpoint as the Core sidecar.

### 4.2 Helper
Implement `propagation.TextMapCarrier` for the `KeyValueGroup` protobuf struct in Go/Rust.

### 4.3 Instrumentation
In the gRPC handler (e.g., `MapFn`):
1. **Extract**: Read `traceparent` from `request.Metadata.SysMetadata["tracing"]`.
2. **Start Span**: Start a new span (`name="UserFunction"`, `kind=Server`).
3. **Execute**: Pass the `context` (containing the span) to the user's function.
   - This allows users to create their own child spans using standard OTel APIs: `otel.Tracer("my-udf").Start(ctx, "sub-task")`.
4. **End Span**: Close the span when the function returns.

## 5. End-to-End Workflow

1. **Source**:
   - Generates data.
   - Starts Trace `T1`, Span `S1`.
   - Writes Message `M1` with `sys_metadata.tracing.traceparent = T1-S1`.

2. **ISB (JetStream)**:
   - Persists `M1`.

3. **Map Vertex (Core)**:
   - Reads `M1`.
   - Extracts context `T1-S1`.
   - Starts Span `S2` (Parent: `S1`) -> **"Vertex Processing"**.
     - Covers overhead (ISB read/write, serialization).
   - Prepares UDF Request. Starts Span `S3` (Parent: `S2`) -> **"UDF RPC Call"**.
     - Covers gRPC roundtrip to UDF container.
   - Injects `T1-S3` into UDF Request Metadata.

4. **UDF (SDK)**:
   - Receives Request.
   - Extracts `T1-S3`.
   - Starts Span `S4` (Parent: `S3`). runs user code... Ends `S4`.
   - **Crucial**: SDK sends Span `S4` data to Collector.

5. **Map Vertex (Resume)**:
   - UDF returns. Ends `S3`.
   - Writes Message `M2` to next ISB.
   - Injects `T1-S2` (or a new child `S5`) into `M2.sys_metadata`.

## 6. Configuration & Deployment

### 6.1 Pipeline Spec [TBD]
Users enable tracing by adding environment variables to the pipeline spec.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline

      container:
        env:
          - name: NUMAFLOW_TRACING_ENABLED
            value: "true"
          - name: OTEL_EXPORTER_OTLP_ENDPOINT
            value: "http://otel-collector.observability:4317"
```

### 6.2 Infrastructure
The user is responsible for deploying the observability stack:
1. **OTEL Collector**: Receives traces from Numaflow.
2. **Backend**: Jaeger, Datadog, etc.

## 7. Reliability and Failure Handling

### 7.1 OTLP Endpoint Down
Since the tracing infrastructure (OTel Collector) is separate from the pipeline, it may become unreachable. The design ensures this **does not impact the reliability or stability of the Numaflow pipeline**.

*   **Behavior**: The OpenTelemetry SDK (Batch Span Processor) buffers spans in memory (default buffer size ~2048 spans).
*   **Retries**: It attempts to export the batch in the background with exponential backoff.
*   **Data Loss**: If the buffer fills up (because the exporter is failing consistently), **new spans are dropped**. This is intentional "lossy" behavior to prevent unbounded memory growth.
*   **Impact**:
    *   **Pipeline**: Continues processing messages normally. No latency or crashes.
    *   **Observability**: Trace data will be missing for the duration of the outage.

### 7.2 Sampling
To control the volume of trace data generated and sent to the collector. It decides "Should I record this trace or not?"

*   **Necessity**: For high-throughput systems, recording every trace can be expensive. Sampling allows us to record only a representative subset (e.g., 1%).
*   **Head-Based Sampling**: The decision is made at the **Source (Head)**. If the Source decides to sample a trace (`sampled=true` in `traceparent`), all downstream components respect this and record their spans.
*   **Configuration**: Users can configure this via standard OTel environment variables in the Pipeline spec.
    *   `OTEL_TRACES_SAMPLER`: Strategy (e.g., `parentbased_traceidratio`).
    *   `OTEL_TRACES_SAMPLER_ARG`: Probability (e.g., `0.1` for 10% sampling).

