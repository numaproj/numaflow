# Tracing

Numaflow can export platform traces from vertex pods using OpenTelemetry Protocol (OTLP). Traces are disabled by default. To enable them, configure an OTLP traces endpoint on the vertex `numa` container.

Tracing is enabled when `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set to a non-empty value and `OTEL_SDK_DISABLED` is not set to `true`. There is no separate Numaflow-specific tracing flag.

Tracing is controlled with standard OpenTelemetry environment variables:

| Environment variable | Description |
| --- | --- |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | OTLP endpoint used for traces. Prefer this when traces and metrics use different collectors. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Generic OTLP endpoint. Used for traces when `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is not set. |
| `OTEL_SDK_DISABLED` | Set to `true` to disable tracing entirely, even when an OTLP endpoint is configured. |
| `OTEL_SERVICE_NAME` | Service name reported to the tracing backend. If unset, Numaflow uses `platform`. |
| `OTEL_TRACES_SAMPLER` | Sampler used to decide which traces are recorded and exported. |
| `OTEL_TRACES_SAMPLER_ARG` | Sampler argument. For ratio-based samplers, this is a value between `0.0` and `1.0`. |

If both endpoint variables are set, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` takes precedence. Numaflow exports traces using OTLP over gRPC, so use the collector's gRPC endpoint, typically port `4317`.

## Enable Tracing for All Vertices

Use `.spec.templates.vertex.containerTemplate.env` to apply tracing configuration to all vertex pods in a pipeline.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    vertex:
      containerTemplate:
        env:
          - name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
            value: "http://otel-collector.observability.svc.cluster.local:4317"
          - name: OTEL_TRACES_SAMPLER
            value: "parentbased_traceidratio"
          - name: OTEL_TRACES_SAMPLER_ARG
            value: "0.001"
  vertices:
    - name: in
      source:
        generator:
          rpu: 1
          duration: 1s
    - name: cat
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: cat
    - from: cat
      to: out
```

This enables tracing for all vertex pods and exports sampled traces to the configured OTLP collector.

For MonoVertex, set `spec.containerTemplate.env` because there is only one vertex `numa` container. The same OTLP endpoint, service name, and sampling settings apply.

## Configure Service Names

If all vertices use the same `OTEL_SERVICE_NAME`, tracing backends such as Jaeger group them under the same service. For pipeline debugging, it is often clearer to set one service name per vertex.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: in
      containerTemplate:
        env:
          - name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
            value: "http://otel-collector.observability.svc.cluster.local:4317"
          - name: OTEL_SERVICE_NAME
            value: "numaflow-my-pipeline-in"
      source:
        generator:
          rpu: 1
          duration: 1s
    - name: cat
      containerTemplate:
        env:
          - name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
            value: "http://otel-collector.observability.svc.cluster.local:4317"
          - name: OTEL_SERVICE_NAME
            value: "numaflow-my-pipeline-cat"
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable
    - name: out
      containerTemplate:
        env:
          - name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
            value: "http://otel-collector.observability.svc.cluster.local:4317"
          - name: OTEL_SERVICE_NAME
            value: "numaflow-my-pipeline-out"
      sink:
        log: {}
  edges:
    - from: in
      to: cat
    - from: cat
      to: out
```

A common naming pattern is `numaflow-<pipeline>-<vertex>`.

Use `.spec.templates.vertex.containerTemplate.env` for values that should apply to every pipeline vertex. Use `.spec.vertices[*].containerTemplate.env` when a specific vertex needs different values, such as a per-vertex `OTEL_SERVICE_NAME`.

## Configure Sampling

Numaflow emits multiple spans per message. To avoid overwhelming collectors on high-throughput pipelines, the default sampler is `parentbased_traceidratio` with `OTEL_TRACES_SAMPLER_ARG=0.001`, which samples approximately 0.1% of root traces while preserving the sampling decision for child spans.

To change the sampling rate, set both sampler variables:

```yaml
containerTemplate:
  env:
    - name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
      value: "http://otel-collector.observability.svc.cluster.local:4317"
    - name: OTEL_TRACES_SAMPLER
      value: "parentbased_traceidratio"
    - name: OTEL_TRACES_SAMPLER_ARG
      value: "0.01" # 1%
```

The `parentbased_*` samplers only make a fresh decision for root spans, which are traces that start in this process. For spans with a remote parent, such as spans received through `traceparent` from an upstream service, Numaflow honors the parent's sampling flag. This is the recommended default because it preserves cross-service trace consistency.

Supported sampler values:

| `OTEL_TRACES_SAMPLER` | `OTEL_TRACES_SAMPLER_ARG` | Behavior |
| --- | --- | --- |
| `always_on` | Not used | Records every trace. |
| `always_off` | Not used | Records no traces. |
| `traceidratio` | Ratio from `0.0` to `1.0` | Samples root traces by trace ID ratio. |
| `parentbased_always_on` | Not used | Records root traces and follows the parent decision for child spans. Useful for short debugging sessions. |
| `parentbased_always_off` | Not used | Drops root traces and follows the parent decision for child spans. |
| `parentbased_traceidratio` | Ratio from `0.0` to `1.0` | Samples root traces by ratio and follows the parent decision for child spans. This is the default. |

Examples:

```yaml
# Drop all root traces. Child spans with a remotely sampled parent context
# are still recorded. To fully disable tracing, unset OTLP endpoints or
# set OTEL_SDK_DISABLED=true.
- name: OTEL_TRACES_SAMPLER
  value: "always_off"
```

```yaml
# Sample about 10% of root traces.
- name: OTEL_TRACES_SAMPLER
  value: "parentbased_traceidratio"
- name: OTEL_TRACES_SAMPLER_ARG
  value: "0.1"
```

```yaml
# Export every trace. Use carefully on high-throughput pipelines.
- name: OTEL_TRACES_SAMPLER
  value: "parentbased_always_on"
```

## Span Hierarchy

Numaflow emits one trace per source message. Each vertex pod contributes spans under a `numaflow.vertex.process` lifecycle span. The lifecycle span closes when the message is acked.

### MonoVertex

MonoVertex has no inter-step buffer; source, map, and sink run in a single pod. No `isb.read` or `isb.write` spans are emitted.

```text
numaflow.vertex.process                         (Internal)
|-- numaflow.monovertex.source.dispatch         (Consumer)
|   `-- numaflow.monovertex.source.transform    (Client, only if transformer is configured)
|-- numaflow.monovertex.map                     (Client, one per UDF call)
|-- numaflow.monovertex.sink.write              (Client, one per output message)
|-- numaflow.monovertex.sink.fallback           (Client, only on fallback path)
`-- numaflow.monovertex.sink.on_success         (Client, only when configured)
```

For map fan-out, Numaflow still emits one `numaflow.monovertex.map` span for the UDF call. Each output message gets its own downstream `numaflow.monovertex.sink.write` span as a sibling under `numaflow.vertex.process`.

### Pipeline

Each pipeline vertex pod has its own `numaflow.vertex.process` lifecycle span. The downstream vertex process is parented to the upstream vertex process through tracing context carried in ISB message metadata.

```text
[source pod]
numaflow.vertex.process                         (Internal)
|-- numaflow.pipeline.source.dispatch           (Consumer)
|   `-- numaflow.pipeline.source.transform      (Client, only if transformer is configured)
`-- numaflow.pipeline.isb.write                 (Producer)

[map pod]
numaflow.vertex.process                         (Internal, parent = source vertex.process)
|-- numaflow.pipeline.isb.read                  (Consumer)
|-- numaflow.pipeline.map                       (Client)
`-- numaflow.pipeline.isb.write                 (Producer)

[sink pod]
numaflow.vertex.process                         (Internal, parent = map vertex.process)
|-- numaflow.pipeline.isb.read                  (Consumer)
`-- numaflow.pipeline.sink.write                (Client)
```

Common attributes on Numaflow spans include `messaging.system=numaflow`, `numaflow.topology`, `numaflow.pipeline.name`, `numaflow.vertex.name`, and `messaging.message.id`.

## Current Limitations

Tracing currently covers source, source transformer, map UDF calls, sink writes, and pipeline ISB read/write spans. Reduce aggregation tracing is not supported yet and will be taken up in a later version.

WMB control messages are not traced. They are acknowledged and skipped before platform span creation.

## Operational Notes

- Use `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` if you anticipate routing traces and OTLP metrics or logs to different collectors. Otherwise, `OTEL_EXPORTER_OTLP_ENDPOINT` is sufficient.
- Start with low sampling, such as the default `0.001`, for production pipelines.
- Use `parentbased_always_on` only for debugging or low-throughput pipelines.
- Tracing environment variables must be configured on the Numaflow vertex container through `containerTemplate.env`; setting them only on a UDF container does not enable Numaflow platform spans.
