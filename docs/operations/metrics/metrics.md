# Metrics

Numaflow provides the following Prometheus metrics which can be used to monitor your pipeline and setup any alerts if needed.

## Pipeline Metrics

These metrics are emitted by pipeline vertex pods. All pipeline forwarder metrics use the following common labels unless noted otherwise:

`pipeline=<pipeline-name>`, `vertex=<vertex-name>`, `vertex_type=<vertex-type>`, `replica=<replica-index>`

### Forwarder Metrics

| Metric name                                  | Metric type | Additional Labels    | Description                                                                                                |
|----------------------------------------------|-------------|----------------------|------------------------------------------------------------------------------------------------------------|
| `forwarder_read_total`                       | Counter     | `partition_name`     | Total number of messages read by a vertex from an ISB partition or source                                  |
| `forwarder_data_read_total`                  | Counter     | `partition_name`     | Total number of data messages read (excludes watermark/control messages)                                   |
| `forwarder_read_bytes_total`                 | Counter     | `partition_name`     | Total number of bytes read by a vertex from an ISB partition or source                                     |
| `forwarder_data_read_bytes_total`            | Counter     | `partition_name`     | Total number of data message bytes read (excludes watermark/control messages)                              |
| `forwarder_write_total`                      | Counter     | `partition_name`     | Total number of messages written to ISB by a vertex                                                        |
| `forwarder_write_bytes_total`                | Counter     | `partition_name`     | Total number of bytes written to ISB by a vertex                                                           |
| `forwarder_ack_total`                        | Counter     | `partition_name`     | Total number of messages acknowledged by a vertex                                                          |
| `forwarder_drop_total`                       | Counter     | `partition_name`, `reason` | Total number of messages dropped by a vertex                                                          |
| `forwarder_drop_bytes_total`                 | Counter     | `partition_name`, `reason` | Total number of bytes dropped by a vertex                                                             |
| `forwarder_read_error_total`                 | Counter     | `partition_name`     | Total number of read errors in the forwarder                                                               |
| `forwarder_write_error_total`                | Counter     | `partition_name`     | Total number of write errors in the forwarder                                                              |
| `forwarder_critical_error_total`             | Counter     | `reason`             | Total number of critical errors (e.g., EOT, UDF crashes) that could stop pipeline processing               |
| `forwarder_read_processing_time`             | Histogram   | `partition_name`     | Processing times of read operations, in microseconds                                                       |
| `forwarder_write_processing_time`            | Histogram   | `partition_name`     | Processing times of write operations, in microseconds                                                      |
| `forwarder_ack_processing_time`              | Histogram   | `partition_name`     | Processing times of ack operations, in microseconds                                                        |
| `forwarder_processing_time`                  | Histogram   | `partition_name`     | End-to-end processing time of a forwarding chunk, in microseconds                                          |
| `forwarder_read_batch_size`                  | Gauge       |                      | Read batch size for the vertex                                                                             |
| `vertex_pending_messages_raw`                | Gauge       | `partition_name`     | Raw pending messages count exposed by each vertex pod (aggregated by daemon into `vertex_pending_messages`) |

### Source Transformer Metrics

These metrics are specific to source vertices with a transformer configured.

| Metric name                                  | Metric type | Additional Labels    | Description                                                    |
|----------------------------------------------|-------------|----------------------|----------------------------------------------------------------|
| `source_forwarder_transformer_read_total`    | Counter     | `partition_name`     | Total number of messages read by the source transformer        |
| `source_forwarder_transformer_write_total`   | Counter     | `partition_name`     | Total number of messages written by the source transformer     |
| `source_forwarder_transformer_error_total`   | Counter     | `partition_name`     | Total number of source transformer errors                      |
| `source_forwarder_transformer_drop_total`    | Counter     | `partition_name`     | Total number of messages dropped by the source transformer     |
| `source_forwarder_transformer_processing_time` | Histogram | `partition_name`     | Processing times of the source transformer, in microseconds    |

### UDF Metrics

These metrics are specific to map/UDF vertices.

| Metric name                    | Metric type | Additional Labels | Description                                                       |
|--------------------------------|-------------|-------------------|-------------------------------------------------------------------|
| `forwarder_udf_read_total`     | Counter     |                   | Total number of messages read by UDF                              |
| `forwarder_udf_write_total`    | Counter     |                   | Total number of messages written by UDF                           |
| `forwarder_ud_drop_total`      | Counter     |                   | Total number of messages dropped by the user in UDF               |
| `forwarder_udf_error_total`    | Counter     |                   | Total number of UDF errors                                        |
| `forwarder_udf_processing_time` | Histogram  |                   | Processing times of User-Defined Functions (UDFs), in microseconds |

### Fallback Sink Metrics

These metrics are specific to sink vertices with a fallback sink configured.

| Metric name                              | Metric type | Additional Labels    | Description                                                              |
|------------------------------------------|-------------|----------------------|--------------------------------------------------------------------------|
| `forwarder_fbsink_write_total`           | Counter     | `partition_name`     | Total number of messages written to a fallback sink                      |
| `forwarder_fbsink_write_bytes_total`     | Counter     | `partition_name`     | Total number of bytes written to a fallback sink                         |
| `forwarder_fbsink_write_errors_total`    | Counter     | `partition_name`     | Total number of write errors for a fallback sink                         |
| `forwarder_fbsink_write_processing_time` | Histogram   | `partition_name`     | Processing times of write operations to a fallback sink, in microseconds |

### On-Success Sink Metrics

These metrics are specific to sink vertices with an on-success sink configured.

| Metric name                                      | Metric type | Additional Labels    | Description                                                                |
|--------------------------------------------------|-------------|----------------------|----------------------------------------------------------------------------|
| `forwarder_onsuccess_sink_write_total`           | Counter     | `partition_name`     | Total number of messages written to an on-success sink                     |
| `forwarder_onsuccess_sink_write_bytes_total`     | Counter     | `partition_name`     | Total number of bytes written to an on-success sink                        |
| `forwarder_onsuccess_sink_write_errors_total`    | Counter     | `partition_name`     | Total number of write errors for an on-success sink                        |
| `forwarder_onsuccess_sink_write_processing_time` | Histogram   | `partition_name`     | Processing times of write operations to an on-success sink, in microseconds |

## MonoVertex Metrics

These metrics are emitted by MonoVertex pods. All MonoVertex metrics use the following common labels:

`mvtx_name=<monovertex-name>`, `mvtx_replica=<replica-index>`

### Core Metrics

| Metric name                  | Metric type | Additional Labels | Description                                                              |
|------------------------------|-------------|-------------------|--------------------------------------------------------------------------|
| `monovtx_read_total`         | Counter     |                   | Total number of messages read from the source                            |
| `monovtx_read_bytes_total`   | Counter     |                   | Total number of bytes read from the source                               |
| `monovtx_ack_total`          | Counter     |                   | Total number of messages acknowledged by the sink                        |
| `monovtx_dropped_total`      | Counter     |                   | Total number of messages dropped by the monovertex                       |
| `monovtx_critical_error_total` | Counter   | `reason`          | Total number of critical errors (e.g., EOT, UDF crashes)                 |
| `monovtx_pending_raw`        | Gauge       |                   | Total number of source pending messages for the monovertex               |
| `monovtx_read_batch_size`    | Gauge       |                   | Read batch size for the monovertex                                       |
| `monovtx_processing_time`    | Histogram   |                   | Total time taken to forward a chunk, in microseconds                     |
| `monovtx_read_time`          | Histogram   |                   | Total time taken to read from the source, in microseconds                |
| `monovtx_ack_time`           | Histogram   |                   | Total time taken to ack to the source, in microseconds                   |

### Transformer Metrics

| Metric name                          | Metric type | Description                                                         |
|--------------------------------------|-------------|---------------------------------------------------------------------|
| `monovtx_transformer_time`           | Histogram   | Total time taken to transform, in microseconds                      |
| `monovtx_transformer_dropped_total`  | Counter     | Total number of messages dropped by the transformer                 |

### UDF Metrics

| Metric name                    | Metric type | Description                             |
|--------------------------------|-------------|-----------------------------------------|
| `monovtx_udf_time`            | Histogram   | Total time taken in UDF, in microseconds |
| `monovtx_udf_udf_error_total` | Counter     | Total number of UDF errors              |

### Sink Metrics

| Metric name                       | Metric type | Description                                                          |
|-----------------------------------|-------------|----------------------------------------------------------------------|
| `monovtx_sink_write_total`        | Counter     | Total number of messages written to the sink                         |
| `monovtx_sink_time`               | Histogram   | Total time taken to write to the sink, in microseconds               |
| `monovtx_sink_write_errors_total` | Counter     | Total number of write errors for the sink                            |
| `monovtx_sink_dropped_total`      | Counter     | Total number of messages dropped by sink                             |

### Fallback Sink Metrics

| Metric name                         | Metric type | Description                                                                  |
|-------------------------------------|-------------|------------------------------------------------------------------------------|
| `monovtx_fallback_sink_write_total` | Counter     | Total number of messages written to the fallback sink                        |
| `monovtx_fallback_sink_time`        | Histogram   | Total time taken to write to the fallback sink, in microseconds              |

### On-Success Sink Metrics

| Metric name                           | Metric type | Description                                                                    |
|---------------------------------------|-------------|--------------------------------------------------------------------------------|
| `monovtx_onsuccess_sink_write_total`  | Counter     | Total number of messages written to the on-success sink                        |
| `monovtx_onsuccess_sink_time`         | Histogram   | Total time taken to write to the on-success sink, in microseconds              |

## ISB Metrics (NATS JetStream)

These metrics are emitted by pipeline vertex pods for NATS JetStream Inter-Step Buffer operations.

| Metric name                        | Metric type | Labels                      | Description                                                                                               |
|------------------------------------|-------------|-----------------------------|-----------------------------------------------------------------------------------------------------------|
| `isb_jetstream_isFull_total`       | Counter     | `buffer=<buffer-name>`      | Total number of times the ISB was full. Continual increase indicates potential backpressure on the pipeline |
| `isb_jetstream_isFull_error_total` | Counter     | `buffer=<buffer-name>`, `reason=<reason>` | Total number of isFull errors with NATS JetStream ISB                                        |
| `isb_jetstream_read_error_total`   | Counter     | `buffer=<buffer-name>`, `reason=<reason>` | Total number of read errors with NATS JetStream ISB                                          |
| `isb_jetstream_write_error_total`  | Counter     | `buffer=<buffer-name>`, `reason=<reason>` | Total number of write errors with NATS JetStream ISB                                         |
| `isb_jetstream_write_timeout_total` | Counter    | `buffer=<buffer-name>`, `reason=<reason>` | Total number of write timeouts with NATS JetStream ISB                                       |
| `isb_jetstream_buffer_soft_usage`  | Gauge       | `buffer=<buffer-name>`      | Percentage of buffer soft usage (based on pending + ack pending messages)                                  |
| `isb_jetstream_buffer_solid_usage` | Gauge       | `buffer=<buffer-name>`      | Percentage of buffer solid usage (based on messages remaining in the stream)                               |
| `isb_jetstream_buffer_pending`     | Gauge       | `buffer=<buffer-name>`      | Number of pending messages at a given point in time                                                        |
| `isb_jetstream_buffer_ack_pending` | Gauge       | `buffer=<buffer-name>`      | Number of messages pending acknowledgment at a given point in time                                         |
| `isb_jetstream_read_time_total`    | Histogram   | `buffer=<buffer-name>`      | Processing times of JetStream read operations, in microseconds                                             |
| `isb_jetstream_write_time_total`   | Histogram   | `buffer=<buffer-name>`      | Processing times of JetStream write operations, in microseconds                                            |
| `isb_jetstream_ack_time_total`     | Histogram   | `buffer=<buffer-name>`      | Processing times of JetStream ack operations, in microseconds                                              |

## Controller Metrics

These metrics are emitted by the Numaflow controller.

### Health Metrics

| Metric name                | Metric type | Labels                                                 | Description                                                                                          |
|----------------------------|-------------|--------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `controller_isbsvc_health` | Gauge       | `ns=<namespace>` <br> `isbsvc=<isbsvc-name>`          | ISB Service health. '1' means healthy, '0' means unhealthy                                           |
| `controller_pipeline_health` | Gauge     | `ns=<namespace>` <br> `pipeline=<pipeline-name>`      | Pipeline health. '1' means healthy, '0' means unhealthy                                              |
| `controller_monovtx_health` | Gauge      | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`         | MonoVertex health. '1' means healthy, '0' means unhealthy                                            |

### Phase Metrics

| Metric name                           | Metric type | Labels                                                 | Description                                                                                                                        |
|---------------------------------------|-------------|--------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `controller_pipeline_desired_phase`   | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>`      | Pipeline desired phase. '1' means Running, '2' means Paused                                                                        |
| `controller_pipeline_current_phase`   | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>`      | Pipeline current phase. '0' = Unknown, '1' = Running, '2' = Paused, '3' = Failed, '4' = Pausing, '5' = Deleting                   |
| `controller_monovtx_desired_phase`    | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`         | MonoVertex desired phase. '1' means Running, '2' means Paused                                                                      |
| `controller_monovtx_current_phase`    | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`         | MonoVertex current phase. '0' = Unknown, '1' = Running, '2' = Paused, '3' = Failed                                                 |

### Replica Metrics

| Metric name                           | Metric type | Labels                                                                              | Description                                    |
|---------------------------------------|-------------|--------------------------------------------------------------------------------------|------------------------------------------------|
| `controller_vertex_desired_replicas`  | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`        | Desired replicas of a vertex                   |
| `controller_vertex_current_replicas`  | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`        | Current replicas of a vertex                   |
| `controller_vertex_min_replicas`      | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`        | Minimum replicas of a vertex                   |
| `controller_vertex_max_replicas`      | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`        | Maximum replicas of a vertex                   |
| `controller_monovtx_desired_replicas` | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`                                       | Desired replicas of a MonoVertex               |
| `controller_monovtx_current_replicas` | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`                                       | Current replicas of a MonoVertex               |
| `controller_monovtx_min_replicas`     | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`                                       | Minimum replicas of a MonoVertex               |
| `controller_monovtx_max_replicas`     | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`                                       | Maximum replicas of a MonoVertex               |
| `controller_isbsvc_jetstream_replicas` | Gauge      | `ns=<namespace>` <br> `isbsvc=<isbsvc-name>`                                        | JetStream ISB Service replicas                 |

## Daemon Metrics

These metrics are emitted by the pipeline daemon and MonoVertex daemon services, which aggregate data from individual vertex pods.

### Pipeline Daemon Metrics

| Metric name                        | Metric type | Labels                                                                                                        | Description                                                                                                 |
|------------------------------------|-------------|---------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| `pipeline_processing_lag`          | Gauge       | `pipeline=<pipeline-name>`                                                                                    | Pipeline processing lag in milliseconds (max watermark - min watermark)                                     |
| `pipeline_watermark_cmp_now`       | Gauge       | `pipeline=<pipeline-name>`                                                                                    | Max watermark of source compared with current time in milliseconds                                          |
| `pipeline_data_processing_health`  | Gauge       | `pipeline=<pipeline-name>`                                                                                    | Pipeline data processing health. 1: Healthy, 0: Unknown, -1: Warning, -2: Critical                         |
| `vertex_pending_messages`          | Gauge       | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `partition_name=<partition-name>` <br> `period=<duration>` | Average pending messages in the last period of seconds for a vertex (aggregated from `vertex_pending_messages_raw`) |
| `vertex_lookback_window_seconds`   | Gauge       | `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>`                                                       | Lookback window for a vertex in seconds                                                                      |

### MonoVertex Daemon Metrics

| Metric name                          | Metric type | Labels                                             | Description                                                                                                   |
|--------------------------------------|-------------|----------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `monovtx_pending`                    | Gauge       | `mvtx_name=<mvtx-name>` <br> `period=<duration>`  | Pending messages for a MonoVertex (aggregated from `monovtx_pending_raw`)                                     |
| `monovtx_lookback_window_seconds`    | Gauge       | `mvtx_name=<mvtx-name>`                           | Lookback window for a MonoVertex in seconds                                                                    |

## SDK Info Metrics

| Metric name | Metric type | Labels                                                                                                                                            | Description                                                                                       |
|-------------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `sdk_info`  | Gauge       | `component=<component>` <br> `component_name=<component-name>` <br> `language=<sdk-language>` <br> `version=<sdk-version>` <br> `type=<sdk-type>` | A metric with a constant value '1', labeled by SDK information such as version, language, and type |

## Build Info Metrics

| Metric name            | Metric type | Labels                                                                                                          | Description                                                                                                                                                                              |
|------------------------|-------------|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `build_info`           | Gauge       | `component=<component>` <br> `component_name=<component-name>` <br> `version=<version>` <br> `platform=<platform>` | A metric with a constant value '1', labeled by Numaflow binary version, platform, and other information. The value of `component` could be 'daemon', 'vertex', 'mono-vertex-daemon', etc |
| `controller_build_info` | Gauge      | `version=<version>` <br> `platform=<platform>`                                                                  | A metric with a constant value '1', labeled with controller version and platform from which Numaflow was built                                                                           |

## Prometheus Operator for Scraping Metrics:

You can follow the [prometheus operator](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/getting-started/installation.md) setup guide if you would like to use prometheus operator configured in your cluster.

You can also set up prometheus operator via [helm](https://bitnami.com/stack/prometheus-operator/helm).

### Configure the below Service/Pod Monitors for scraping your pipeline/monovertex metrics:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  labels:
    app.kubernetes.io/part-of: numaflow
  name: numaflow-pipeline-metrics
spec:
  endpoints:
    - scheme: https
      port: metrics
      targetPort: 2469
      tlsConfig:
        insecureSkipVerify: true
  selector:
    matchLabels:
      app.kubernetes.io/component: vertex
      app.kubernetes.io/managed-by: vertex-controller
      app.kubernetes.io/part-of: numaflow
    matchExpressions:
      - key: numaflow.numaproj.io/pipeline-name
        operator: Exists
      - key: numaflow.numaproj.io/vertex-name
        operator: Exists
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  labels:
    app.kubernetes.io/part-of: numaflow
  name: numaflow-pipeline-daemon-metrics
spec:
  endpoints:
    - scheme: https
      port: tcp
      targetPort: 4327
      tlsConfig:
        insecureSkipVerify: true
  selector:
    matchLabels:
      app.kubernetes.io/component: daemon
      app.kubernetes.io/managed-by: pipeline-controller
      app.kubernetes.io/part-of: numaflow
    matchExpressions:
      - key: numaflow.numaproj.io/pipeline-name
        operator: Exists
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  labels:
    app.kubernetes.io/part-of: numaflow
  name: numaflow-mvtx-metrics
spec:
  endpoints:
    - scheme: https
      port: metrics
      targetPort: 2469
      tlsConfig:
        insecureSkipVerify: true
  selector:
    matchLabels:
      app.kubernetes.io/component: mono-vertex
      app.kubernetes.io/managed-by: mono-vertex-controller
      app.kubernetes.io/part-of: numaflow
    matchExpressions:
      - key: numaflow.numaproj.io/mono-vertex-name
        operator: Exists
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  labels:
    app.kubernetes.io/part-of: numaflow
  name: numaflow-mvtx-daemon-metrics
spec:
  endpoints:
    - scheme: https
      port: tcp
      targetPort: 4327
      tlsConfig:
        insecureSkipVerify: true
  selector:
    matchLabels:
      app.kubernetes.io/component: mono-vertex-daemon
      app.kubernetes.io/managed-by: mono-vertex-controller
      app.kubernetes.io/part-of: numaflow
    matchExpressions:
      - key: numaflow.numaproj.io/mono-vertex-name
        operator: Exists
---
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  labels:
    app.kubernetes.io/part-of: numaflow
  name: numaflow-controller-metrics
spec:
  podMetricsEndpoints:
    - scheme: http
      port: metrics
      targetPort: 9090
  selector:
    matchLabels:
      app.kubernetes.io/component: controller-manager
      app.kubernetes.io/name: controller-manager
      app.kubernetes.io/part-of: numaflow
```

### Configure the below Service Monitor if you use the NATS Jetstream ISB for your NATS Jetstream metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  labels:
    app.kubernetes.io/part-of: numaflow
  name: numaflow-isbsvc-jetstream-metrics
spec:
  endpoints:
    - scheme: http
      port: metrics
      targetPort: 7777
  selector:
    matchLabels:
      app.kubernetes.io/component: isbsvc
      app.kubernetes.io/managed-by: isbsvc-controller
      app.kubernetes.io/part-of: numaflow
      numaflow.numaproj.io/isbsvc-type: jetstream
    matchExpressions:
      - key: numaflow.numaproj.io/isbsvc-name
        operator: Exists
```

## Metrics Exposition Format

Starting with Numaflow **v1.6**, the dataplane (vertex pods, monovertex pods) was rewritten in Rust, which changed the 
metrics exposition format from Prometheus text format to OpenMetrics text format due to the switch from using 
`client_golang` [prometheus client](https://github.com/prometheus/client_golang) to 
`client_rust` [prometheus client](https://github.com/prometheus/client_rust) for exposing metrics.
As we rewrite other parts (e.g., daemon server), they will also adopt OpenMetrics text format. Controller will be in Prometheus text format for the foreseeable future. 

### Version Differences

| Component                | Numaflow < v1.6        | Numaflow >= v1.6        |
| ------------------------ |------------------------|-------------------------|
| **Dataplane (vertices)** | Prometheus text format | OpenMetrics text format |
| **Controller**           | Prometheus text format | Prometheus text format  |
| **Daemon**               | Prometheus text format | Prometheus text format  |

### Format by Component

#### Numaflow v1.5.x and below

Prometheus text format across all components

#### Numaflow v1.6 and above

| Component / Endpoint                     | Exposition Format       |
| ---------------------------------------- | ----------------------- |
| Vertex pods (`/metrics`)                 | OpenMetrics text format |
| MonoVertex pods (`/metrics`)             | OpenMetrics text format |
| Serving component (`/metrics`)           | OpenMetrics text format |
| Pipeline Daemon (`/metrics`)             | Prometheus text format  |
| MonoVertex Daemon (`/metrics`)           | Prometheus text format  |
| Controller Manager (`/metrics`)          | Prometheus text format  |

### Key Differences Between Formats

The [Prometheus text-based exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format) and [OpenMetrics text format](https://prometheus.io/docs/instrumenting/exposition_formats/#openmetrics-text-format) are very similar but have subtle differences:

| Feature                  | Prometheus Text Format                                                                                               | OpenMetrics Text Format                                                                                                  |
| ------------------------ |----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| Counter suffix           | `_total` (optional)                                                                                                  | `_total` [required](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#counter-1)        |
| EOF marker               | Not required                                                                                                         | `# EOF` [required](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#overall-structure) |
| Timestamp format         | [Milliseconds](https://prometheus.io/docs/instrumenting/exposition_formats/#comments-help-text-and-type-information) | [Seconds](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#timestamps)                 |
| Info/StateSet types      | Not supported                                                                                                        | [Supported](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#stateset)                 |
| Exemplars                | Not supported                                                                                                        | [Supported](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#exemplars)                |

### Collector Configuration

When collecting metrics from Numaflow, ensure you use the appropriate collector for each component:

- **For dataplane metrics (vertex/monovertex pods):** Use an OpenMetrics-compatible collector
- **For control plane metrics (controller, daemon, ISB):** Use a Prometheus-compatible collector

> **Note:** Most modern Prometheus-compatible systems (including Prometheus itself) can scrape both formats. 
> The format is auto-detected based on the `Content-Type` header. However, some collectors may require explicit configuration
> ([link](https://prometheus.io/docs/instrumenting/content_negotiation/))
