# Metrics

Numaflow provides the following prometheus metrics which we can use to monitor our pipeline and setup any alerts if needed.

## Golden Signals

These metrics in combination can be used to determine the overall health of your pipeline

### Traffic

These metrics can be used to determine throughput of your pipeline.

| Metric name                                | Metric type | Labels                                                                                                                                                        | Description                                                                                                     |
| ------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `forwarder_read_total`                     | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages read by a given Vertex from an Inter-Step Buffer Partition or Source      |
| `forwarder_data_read_total`                | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of data messages read by a given Vertex from an Inter-Step Buffer Partition or Source |
| `forwarder_read_bytes_total`               | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes read by a given Vertex from an Inter-Step Buffer Partition or Source         |
| `forwarder_data_read_bytes_total`          | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of data bytes read by a given Vertex from an Inter-Step Buffer Partition or Source    |
| `source_forwarder_transformer_read_total`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=Source` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>`        | Provides the total number of messages read by source transformer                                                |
| `forwarder_write_total`                    | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages written to Inter-Step Buffer by a given Vertex                            |
| `forwarder_write_bytes_total`              | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes written to Inter-Step Buffer by a given Vertex                               |
| `source_forwarder_transformer_write_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=Source` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>`        | Provides the total number of messages written by source transformer                                             |
| `forwarder_fbsink_write_total`             | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages written to a fallback sink                                                |
| `forwarder_fbsink_write_bytes_total`       | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes written to a fallback sink                                                   |
| `forwarder_ack_total`                      | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages acknowledged by a given Vertex from an Inter-Step Buffer Partition        |
| `forwarder_drop_total`                     | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages dropped by a given Vertex due to a full Inter-Step Buffer Partition       |
| `forwarder_drop_bytes_total`               | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes dropped by a given Vertex due to a full Inter-Step Buffer Partition          |
| `forwarder_udf_read_total`                 | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages read by UDF                                                               |
| `forwarder_udf_write_total`                | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages written by UDF                                                            |

### Latency

These metrics can be used to determine the latency of your pipeline.

| Metric name                                    | Metric type | Labels                                                                                                                                                        | Description                                                                                                    |
| ---------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `pipeline_processing_lag`                      | Gauge       | `pipeline=<pipeline-name>`                                                                                                                                    | Pipeline processing lag in milliseconds (max watermark - min watermark)                                        |
| `pipeline_watermark_cmp_now`                   | Gauge       | `pipeline=<pipeline-name>`                                                                                                                                    | Max watermark of source compared with current time in milliseconds                                             |
| `forwarder_read_processing_time`               | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the histogram distribution of the processing times of read operations                                 |
| `forwarder_write_processing_time`              | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the histogram distribution of the processing times of write operations                                |
| `forwarder_ack_processing_time`                | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the histogram distribution of the processing times of ack operations                                  |
| `forwarder_fbsink_write_processing_time`       | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the histogram distribution of the processing times of write operations to a fallback sink             |
| `source_forwarder_transformer_processing_time` | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=Source` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>`        | Provides a histogram distribution of the processing times of source transformer                                |
| `forwarder_udf_processing_time`                | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>`                                        | Provides a histogram distribution of the processing times of User-defined Functions (UDFs) in a map vertex     |
| `forwarder_forward_chunk_processing_time`      | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>`                                        | Provides a histogram distribution of the processing times of the forwarder function as a whole in a map vertex |
| `reduce_data_forward_forward_time`             | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `replica=<replica-index>`                                                                         | Provides a histogram distribution of the forwarding times of the data from an ISB to a PBQ                     |
| `reduce_pbq_write_time`                        | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `replica=<replica-index>`                                                                         | Provides a histogram distribution of the processing times of write operations to a PBQ                         |
| `reduce_pnf_process_time`                      | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `replica=<replica-index>`                                                                         | Provides a histogram distribution of the processing times of the reducer                                       |
| `vertex_pending_messages`                      | Gauge       | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `period=<duration>` <br> `partition_name=<partition-name>`                                        | Provides the average pending messages in the last period of seconds. It is the pending messages of a vertex    |

### Errors

These metrics can be used to determine if there are any errors in the pipeline.

| Metric name                                | Metric type | Labels                                                                                                                                                        | Description                                                                                     |
| ------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `pipeline_data_processing_health`          | Gauge       | `pipeline=<pipeline-name>`                                                                                                                                    | Pipeline data processing health status. 1: Healthy, 0: Unknown, -1: Warning, -2: Critical       |
| `controller_isbsvc_health`                 | Gauge       | `ns=<namespace>` <br> `isbsvc=<isbsvc-name>`                                                                                                                  | A metric to indicate whether the ISB Service is healthy. '1' means healthy, '0' means unhealthy |
| `controller_pipeline_health`               | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline-name>`                                                                                                              | A metric to indicate whether the Pipeline is healthy. '1' means healthy, '0' means unhealthy    |
| `controller_monovtx_health`                | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx-name>`                                                                                                                 | A metric to indicate whether the MonoVertex is healthy. '1' means healthy, '0' means unhealthy  |
| `forwarder_platform_error_total`           | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>`                                        | Indicates any internal errors which could stop pipeline processing                              |
| `forwarder_read_error_total`               | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while reading messages by the forwarder                                    |
| `source_forwarder_transformer_error_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=Source` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>`        | Indicates source transformer errors                                                             |
| `forwarder_write_error_total`              | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` `vertex_type=<vertex-type>` <br> <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while writing messages by the forwarder                                    |
| `forwarder_fbsink_write_error_total`       | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` `vertex_type=<vertex-type>` <br> <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while writing to a fallback sink                                           |
| `forwarder_ack_error_total`                | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while acknowledging messages by the forwarder                              |
| `kafka_sink_write_timeout_total`           | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`                                                                                                        | Provides the write timeouts while writing to the Kafka sink                                     |
| `isb_jetstream_read_error_total`           | Counter     | `buffer=<buffer-name>`                                                                                                                                        | Indicates any read errors with NATS Jetstream ISB                                               |
| `isb_jetstream_write_error_total`          | Counter     | `buffer=<buffer-name>`                                                                                                                                        | Indicates any write errors with NATS Jetstream ISB                                              |

### Saturation

#### NATS JetStream ISB

| Metric name                        | Metric type | Labels                 | Description                                                                                                                                  |
| ---------------------------------- | ----------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `isb_jetstream_isFull_total`       | Counter     | `buffer=<buffer-name>` | Indicates if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline |
| `isb_jetstream_buffer_soft_usage`  | Gauge       | `buffer=<buffer-name>` | Indicates the usage/utilization of a NATS Jetstream ISB                                                                                      |
| `isb_jetstream_buffer_solid_usage` | Gauge       | `buffer=<buffer-name>` | Indicates the solid usage of a NATS Jetstream ISB                                                                                            |
| `isb_jetstream_buffer_pending`     | Gauge       | `buffer=<buffer-name>` | Indicate the number of pending messages at a given point in time.                                                                            |
| `isb_jetstream_buffer_ack_pending` | Gauge       | `buffer=<buffer-name>` | Indicates the number of messages pending acknowledge at a given point in time                                                                |

### Others

| Metric name                           | Metric type | Labels                                                                                                                                            | Description                                                                                                                                                                              |
| ------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build_info`                          | Gauge       | `component=<component>` <br> `component_name=<component_name>` <br> `version=<version>` <br> `platform=<platform>`                                | A metric with a constant value '1', labeled by Numaflow binary version, platform, and other information. The value of `component` could be 'daemon', 'vertex', 'mono-vertex-daemon', etc |
| `controller_build_info`               | Gauge       | `version=<version>` <br> `platform=<platform>`                                                                                                    | A metric with a constant value '1', labeled with controller version and platform from which Numaflow was built                                                                           |
| `sdk_info`                            | Gauge       | `component=<component>` <br> `component_name=<component_name>` <br> `type=<sdk_type>` <br> `version=<sdk_version>` <br> `language=<sdk_language>` | A metric with a constant value '1', labeled by SDK information such as version, language, and type                                                                                       |
| `controller_pipeline_desired_phase`   | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline>`                                                                                                       | A metric to indicate the pipeline phase. '1' means Running, '2' means Paused                                                                                                             |
| `controller_pipeline_current_phase`   | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline>`                                                                                                       | A metric to indicate the pipeline phase. '0' means Unknown, '1' means Running, '2' means Paused, '3' means Failed, '4' means Pausing, '5' means 'Deleting'                               |
| `controller_monovtx_desired_phase`    | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx>`                                                                                                          | A metric to indicate the MonoVertex phase. '1' means Running, '2' means Paused                                                                                                           |
| `controller_monovtx_current_phase`    | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx>`                                                                                                          | A metric to indicate the MonoVertex phase. '0' means Unknown, '1' means Running, '2' means Paused, '3' means Failed                                                                      |
| `controller_vertex_desired_replicas`  | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline>` <br> `vertex=<vertex>`                                                                                | A metric indicates the desired replicas of a Vertex                                                                                                                                      |
| `controller_vertex_current_replicas`  | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline>` <br> `vertex=<vertex>`                                                                                | A metric indicates the current replicas of a Vertex                                                                                                                                      |
| `controller_vertex_min_replicas`      | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline>` <br> `vertex=<vertex>`                                                                                | A metric indicates the min replicas of a Vertex                                                                                                                                          |
| `controller_vertex_max_replicas`      | Gauge       | `ns=<namespace>` <br> `pipeline=<pipeline>` <br> `vertex=<vertex>`                                                                                | A metric indicates the max replicas of a Vertex                                                                                                                                          |
| `controller_monovtx_desired_replicas` | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx>`                                                                                                          | A metric indicates the desired replicas of a MonoVertex                                                                                                                                  |
| `controller_monovtx_current_replicas` | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx>`                                                                                                          | A metric indicates the current replicas of a MonoVertex                                                                                                                                  |
| `controller_monovtx_min_replicas`     | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx>`                                                                                                          | A metric indicates the min replicas of a MonoVertex                                                                                                                                      |
| `controller_monovtx_max_replicas`     | Gauge       | `ns=<namespace>` <br> `mvtx_name=<mvtx>`                                                                                                          | A metric indicates the max replicas of a MonoVertex                                                                                                                                      |

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

### Configure the below Service Monitor if you use the NATS Jetstream ISB for your NATS Jetstream metrics:

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
