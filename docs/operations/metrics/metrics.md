# Metrics

Numaflow provides the following prometheus metrics which we can use to monitor our pipeline and setup any alerts if needed.

## Golden Signals

These metrics in combination can be used to determine the overall health of your pipeline

### Traffic

These metrics can be used to determine throughput of your pipeline.

#### Data-forward

| Metric name                   | Metric type | Labels                                                                                                                                                        | Description                                                                                               |
| ----------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `forwarder_data_read_total`   | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages read by a given Vertex from an Inter-Step Buffer Partition          |
| `forwarder_read_bytes_total`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes read by a given Vertex from an Inter-Step Buffer Partition             |
| `forwarder_write_total`       | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages written to Inter-Step Buffer by a given Vertex                      |
| `forwarder_write_bytes_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes written to Inter-Step Buffer by a given Vertex                         |
| `forwarder_ack_total`         | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages acknowledged by a given Vertex from an Inter-Step Buffer Partition  |
| `forwarder_drop_total`        | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of messages dropped by a given Vertex due to a full Inter-Step Buffer Partition |
| `forwarder_drop_bytes_total`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides the total number of bytes dropped by a given Vertex due to a full Inter-Step Buffer Partition    |

#### Kafka Source

| Metric name               | Metric type | Labels                                                 | Description                                                                       |
| ------------------------- | ----------- | ------------------------------------------------------ | --------------------------------------------------------------------------------- |
| `kafka_source_read_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` | Provides the number of messages read by the Kafka Source Vertex/Processor.        |
| `kafka_source_ack_total`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` | Provides the number of messages acknowledged by the Kafka Source Vertex/Processor |

#### Generator Source

| Metric name                 | Metric type | Labels                                                 | Description                                                                    |
| --------------------------- | ----------- | ------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `tickgen_source_read_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` | Provides the number of messages read by the Generator Source Vertex/Processor. |

#### Http Source

| Metric name              | Metric type | Labels                                                 | Description                                                               |
| ------------------------ | ----------- | ------------------------------------------------------ | ------------------------------------------------------------------------- |
| `http_source_read_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` | Provides the number of messages read by the HTTP Source Vertex/Processor. |

#### Kafka Sink

| Metric name              | Metric type | Labels                                                 | Description                                                                |
| ------------------------ | ----------- | ------------------------------------------------------ | -------------------------------------------------------------------------- |
| `kafka_sink_write_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` | Provides the number of messages written by the Kafka Sink Vertex/Processor |

#### Log Sink

| Metric name            | Metric type | Labels                                                 | Description                                                              |
| ---------------------- | ----------- | ------------------------------------------------------ | ------------------------------------------------------------------------ |
| `log_sink_write_total` | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` | Provides the number of messages written by the Log Sink Vertex/Processor |

### Latency

These metrics can be used to determine the latency of your pipeline.

| Metric name                                    | Metric type | Labels                                                                                                                                                        | Description                                                                                    |
| ---------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |------------------------------------------------------------------------------------------------|
| `pipeline_lag_milliseconds` | Gauge| `pipeline=<pipeline-name>` | Provides the pipeline processing lag in milliseconds |
| `watermark_cmp_now_milliseconds` | Gauge| `pipeline=<pipeline-name>` | Provides the Watermark compared with current time in milliseconds |
| `source_forwarder_transformer_processing_time` | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Provides a histogram distribution of the processing times of User-defined Source Transformer   |
| `forwarder_udf_processing_time`                | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>`                                        | Provides a histogram distribution of the processing times of User-defined Functions. (UDF's)   |
| `forwarder_forward_chunk_processing_time`      | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>`                                        | Provides a histogram distribution of the processing times of the forwarder function as a whole |
| `reduce_pnf_process_time`                      | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `replica=<replica-index>`                                                                         | Provides a histogram distribution of the processing times of the reducer                       |
| `reduce_pnf_forward_time`                      | Histogram   | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `replica=<replica-index>`                                                                         | Provides a histogram distribution of the forwarding times of the reducer                       |

### Errors

These metrics can be used to determine if there are any errors in the pipeline

| Metric name                       | Metric type | Labels                                                                                                                                                        | Description                                                        |
| --------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `forwarder_platform_error_total`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>`                                        | Indicates any internal errors which could stop pipeline processing |
| `forwarder_read_error_total`      | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while reading messages by the forwarder       |
| `forwarder_write_error_total`     | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` `vertex_type=<vertex-type>` <br> <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while writing messages by the forwarder       |
| `forwarder_ack_error_total`       | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>` <br> `vertex_type=<vertex-type>` <br> `replica=<replica-index>` <br> `partition_name=<partition-name>` | Indicates any errors while acknowledging messages by the forwarder |
| `kafka_source_offset_ack_errors`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`                                                                                                        | Indicates any kafka acknowledgement errors                         |
| `kafka_sink_write_error_total`    | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`                                                                                                        | Provides the number of errors while writing to the Kafka sink      |
| `kafka_sink_write_timeout_total`  | Counter     | `pipeline=<pipeline-name>` <br> `vertex=<vertex-name>`                                                                                                        | Provides the write timeouts while writing to the Kafka sink        |
| `isb_jetstream_read_error_total`  | Counter     | `partition_name=<partition-name>`                                                                                                                             | Indicates any read errors with NATS Jetstream ISB                  |
| `isb_jetstream_write_error_total` | Counter     | `partition_name=<partition-name>`                                                                                                                             | Indicates any write errors with NATS Jetstream ISB                 |
| `isb_redis_read_error_total`      | Counter     | `partition_name=<partition-name>`                                                                                                                             | Indicates any read errors with Redis ISB                           |
| `isb_redis_write_error_total`     | Counter     | `partition_name=<partition-name>`                                                                                                                             | Indicates any write errors with Redis ISB                          |

### Saturation

#### NATS JetStream ISB

| Metric name                        | Metric type | Labels                 | Description                                                                                                                                  |
| ---------------------------------- | ----------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `isb_jetstream_isFull_total`       | Counter     | `buffer=<buffer-name>` | Indicates if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline |
| `isb_jetstream_buffer_soft_usage`  | Gauge       | `buffer=<buffer-name>` | Indicates the usage/utilization of a NATS Jetstream ISB                                                                                      |
| `isb_jetstream_buffer_solid_usage` | Gauge       | `buffer=<buffer-name>` | Indicates the solid usage of a NATS Jetstream ISB                                                                                            |
| `isb_jetstream_buffer_pending`     | Gauge       | `buffer=<buffer-name>` | Indicate the number of pending messages at a given point in time.                                                                            |
| `isb_jetstream_buffer_ack_pending` | Gauge       | `buffer=<buffer-name>` | Indicates the number of messages pending acknowledge at a given point in time                                                                |

#### Redis ISB

| Metric name              | Metric type | Labels                 | Description                                                                                                                                  |
| ------------------------ | ----------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `isb_redis_isFull_total` | Counter     | `buffer=<buffer-name>` | Indicates if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline |
| `isb_redis_buffer_usage` | Gauge       | `buffer=<buffer-name>` | Indicates the usage/utilization of a Redis ISB                                                                                               |
| `isb_redis_consumer_lag` | Gauge       | `buffer=<buffer-name>` | Indicates the the consumer lag of a Redis ISB                                                                                                |

## Prometheus Operator for Scraping Metrics:

You can follow the [prometheus operator](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/user-guides/getting-started.md) setup guide if you would like to use prometheus operator configured in your cluster.

You can also set up prometheus operator via [helm](https://bitnami.com/stack/prometheus-operator/helm).

### Configure the below Service Monitors for scraping your pipeline metrics:

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
