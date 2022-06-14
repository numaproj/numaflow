# Metrics

NumaFlow provides the following prometheus metrics which we can use to monitor our pipeline and setup any alerts if needed.

## Golden Signals
These metrics in combination can be used to determine the overall health of your pipeline

### Traffic
These metrics can be used to determine throughput of your pipeline.

#### Data-forward


| Metric name             | Metric type | Labels                                                                                               | Description                                                                                    |
|-------------------------|-------------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `forwarder_read_total`  | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides the total number of messages read by a given Vertex from an Inter-Step Buffer         |
| `forwarder_write_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides the total number of messages written to Inter-Step Buffer by a given Vertex           |
| `forwarder_ack_total`   | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides the total number of messages acknowledged by a given Vertex from an Inter-Step Buffer |

#### Kafka Source

| Metric name               | Metric type | Labels                                                               | Description                                                                       |
|---------------------------|-------------|----------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `kafka_source_read_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;   | Provides the number of messages read by the Kafka Source Vertex/Processor.        |
| `kafka_source_ack_total`  | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;   | Provides the number of messages acknowledged by the Kafka Source Vertex/Processor |


#### Generator Source

| Metric name                 | Metric type | Labels                                                             | Description                                                                    |
|-----------------------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `tickgen_source_read_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; | Provides the number of messages read by the Generator Source Vertex/Processor. |

#### Http Source

| Metric name              | Metric type | Labels                                                              | Description                                                               |
|--------------------------|-------------|---------------------------------------------------------------------|---------------------------------------------------------------------------|
| `http_source_read_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;  | Provides the number of messages read by the HTTP Source Vertex/Processor. |


#### Kafka Sink

| Metric name              | Metric type | Labels                                                              | Description                                                                |
|--------------------------|-------------|---------------------------------------------------------------------|----------------------------------------------------------------------------|
| `kafka_sink_write_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;  | Provides the number of messages written by the Kafka Sink Vertex/Processor |


#### Log Sink

| Metric name            | Metric type | Labels                                                              | Description                                                              |
|------------------------|-------------|---------------------------------------------------------------------|--------------------------------------------------------------------------|
| `log_sink_write_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;  | Provides the number of messages written by the Log Sink Vertex/Processor |

### Latency 
These metrics can be used to determine the latency of your pipeline.

| Metric name                               | Metric type | Labels                                                                                               | Description                                                                                    |
|-------------------------------------------|-------------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `forwarder_udf_processing_time`           | Histogram   | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides a histogram distribution of the processing times of User Defined Functions. (UDF's)   |
| `forwarder_forward_chunk_processing_time` | Histogram   | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides a histogram distribution of the processing times of the forwarder function as a whole |


### Errors
These metrics can be used to determine if there are any errors in the pipeline


| Metric name                       | Metric type | Labels                                                                                               | Description                                                        |
|-----------------------------------|-------------|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `forwarder_platform_error`        | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Indicates any internal errors which could stop pipeline processing |
| `forwarder_read_error`            | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Indicates any errors while reading messages by the forwarder       |
| `forwarder_write_error`           | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Indicates any errors while writing messages by the forwarder       |
| `forwarder_ack_error`             | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Indicates any errors while acknowledging messages by the forwarder |
| `kafka_source_offset_ack_errors`  | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;                                   | Indicates any kafka acknowledgement errors                         |
| `kafka_sink_write_error_total`    | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;                                   | Provides the number of errors while writing to the Kafka sink      |
| `kafka_sink_write_timeout_total`  | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt;                                   | Provides the write timeouts  while writing to the Kafka sink       |
| `isb_jetstream_read_error_total`  | Counter     | `buffer`=&lt;buffer-name&gt;                                                                         | Indicates any read errors with NATS Jetstream ISB                  |
| `isb_jetstream_write_error_total` | Counter     | `buffer`=&lt;buffer-name&gt;                                                                         | Indicates any write errors  with NATS Jetstream ISB                |
| `isb_redis_read_error_total`      | Counter     | `buffer`=&lt;buffer-name&gt;                                                                         | Indicates any read errors with Redis ISB                           |
| `isb_redis_write_error_total`     | Counter     | `buffer`=&lt;buffer-name&gt;                                                                         | Indicates any write errors with Redis ISB                          |

### Saturation

#### NATS JetStream ISB

| Metric name                        | Metric type | Labels                       | Description                                                                                                                                  |
|------------------------------------|-------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `isb_jetstream_isFull_total`       | Counter     | `buffer`=&lt;buffer-name&gt; | Indicates if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline |
| `isb_jetstream_buffer_soft_usage`  | Gauge       | `buffer`=&lt;buffer-name&gt; | Indicates the usage/utilization of a NATS Jetstream ISB                                                                                      |
| `isb_jetstream_buffer_solid_usage` | Gauge       | `buffer`=&lt;buffer-name&gt; | Indicates the solid usage of a NATS Jetstream ISB                                                                                            |
| `isb_jetstream_buffer_pending`     | Gauge       | `buffer`=&lt;buffer-name&gt; | Indicate the number of pending messages at a given point in time.                                                                            |
| `isb_jetstream_buffer_ack_pending` | Gauge       | `buffer`=&lt;buffer-name&gt; | Indicates the number of messages pending acknowledge at a given point in time                                                                |


#### Redis ISB

| Metric name              | Metric type | Labels                       | Description                                                                                                                                  |
|--------------------------|-------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `isb_redis_isFull_total` | Counter     | `buffer`=&lt;buffer-name&gt; | Indicates if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline |
| `isb_redis_buffer_usage` | Gauge       | `buffer`=&lt;buffer-name&gt; | Indicates the usage/utilization of a Redis ISB                                                                                               |
| `isb_redis_consumer_lag` | Gauge       | `buffer`=&lt;buffer-name&gt; | Indicates the the consumer lag of a Redis ISB                                                                                                |






