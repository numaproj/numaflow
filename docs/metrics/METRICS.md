# Metrics

NumaFlow provides the following prometheus metrics which we can use to monitor our pipeline and setup any alerts if needed.

## Golden Signals
These metrics in combination can be used to determine the overall health of your pipeline

### Traffic
These metrics can be used to determine throughput of your pipeline.

#### Data-forward

| Metric name| Metric type | Labels/tags | Description                                                                                    |
| ---------- |-------------| ----------- |------------------------------------------------------------------------------------------------|
| `forwarder_read_total`  | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides the total number of messages read by a given Vertex from an Inter-Step Buffer         |
| `forwarder_write_total` | Counter     | `vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides the total number of messages written to Inter-Step Buffer by a given Vertex           |
| `forwarder_ack_total` | Counter     |`vertex`=&lt;vertex-name&gt; <br> `pipeline`=&lt;pipeline-name&gt; <br> `buffer`=&lt;buffer-name&gt; | Provides the total number of messages acknowledged by a given Vertex from an Inter-Step Buffer |

#### Kafka Source
* `kafka_source_read_total`: This metric is used to provide the number of messages read by the Kafka Source Vertex/Processor
* `kafka_source_ack_total`: This metric is used to provide the number of messages acknowledged by the Kafka Source Vertex/Processor

#### Generator Source
* `tickgen_source_read_total`: This metric is used to indicate the number of messages read by the generator source processor/vertex

#### Http Source
* `http_source_read_total`: This metric is used to indicate the number of messages read by the http source processor/vertex

#### Kafka Sink
* `kafka_sink_write_total`: This metric is used to provide the number of messages written by the Kafka Sink Vertex/Processor

#### Log Sink
* `log_sink_write_total`: This metric is used to provide the number of messages written by the Log Sink Vertex/Processor

### Latency 
These metrics can be used to determine the latency of your pipeline.

* `forwarder_udf_processing_time`: This metric is used to provide a histogram distribution of the processing times of User Defined Functions. (UDF's)
* `forwarder_forward_chunk_processing_time`: This metric is used to provide a histogram distribution of the processing times of the forwarder function as a whole

### Errors
These metrics can be used to determine if there are any errors in the pipeline

* `forwarder_platform_error`: This metric is used to indicate any internal errors which could cause the pipeline to stop processing.
* `forwarder_read_error`: This metric is used to indicate any errors while reading messages.
* `forwarder_write_error`: This metric is used to indicate any errors while writing messages
* `forwarder_ack_error`: This metric is used to indicate any errors while acknowledging messages.
* `kafka_source_offset_ack_errors`: This metric is used to indicate the kafka offset acknowledgment errors.
* `kafka_sink_write_error_total`: This metric is used to provide the number of errors while while writing to the Kafka Sink.
* `kafka_sink_write_timeout_total`: This metric is used to provide the write timeouts while writing to the Kafka Sink.
* `isb_jetstream_read_error_total`: This metric is used to indicate any read errors with Jetstream.
* `isb_jetstream_write_error_total`: This metric is used to indicate any write errors with Jetstream.
* `isb_redis_read_error_total`: This metric is used to indicate any read errors with Jetstream.
* `isb_redis_write_error_total`: This metric is used to indicate any write errors with Jetstream.

### Saturation

#### NATS JetStream ISB
* `isb_jetstream_isFull_total`: This metric is used to indicate if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline
* `isb_jetstream_buffer_soft_usage`: This metric is a gauge which is used to indicate the usage/utilization of a NATS Jetstream ISB
* `isb_jetstream_buffer_solid_usage`: This metric is a gauge which is used to indicate the solid usage of a NATS Jetstream ISB
* `isb_jetstream_buffer_pending`: This metric is a gauge which is used to indicate the number of pending messages at a given point in time.
* `isb_jetstream_buffer_ack_pending`: This metric is a gauge which is used to indicate the number of messages pending acknowledge at a given point in time.

#### Redis ISB
* `isb_redis_isFull_total`: This metric is used to indicate if the ISB is full. Continual increase of this counter metric indicates a potential backpressure that can be built on the pipeline
* `isb_redis_buffer_usage`: This metric is a gauge which is used to indicate the usage/utilization of a Redis ISB
* `isb_redis_consumer_lag`: This metric is a gauge which is used to indicate the consumer lag of a Redis ISB Buffer





