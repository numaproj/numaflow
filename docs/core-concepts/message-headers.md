# Message Headers

Message Headers are metadata key-value pairs attached to each message (also called a *datum*) as it flows through the Numaflow pipeline. They provide contextual information about the message — such as event timestamps, routing keys, or custom tags — without modifying the message’s main content.

Headers allow components like User-Defined Functions (UDFs), sources, and sinks to make informed processing decisions, perform filtering, enable conditional routing, or log events with context.

---

## Use Cases

1. **Propagating Source Metadata**  
   Carry metadata from external systems (like Kafka or HTTP) into the pipeline by copying their headers into message headers.

2. **Tracing and Debugging**  
   Attach unique IDs or trace information to each message, making it easier to follow a message’s journey through the pipeline for troubleshooting.

3. **Conditional Routing and Processing**  
   Use header values (such as priority, type, or custom flags) to decide how messages are routed or processed at different steps.

4. **Auditing and Compliance**  
   Store audit information (e.g., who triggered the message, when it was created) in headers for logging and compliance tracking.

5. **Custom Enrichment in UDFs**  
   UDFs can read, modify, or add headers to enrich messages with new metadata or processing results.

6. **Feature Flags and Experimentation**  
   Pass feature flags or experiment IDs in headers to enable A/B testing or canary deployments within your data processing logic.

---

## Application References

- **Kafka Source Example**  
  In `pkg/sources/kafka/reader.go`, when a message is read from Kafka, all Kafka headers are copied into the Numaflow message headers. This allows downstream components to access all original Kafka metadata.

- **HTTP Source Example**  
  In `pkg/sources/http/http.go`, HTTP headers from incoming requests are added to the message headers. This means any custom or standard HTTP header is available throughout the pipeline.

- **Accessing Headers in UDFs**

  - **Go SDK**  
    The `datum` object gives you a `map[string]string` of all headers.
    ```go
    headers := datum.Headers()
    fmt.Println(headers["__key"])
    ```

  - **Python SDK**  
    The `datum` object exposes headers as a dictionary.
    ```python
    headers = datum.headers
    print(headers.get("__key"))
    ```

> Under the hood, both SDKs retrieve this header information from the Go backend, where core pipeline logic runs.

---

## How Message Headers Work in Numaflow

1. **Creation at the Source**  
   Headers are created at the beginning of the pipeline. For example, Kafka headers or HTTP request headers are copied into the Numaflow message when the source ingests it.

2. **Propagation Through the Pipeline**  
   As the datum flows through the pipeline—from source to UDF to sink—the headers are preserved and passed through each vertex and buffer unless explicitly changed.

3. **Reading and Modifying in UDFs**  
   Developers can access and modify headers using the SDKs. This enables logic like tagging, enrichment, or selective processing based on metadata.

4. **Delivery to the Sink**  
   When the datum reaches the sink, the headers are still attached. Sinks can use this data for routing, logging, or integration with other systems.

This end-to-end propagation ensures metadata remains intact and useful across the full lifecycle of a message.

---

## Example Pipeline with Headers

Here’s a minimal example of a pipeline where message headers are passed from a Kafka source to a UDF, then to a log sink.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: header-demo-pipeline
spec:
  vertices:
    - name: in
      source:
        kafka:
          brokers:
            - my-kafka-broker:9092
          topic: my-topic
    - name: process
      udf:
        container:
          image: my-udf-image
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: process
    - from: process
      to: out
