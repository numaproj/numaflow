# Message Headers

Message Headers are metadata key-value pairs attached to each message (also called a datum) as it flows through the Numaflow pipeline.
They provide contextual information about the message—such as event timestamps, routing keys, or custom tags—without modifying the message's main content.
Message headers are immutable and cannot be manipulated through the SDKs.

---

## Use Cases
1. **Propagating Source Metadata**  
   Carry metadata from external systems (like Kafka or HTTP) into the pipeline by copying their headers into message headers.

2. **Tracing and Debugging**  
   Attach unique IDs or trace information to each message, making it easier to follow a message's journey through the pipeline for troubleshooting.

3. **Conditional Routing and Processing**  
   Use header values (such as priority, type, or custom flags) to decide how messages are routed or processed at different steps.

4. **Auditing and Compliance**  
   Store audit information (e.g., who triggered the message, when it was created) in headers for logging and compliance tracking.

5. **Custom Enrichment in UDFs**  
   UDFs can read message headers to enrich processing logic, but they cannot modify or add new headers.

6. **Feature Flags and Experimentation**  
   Pass feature flags or experiment IDs in headers to enable A/B testing or canary deployments within your data processing logic.

---

## Application References
- **Kafka Source Example**  
  In `pkg/sources/kafka/reader.go`, when a message is read from Kafka, all Kafka headers are copied into the Numaflow message headers. This allows downstream components to access all original Kafka metadata.

- **HTTP Source Example**  
  In `pkg/sources/http/http.go`, HTTP headers from incoming requests are added to the message headers. This means any custom or standard HTTP header is available throughout the pipeline.

- **Accessing Headers in UDFs**

  > ⚠️ Note: Headers are read-only in UDFs. You can access them, but not modify or add new headers.

  - **Go SDK**  
    The `datum` object provides a `map[string]string` of all headers:
    ```go
    headers := datum.Headers()
    fmt.Println(headers["X-My-Header"])
    ```

  - **Python SDK**  
    The `datum` object exposes headers as a dictionary:
    ```python
    headers = datum.headers
    print(headers.get("X-My-Header"))
    ```

> Under the hood, header information is preserved and passed through the dataplane, making it accessible across all SDKs.

---

## How Message Headers Work in Numaflow
1. **Creation at the Source**  
   Headers are created at the beginning of the pipeline. For example, Kafka headers or HTTP request headers are copied into the Numaflow message when the source ingests it.

2. **Propagation Through the Pipeline**  
   As the datum flows through the pipeline—from source to UDF to sink—the headers are preserved and passed through each vertex and buffer.

3. **Reading in UDFs**  
   Developers can access headers using the SDKs to make decisions such as filtering, routing, or branching logic. Headers are read-only and cannot be modified or added in UDFs.

4. **Delivery to the Sink**  
   When the datum reaches the sink, the headers are still attached. Sinks can use this data for routing, logging, or integration with external systems.

This end-to-end propagation ensures metadata remains intact and useful across the full lifecycle of a message.

---

## Example Pipeline with Headers (HTTP Source)
Here's a minimal example of a pipeline where HTTP headers are passed from an HTTP source to a UDF, then to a log sink:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-header-demo
spec:
  vertices:
    - name: in
      source:
        http: {}
    - name: process
      udf:
        container:
          # A simple UDF that logs the message and its headers
          image: quay.io/numaproj/numaflow-go/map-log:v1.2.1
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: process
    - from: process
      to: out
```

You can send a request to the HTTP source with custom headers using `curl`. First, port-forward the pod (replace `${pod-name}` with your actual pod name):

```bash
kubectl port-forward pod/${pod-name} 8443:8443
```

Then, send the request:

```bash
curl -k -H "X-My-Header: test-value" -d "hello" https://localhost:8443/vertices/in
```
In your UDF, `X-My-Header` will be available as a message header.
