# User-defined Sinks

Numaflow provides builtin Sinks but there are many use cases where an user might want to write a custom Sink implementation, and User Defined Sink (`udsink`) can be used to write those custom Sinks.


A pre-defined sink vertex runs single-container pods, while a user-defined sink runs two-container pods.

> **Related Topics:**
> - [Sinks Overview](./overview.md) - General information about sinks
> - [Fallback Sink](./fallback.md) - Dead Letter Queue (DLQ) functionality
> - [Retry Strategy](./retry-strategy.md) - Configuring retry behavior for failed messages

## Build Your Own User-defined Sinks

You can build your own user-defined sinks in multiple languages. Check the SDK examples section below for detailed implementation patterns and working examples.

## Message–Response Contract

When developing a user-defined sink in Numaflow, your sink container communicates with the sidecar via an ordered batches of messages. For each batch of messages received, your sink must return a list of responses that strictly adheres to the following contract:

### Core Rules:

1. **Response Count:**
   - For every batch of `N` input messages received, your sink **must return exactly** `N` responses.

2. **Response Matching:**
   - Each response must include the corresponding message ID (`datumID`) to identify which message it corresponds to.
   - The order of responses does **not** need to match the order of incoming messages.
   - Every input message must have exactly one matching response.

3. **Response Status:**
   - Each response must indicate how the message was handled by returning one of the following statuses:
     - `OK`
     - `FAILURE`
     - `FALLBACK`
     - `SERVE`

### Response Types

#### **OK** (`ResponseOK`)
- **Meaning:** Message was successfully processed and written to the sink.
- **Action:** Message is acknowledged and removed from the pipeline.
- **Use Case:** Normal successful operations.

#### **FAILURE** (`ResponseFailure`)
- **Meaning:** Permanent error occurred during processing.
- **Action:** Message will be dropped from the pipeline (no retry).
- **Requirement:** Must include a descriptive error message.
- **Use Case:** Invalid data, unsupported formats, or unrecoverable failures.
- **Related:** See [Retry Strategy](./retry-strategy.md) for configuring retry behavior.

#### **FALLBACK** (`ResponseFallback`)
- **Meaning:** Message should be routed to the fallback sink (dead-letter queue).
- **Action:** Message is immediately sent to the configured fallback sink without retry.
- **Requirements:** 
  - A fallback sink must be configured in the pipeline spec.
  - Fallback responses **must not** return further fallback or serve statuses.
- **Use Case:** Messages that cannot be processed by the primary sink but should be preserved.
- **Related:** See [Fallback Sink](./fallback.md) for detailed configuration.

#### **SERVE** (`ResponseServe`)
- **Meaning:** Message should be stored in the serving store for later retrieval.
- **Action:** Message is stored in the configured serving store (e.g., NATS or user-defined).
- **Requirements:** Serving store must be configured in the pipeline spec.
- **Use Case:** Storing results for serving pipelines or caching responses.

###  Example

If the input batch is:

```json
[
  {"id": "d1", "value": "event 1"},
  {"id": "d2", "value": "event 2"},
  {"id": "d3", "value": "event 3"}
]
```

Then your response must be:

```json
[
  {"id": "d1", "status": "OK"},
  {"id": "d2", "status": "FAILURE", "err_msg": "Invalid format"},
  {"id": "d3", "status": "FALLBACK"}
]
```

###  Important Assumptions & Constraints

#### **Message ID Uniqueness:**
- Within a single sink iterator, no duplicate datumIDs exist.
- Message IDs are unique across the entire pipeline for correct tracking.

#### **Response Validation:**
- Fallback sink responses must not return fallback or serve statuses.
- Error messages are required for FAILURE responses and optional for others.

#### **Retry Behavior:**
- FAILURE responses follow the configured retry strategy if enabled.
- FALLBACK responses bypass retry and go directly to the fallback sink.
- OK responses are not retried.

#### **Batch Processing:**
- Messages are processed in batches for efficiency.
- All messages in a batch must be responded to before the next batch.
- Incorrect response counts or missing IDs will cause stalling of processing.

#### **Idempotency:**
- Since retries can occur for FAILURE statuses, your sink implementation must be idempotent to avoid duplicated side effects.

### SDK Methods

Check the links below to see the examples for different languages.

**Golang:**
```go
ResponseOK(id string)
ResponseFailure(id string, errMsg string)
ResponseFallback(id string)
ResponseServe(id string, serveResponse []byte)
```

**Java:**
```java
Response.ok(id)
Response.failure(id, errMsg)
Response.fallback(id)
Response.serve(id, serveResponse)
```

**Python:**
```python
Response.ok(id)
Response.failure(id, err_msg)
Response.fallback(id)
Response.serve(id, serve_response)
```

###  Explore SDK Examples

For more detailed examples and implementation patterns, check out the official SDK repositories:

- **[Golang SDK Examples](https://github.com/numaproj/numaflow-go/tree/main/examples/sinker)**
- **[Java SDK Examples](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sink/simple/)**
- **[Python SDK Examples](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink/)**

These repositories contain complete working examples of user-defined sinks with various response types and error handling patterns.

### **Reminder**

Always validate that the number of responses equals the number of incoming messages before returning your response to the sidecar. Incorrect response counts or missing IDs will stall the pipeline.

## Example User-Defined Sink Vertex Configuration

```yaml
spec:
  vertices:
    - name: output
      sink:
        udsink:
          container:
            image: my-sink:latest
```

## Available Environment Variables

Some environment variables are available in the user-defined sink container:

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

## User-Defined Sinks Contributed by the Open Source Community

If you're looking for examples and usages contributed by the open source community, head over to [the numaproj-contrib repositories](https://github.com/orgs/numaproj-contrib/repositories).

These user-defined sinks, like AWS SQS and GCP Pub/Sub, provide valuable insights and guidance on how to use and write a user-defined sink.