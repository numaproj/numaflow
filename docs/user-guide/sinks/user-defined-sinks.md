# User-defined Sinks

Numaflow provides builtin Sinks but there are many use cases where an user might want to write a custom Sink implementation, and User Defined Sink (`udsink`) can be used to write those custom Sinks.


A pre-defined sink vertex runs single-container pods, while a user-defined sink runs two-container pods.

> **Related Topics:**
> - [Sinks Overview](./overview.md) - General information about sinks
> - [Fallback Sink](./fallback.md) - Dead Letter Queue (DLQ) functionality
> - [Retry Strategy](./retry-strategy.md) - Configuring retry behavior for failed messages
> - [OnSuccess Sink](./on-success.md) - Sink to receive successful message processing notifications

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
        - `ON_SUCCESS`

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
  - Fallback responses **must not** return further fallback, on-success or serve statuses.
- **Use Case:** Messages that cannot be processed by the primary sink but should be preserved.
- **Related:** See [Fallback Sink](./fallback.md) for detailed configuration.

#### **ON_SUCCESS** (`ResponseOnSuccess`)
- **Meaning:** Message should be routed to the on-success sink.
- **Action:** Message is immediately sent to the configured on-success sink without retry.
- **Requirements:**
    - An on-success sink must be configured in the pipeline spec.
    - OnSuccess responses **must not** return further fallback, on-success, or serve statuses.
- **Use Case:** Notify downstream through on-success sink of the successful processing of a message (e.g., send a kafka message after writing to the primary DB sink) .
- **Related:** See [OnSuccess Sink](./on-success.md) for detailed configuration.

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

=== "Golang"

    ```go
    ResponseOK(id string)
    ResponseFailure(id string, errMsg string)
    ResponseFallback(id string)
    ResponseServe(id string, serveResponse []byte)
    ResponseOnSuccess(id string, onSuccessMessage Message)
    ```
    [Golang SDK Examples](https://github.com/numaproj/numaflow-go/tree/main/examples/sinker)

=== "Java"
    
    ```java
    Response.responseOK(id)
    Response.responseFailure(id, errMsg)
    Response.responseFallback(id)
    Response.responseServe(id, serveResponse)
    Response.responseOnSuccess(id, onSuccessMessage)
    ```
    [Java SDK Examples](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sink/simple/)

=== "Python"

    ```python
    Response.as_success(id)
    Response.as_failure(id, err_msg)
    Response.as_fallback(id)
    Response.as_serve(id, serve_response)
    Response.as_on_success(id, message)
    ```
    [Python SDK Examples](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink/)

=== "Rust"

    ```rust
    Response::ok(id)
    Response::failure(id, err_msg)
    Response::fallback(id)
    Response::serve(id, serve_response)
    Response::on_success(id, payload)
    ```
    [Rust SDK Examples](https://github.com/numaproj/numaflow-rs/tree/main/examples)

=== "JS/TS"

    ```typescript
    Response.ok(id)
    Response.failure(id, err_msg)
    Response.fallback(id)
    Response.serve(id, serve_response)
    Response.onSuccess(id, payload)
    ```
    [Javascript SDK Examples](https://github.com/numaproj/numaflow-js/tree/main/examples/sinker)

###  Explore SDK Examples

A user-defined sink implements a single handler that receives a batch of messages and returns one response per
message. The snippets below are trimmed from the log sink example of each SDK: every message is logged and
acknowledged with an `OK` response. Follow the links for the full examples.

=== "Golang"

    ```go
    // logSink is a sinker implementation that logs the input to stdout
    type logSink struct {
    }

    func (l *logSink) Sink(ctx context.Context, datumStreamCh <-chan sinksdk.Datum) sinksdk.Responses {
        result := sinksdk.ResponsesBuilder()
        for d := range datumStreamCh {
            fmt.Println("User Defined Sink:", string(d.Value()))
            id := d.ID()
            result = result.Append(sinksdk.ResponseOK(id))
        }
        return result
    }

    func main() {
        err := sinksdk.NewServer(&logSink{}).Start(context.Background())
        if err != nil {
            log.Panic("Failed to start sink function server: ", err)
        }
    }
    ```
    [View the full example on numaflow-go GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/sinker/log)

=== "Java"

    ```java
    @Slf4j
    public class SimpleSink extends Sinker {

        public static void main(String[] args) throws Exception {
            Server server = new Server(new SimpleSink());
            server.start();
            server.awaitTermination();
        }

        @Override
        public ResponseList processMessages(DatumIterator datumIterator) {
            ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
            while (true) {
                Datum datum = null;
                try {
                    datum = datumIterator.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                // null means the iterator is closed, so we break the loop
                if (datum == null) {
                    break;
                }
                try {
                    String msg = new String(datum.getValue());
                    log.info("Received message: {}, headers - {}", msg, datum.getHeaders());
                    responseListBuilder.addResponse(Response.responseOK(datum.getId()));
                } catch (Exception e) {
                    responseListBuilder.addResponse(Response.responseFailure(
                            datum.getId(),
                            e.getMessage()));
                }
            }
            return responseListBuilder.build();
        }
    }
    ```
    [View the full example on numaflow-java GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sink/simple/)

=== "Python"

    ```python
    class UserDefinedSink(Sinker):
        def handler(self, datums: Iterator[Datum]) -> Responses:
            responses = Responses()
            for msg in datums:
                _LOGGER.info("User Defined Sink %s", msg.value.decode("utf-8"))
                responses.append(Response.as_success(msg.id))
            return responses


    if __name__ == "__main__":
        grpc_server = SinkServer(UserDefinedSink())
        grpc_server.start()
    ```
    [View the full example on numaflow-python GitHub](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink/log)

=== "Rust"

    ```rust
    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        sink::Server::new(Logger).start().await
    }

    struct Logger;

    #[tonic::async_trait]
    impl sink::Sinker for Logger {
        async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
            let mut responses: Vec<Response> = Vec::new();

            while let Some(datum) = input.recv().await {
                let response = match std::str::from_utf8(&datum.value) {
                    Ok(v) => {
                        println!("{}", v);
                        Response::ok(datum.id)
                    }
                    Err(e) => Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e)),
                };
                responses.push(response);
            }

            responses
        }
    }
    ```
    [View the full example on numaflow-rs GitHub](https://github.com/numaproj/numaflow-rs/tree/main/examples/sink-log)

=== "JS/TS"

    ```typescript
    async function sinkFn(datums: AsyncIterableIterator<sink.Datum>): Promise<sink.Response[]> {
        let responses: Array<sink.Response> = []
        for await (let datum of datums) {
            console.log(`Received datum. id=${datum.id}, keys=${datum.keys}, payload=${datum.getValue().toString()}`)
            responses.push(sink.Response.ok(datum.id))
        }

        return responses
    }

    async function main() {
        const sinker = new sink.AsyncServer(sinkFn)
        await sinker.start()
    }

    main().catch(console.error)
    ```
    [View the full example on numaflow-js GitHub](https://github.com/numaproj/numaflow-js/tree/main/examples/sinker)

The SDK repositories contain more user-defined sink examples, covering the other response types such as fallback,
on-success and serve.

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

When the pipeline source is SQS (one or many queues), origin is system metadata
group `sqs`, key `queue_name`. See [Source Queue Origin](../sources/sqs.md#source-queue-origin).

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