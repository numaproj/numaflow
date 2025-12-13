# OnSuccess Sink

OnSuccess sink is used to write the messages to a sink only when the processing of the original message has completed 
successfully in the primary sink.

## Use Case

OnSuccess sink is useful in following scenarios:

- When you want to write a confirmation message to a sink when the original message has been written to the primary
sink successfully.
- When you want to write the original message to a secondary sink after it has been written to the primary sink successfully.

## Caveats

- OnSuccess sink is more meaningful when the primary sink is a user-defined sink because builtins are not written to support on-success responses.
- For a given OnSuccess sink, we cannot configure another fallback or onSuccess sink. This means, if the OnSuccess sink 
  is a user-defined sink, then we cannot wrap the message sent from this onSuccess UD sink as a onSuccess/fallback response.  

## How to use

To configure onSuccess sink, changes need to be made to the pipeline specification and the user-defined sink implementation.

### Step 1 - update the specification

Add a `onSuccess` field to the sink configuration in the pipeline specification file.

The following example uses the builtin kafka as an onSuccess sink.

```yaml
    - name: out
      sink:
        udsink:
          container:
            image: my-sink:latest
        onSuccess:
          kafka:
            brokers:
              - my-broker1:19700
              - my-broker2:19700
            topic: my-topic
```

A onSuccess sink can also be a user-defined sink.

```yaml
    - name: out
      sink:
        udsink:
          container:
            image: my-sink:latest
        onSuccess:
          udsink:
            container:
              image: my-sink:latest
```

### Step 2 - update the user-defined sink implementation

Code changes have to be made in the primary sink to generate an **onSuccess** response.

SDK methods to generate an onSuccess response in a primary user-defined sink:

=== "Golang"

    ```go
    package main
    
    import (
        sinksdk "github.com/numaproj/numaflow-go/pkg/sinker"
    )
    
    // onSuccessLogSink is a sinker implementation that logs the input to stdout
    type onSuccessLogSink struct {
    }
    
    func (l *onSuccessLogSink) Sink(ctx context.Context, datumStreamCh <-chan sinksdk.Datum) sinksdk.Responses {
        result := sinksdk.ResponsesBuilder()
        for d := range datumStreamCh {
            result = result.Append(sinksdk.ResponseOnSuccess(d.id, sinksdk.NewMessage([]byte("primary sink write succeeded"))))
        }
        return result
    }
    ```
    - [Golang Response Struct](https://github.com/numaproj/numaflow-go/blob/a75410dfc101ae70edeec4becb30c10f02abca53/pkg/sinker/types.go#L61-L63)

=== "Java"

    ```java
    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        while (datumIterator.next() != null) {
            try {
                responseListBuilder.addResponse(Response.responseOnSuccess(datum.getId(),
                        Message.builder()
                                .value(String.format("Successfully wrote message with ID: %s",
                                        datum.getId()).getBytes())
                                .build()));
            } catch (Exception e) {
                log.warn("Error while writing to any sink: ", e);
                responseListBuilder.addResponse(Response.responseFailure(
                        datum.getId(),
                        e.getMessage()));
            }
        }
        return responseListBuilder.build();
    }
    ```
    - [Java Response Class](https://github.com/numaproj/numaflow-java/blob/3180f88f71c6b6bd2f1fbff1a690d359710265cf/src/main/java/io/numaproj/numaflow/sinker/Response.java#L99)

=== "Rust"

    ```rust
    #[tonic::async_trait]
    impl sink::Sinker for SinkHandler {
        async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
            let mut responses: Vec<Response> = Vec::new();
            while let Some(datum) = input.recv().await {
                responses.push(Response::on_success(
                    datum.id,
                    // To write the original message to the on success sink
                    None,  
                ));
            }
        }
    }
    ```
    - [Rust Response Struct](https://github.com/numaproj/numaflow-rs/blob/0a496b1df9771146cb01930e27bb27f02c69dbee/numaflow/src/sink.rs#L550)