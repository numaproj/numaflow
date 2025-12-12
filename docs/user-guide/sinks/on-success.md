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

The following example uses the builtin kafka as a fallback sink.

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

SDK methods to generate an onSuccess response in a primary user-defined sink can be found here:
[Golang](https://github.com/numaproj/numaflow-go/blob/main/pkg/sinker/types.go#L61), [Java](https://github.com/numaproj/numaflow-java/blob/main/src/main/java/io/numaproj/numaflow/sinker/Response.java#L99), [Rust](https://github.com/numaproj/numaflow-rs/blob/main/numaflow/src/sink.rs#L550)