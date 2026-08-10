# Fallback Sink

A `Fallback` Sink functions as a `Dead Letter Queue (DLQ)` Sink.
It can be configured to serve as a backup sink when the primary sink fails to process messages. 

## The Use Case

Fallback Sink is useful to prevent back pressures caused by failed messages in the primary sink.

It lets you define a DLQ associated with a given user-defined sink (hereafter referred to as the *primary sink*). 
The final fate of a message, eg: success, retry-until-failure, etc. need not be decided within the primary sink; 
the message can instead be moved to the fallback sink to be reprocessed later (for example, by a separate pipeline/MonoVertex).

## Caveats

- A fallback sink can only be configured when the primary sink is a user-defined sink (this is because builtin sinks will not know what tags to use and whether those will be honored).
- A message routed to the fallback sink continues the lifecycle it began in the primary sink.
  - e.g., a message that arrived at the primary sink and was routed to the fallback sink
    is not considered processed until its fate is decided in the fallback sink (success/fail).
  - This matters for a non-streaming MonoVertex, where the next batch is not read until
    the current batch finishes processing.

## How to use

To configure a fallback sink,
changes need to be made on both the pipeline specification and the user-defined sink implementation.

### Step 1 - update the specification

Add a `fallback` field to the sink configuration in the pipeline specification file.

The following example uses the builtin kafka as a fallback sink.

```yaml
    - name: out
      sink:
        udsink:
          container:
            image: my-sink:latest
        fallback:
          kafka:
            brokers:
              - my-broker1:19700
              - my-broker2:19700
            topic: my-topic
```

A fallback sink can also be a user-defined sink.

```yaml
    - name: out
      sink:
        udsink:
          container:
            image: my-sink:latest
        fallback:
          udsink:
            container:
              image: my-sink:latest
```
### Step 2 - update the user-defined sink implementation

Code changes have to be made in the primary sink to generate either a **failed** response or a **fallback** response,
based on the use case.

* a **failed** response gets processed following the [retry strategy](https://numaflow.numaproj.io/user-guide/sinks/retry-strategy/), and if the retry strategy is set to `fallback`, the message will be directed to the fallback sink after the retries are exhausted.
* a **fallback** response doesn't respect the sink retry strategy. It gets immediately directed to the fallback sink without getting retried.

SDK methods to generate either a fallback or a failed response in a primary user-defined sink can be found here:
[Golang](https://github.com/numaproj/numaflow-go/blob/main/pkg/sinker/types.go), [Java](https://github.com/numaproj/numaflow-java/blob/main/src/main/java/io/numaproj/numaflow/sinker/Response.java), [Python](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/pynumaflow/sinker/_dtypes.py)
