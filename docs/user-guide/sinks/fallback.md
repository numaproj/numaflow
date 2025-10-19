# Fallback Sink

A `Fallback` Sink functions as a `Dead Letter Queue (DLQ)` Sink.
It can be configured to serve as a backup sink when the primary sink fails processing messages. 

## The Use Case

Fallback Sink is useful to prevent back pressures caused by failed messages in the primary sink.

In a pipeline without fallback sinks, if a sink fails to process certain messages, 
the failed messages, by default, can get retried indefinitely, 
causing back pressures propagated all the way back to the source vertex.
Eventually, the pipeline will be blocked, and no new messages will be processed.
A fallback sink can be set up to prevent this from happening, by storing the failed messages in a separate sink.

## Caveats

A fallback sink can only be configured when the primary sink is a user-defined sink.

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
