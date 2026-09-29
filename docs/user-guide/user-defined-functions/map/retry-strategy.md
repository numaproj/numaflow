# Retry Strategy

!!! note "Available from v1.9"

    Available for **map UDFs** and **source transformers**.
    For the sink equivalent, and for the full backoff reference, see [Sink Retry Strategy](../../sinks/retry-strategy.md).

### Overview

A `retryStrategy` can be configured on a **map UDF** and on a **source transformer**.
Numaflow calls your user-defined container again, over gRPC, with the same input message.
Your handler is invoked again after the configured backoff interval.
`retryStrategy` applies only to messages your code explicitly marks as **failed**.

It uses the same `backoff` fields as the [sink retry strategy](../../sinks/retry-strategy.md).
This page covers how those fields behave for a map UDF and a source transformer.

The way you signal a failure is different from the sink. A sink returns a failure
response carrying the message id and an error string. A map UDF or source transformer returns a
message built with a reserved **fail tag**, mirroring the existing `Drop` message helpers. There is no
place to attach an error reason: the fail signal is tag-only in every SDK.

=== "Golang"

    ```go
    // map, batch map, map stream
    mapper.MessageToFail()
    batchmapper.MessageToFail()
    mapstreamer.MessageToFail()

    // source transformer — takes the event time, like MessageToDrop
    sourcetransformer.MessageToFail(eventTime)
    ```
    [Golang SDK signature](https://github.com/numaproj/numaflow-go/blob/90c9958d871a1bc99bc687f78ad08e6e0dcdc16b/pkg/mapper/message.go#L38)

=== "Java"

    ```java
    // map, batch map, map stream
    Message.toFail()

    // source transformer — takes the event time, like Message.toDrop
    Message.toFail(eventTime)
    ```
    [Java SDK signature](https://github.com/numaproj/numaflow-java/blob/a312216e5a56d7cc25614b03a13f9ee7960f3c2c/src/main/java/io/numaproj/numaflow/mapper/Message.java#L99)

=== "Python"

    ```python
    # map, batch map, map stream
    Message.to_fail()

    # source transformer — takes the event time, like Message.to_drop
    Message.to_fail(event_time)
    ```
    [Python SDK signature](https://github.com/numaproj/numaflow-python/blob/1079b70f3b73575ce9729b2a03957a824ae68cbc/packages/pynumaflow/pynumaflow/mapper/_dtypes.py#L65)

=== "Rust"

    ```rust
    // map, batch map, map stream
    Message::message_to_fail()

    // source transformer — takes the event time
    Message::message_to_fail(event_time)
    ```
    [Rust SDK Examples](https://github.com/numaproj/numaflow-rs/blob/main/examples/map-conditional-fail/src/main.rs)

The API is shaped identically across all four SDKs, with two consistent differences from the sink:

- **No error reason.** Unlike the sink's `ResponseFailure(id, errMsg)`, the fail helpers take no error
  string. Log the reason from inside your handler if you need it.
- **The source transformer variant takes an event time**, exactly as its `Drop` counterpart does, so the
  watermark can keep advancing even when a message is failed.


### Retry Strategy Configuration

The `retryStrategy` section is accepted at these paths:

| Component          | Pipeline                                           | MonoVertex                              |
|--------------------|----------------------------------------------------|-----------------------------------------|
| Map UDF            | `spec.vertices[].udf.retryStrategy`                | `spec.udf.retryStrategy`                |
| Source transformer | `spec.vertices[].source.transformer.retryStrategy` | `spec.source.transformer.retryStrategy` |

#### Example Configuration

```yaml
udf:
  container:
    image: my-map-image
  retryStrategy:
    # Optional
    backoff:
      interval: 1s # Optional, a string with timestamp suffix
      steps: 3 # Optional, unsigned int, cannot be 0
      factor: 1.5 # Optional, float type, >= 1
      cap: 20s # Optional, a string with timestamp suffix
      jitter: 0.1 # Optional, float type, >=0 and <1
    # Optional — only 'retry' and 'drop' are valid here. 'fallback' is NOT supported.
    onFailure: 'drop'
```

The same block applies to a source transformer:

```yaml
source:
  http: {}
  transformer:
    container:
      image: my-transformer-image
    retryStrategy:
      backoff:
        interval: 1s
        steps: 5
      onFailure: 'drop'
```

#### BackOff Parameters

The `backoff` fields match the sink: `interval`, `steps`, `factor`, `cap`, and `jitter` have the same
types and the same defaults. See
[BackOff Parameters](../../sinks/retry-strategy.md#backoff-parameters) for the full reference.

`steps` is the number of retries **after the first call**. `steps: 2` means the container is called
once and then up to two more times. That limit is applied only when `onFailure` is `drop`. When
`onFailure` is `retry`, including when `onFailure` is omitted, the configured `steps` value is ignored
and the runtime retries up to 65535 times.

#### OnFailure Actions

`fallback` is a sink-only action. For a map UDF or a source transformer the valid values are:

- **`retry`**: Default when `retryStrategy` is set. `backoff.steps` is ignored. Numaflow retries the
  fail-tagged message up to 65535 times after the first call, using `interval`, `factor`, `cap`, and
  `jitter` (`interval` defaults to `1ms`). When that cap is reached, the message is nacked and the
  vertex exits, so the pod restarts. On a pipeline source transformer,
  `source_forwarder_transformer_error_total` is incremented.
- **`drop`**: After `steps` retries, acknowledge the message and drop it. The drop counter that
  increments depends on the component:

    | Component          | Pipeline                                 | MonoVertex                            |
    |--------------------|------------------------------------------|---------------------------------------|
    | Map UDF            | `forwarder_ud_drop_total`                | `monovtx_udf_drop_total`              |
    | Source transformer | `source_forwarder_transformer_drop_total` | `monovtx_transformer_dropped_total`  |

- **`fallback`**: Rejected at validation with:

  ```
  given fallback OnFailure strategy is not currently supported
  ```

#### Defaults

- **`retryStrategy` omitted**: a fail-tagged message is retried forever, with no delay between attempts.
  The vertex keeps running.
- **`retryStrategy` set, `onFailure` omitted**: same as `onFailure: retry`. An empty `retryStrategy: {}`
  waits `1ms` between attempts, retries up to 65535 times, and then the vertex exits. A `steps` value
  in that block is ignored.

### Differences from the Sink Retry Strategy

|                                  | Sink                                                         | Map UDF / Source transformer                                                              |
|----------------------------------|--------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| Where the retry happens          | Re-writes to the external sink destination                   | Another gRPC call to the same sidecar, with the same message                              |
| How a message is failed          | A failure **response** carrying an id and an error message   | A reserved **fail tag** on the returned message (see the SDK tabs above)                  |
| Failure reason string            | Supported (`errMsg`)                                         | Not carried — the tag has no reason field                                                 |
| `onFailure: fallback`            | Supported                                                    | Rejected at validation                                                                    |

### Important Considerations

- **Only messages your code marks as failed are retried.** A message returned normally is forwarded
  downstream as usual.
- **A gRPC/transport error is not a `retryStrategy` retry.** `retryStrategy` applies only to the explicit
  fail signal described above. An error returned by the UDF call itself is treated as fatal and is handled
  by Numaflow's existing error handling, not by this backoff.
- **Map-stream can produce duplicates on retry.** In streaming mode, results are forwarded downstream as
  soon as they are produced. If your handler emits some messages and *then* marks a failure, the messages
  already emitted have left the vertex, and the retry re-invokes your handler for the whole input message.
  Make the handler idempotent, or mark the failure before emitting anything.
- **`retryStrategy` is not supported on reduce UDFs.** Specifying it on a vertex with `groupBy` is rejected
  at validation with `invalid udf spec, retryStrategy not supported for reduce udf`.

### Map UDF Example with Retry Strategy

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: map-retry-drop
spec:
  vertices:
    - name: in
      source:
        http: {}
    - name: udf
      udf:
        container:
          image: my-map-image
        retryStrategy:
          backoff:
            interval: 1s
            steps: 2
            factor: 2
            cap: 3s
            jitter: 0
          onFailure: 'drop'
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: udf
    - from: udf
      to: out
```

#### Explanation

- **Map Processing**: The map container processes each message. If the handler marks a message as failed,
  Numaflow re-invokes the same container with the same message.
- **Retry Behavior**: The first retry happens after 1 second, the interval doubles on each subsequent
  retry, and it is capped at 3 seconds. With `steps: 2`, the container is invoked once and then retried up
  to 2 more times.
- **Drop Handling**: If the message is still marked as failed after the retries are exhausted, it is
  dropped and acknowledged, and the pipeline continues. Had `onFailure` been left at its default of
  `retry`, the vertex would instead have failed and restarted.
