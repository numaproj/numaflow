# Retry Strategy

<div style="padding: 15px; background-color: #e0f2fe; border: 1px solid #7dd3fc; border-radius: 6px; color: #0369a1; margin: 15px 0;">
    💡 <strong>Note:</strong> From v1.9, retry strategy can also be specified for <strong>map UDFs</strong> and <strong>source transformers</strong> (<a href="../../user-defined-functions/map/retry-strategy/">ref</a>). 
</div>

### Overview

The `RetryStrategy` is used to configure the behavior for a sink after encountering failures during a write operation.
This structure allows the user to specify how Numaflow should respond to different fail-over scenarios for Sinks, ensuring that the writing can be resilient and handle
unexpected issues efficiently.

`RetryStrategy` ONLY gets applied to failed messages. To return a failed message, use the methods provided by the SDKs.

=== "Golang"

    ```go
    ResponseFailure(id, errMsg)
    ```
    [Golang SDK Examples](https://github.com/numaproj/numaflow-go/blob/7e699ca5b8125eea1477025c200bedc21c17d0d3/examples/sinker/failure_sink/main.go#L19)

=== "Java"

    ```java
    responseFailure(id, errMsg)
    ```
    [Java SDK Examples](https://github.com/numaproj/numaflow-java/blob/198d66de7a24c6d7e841a0f28eb1d3ad88b5cbff/examples/src/main/java/io/numaproj/numaflow/examples/sink/simple/SimpleSink.java#L49-L52)

=== "Python"

    ```python
    Response.as_failure(id, err_msg)
    ```
    [Python SDK full signature](https://github.com/numaproj/numaflow-python/blob/5e6476085d8fb5e8b689dccca21c51d38713b9fc/packages/pynumaflow/pynumaflow/sinker/_dtypes.py#L119)

=== "Rust"

    ```rust
    Response::failure(id, err_msg)
    ```
    [Rust SDK Examples](https://github.com/numaproj/numaflow-rs/blob/36da7a9783b60b31d561a4fa43ced5c4ecd2e5ed/examples/sink-log/src/main.rs#L26)


### Retry Strategy Configuration

The `retryStrategy` section allows you to define custom retry behavior for sink operations. If no custom fields are defined, the Default values are applied.

#### Example Configuration

```yaml
sink:
  retryStrategy:
    # Optional
    backoff:
      interval: 1s # Optional, a string with timestamp suffix
      steps: 3 # Optional, unsigned int, cannot be 0
      factor: 1.5 # Optional, float type, >= 1
      cap: 20s # Optional, a string with timestamp suffix
      jitter: 0.1 # Optional, float type, >=0 and <1
    # Optional
    onFailure: 'fallback'
```

#### BackOff Parameters

The `BackOff` configuration defines the timing and limits for retries. Below are the available fields:

- **`interval`**: The time interval to wait before retry attempts.

    - Type: String with a timestamp suffix.
    - Default: `1ms`.

- **`steps`**: The maximum number of retry attempts, including the initial attempt.

    - Type: Unsigned integer, must be greater than 0.
    - Default: Infinite.

- **`factor`**: A multiplier applied to the interval after each retry attempt.

    - Type: Float, must be greater than or equal to 1.
    - Default: `1.0`.

- **`cap`**: The maximum value for the interval, limiting exponential backoff growth.

    - Type: String with a timestamp suffix.
    - Default: `indefinite` (no upper limit).

- **`jitter`**: Adds randomness to the interval to avoid retry collisions.
    - Type: Float, must be greater than or equal to 0 and less than 1.
    - Default: `0`.

#### OnFailure Actions

The `onFailure` field specifies the action to take when retries are exhausted. Available options are:

- **`retry`**: Restart the retry logic.
- **`fallback`**: Route the remaining messages to a [fallback sink](https://numaflow.numaproj.io/user-guide/sinks/fallback/).
- **`drop`**: Discard any unprocessed messages.

  > Default: `retry`

### Observing the Retry Count

For user-defined sinks, Numaflow exposes the current retry attempt count to your sink container
through the request's system metadata. This lets your sink logic react to how many times a message
has already been retried, for example to emit a metric or log retry attempts.

The value is available under the `sink` group of the system metadata, keyed by `retry_count`, and is
stored as a string. It is `0` on the first attempt and incremented by one for each subsequent retry.

The exact accessor depends on the SDK; the value lives in the `Datum`/request system metadata under:

- Group: `sink`
- Key: `retry_count`

### Sink Example with Retry Strategy

```yaml
sink:
  retryStrategy:
    backoff:
      interval: 500ms
      steps: 10
      factor: 2.2
      cap: 10s
    onFailure: 'fallback'
  udsink:
    container:
      image: my-sink-image
  fallback:
    udsink:
      container:
        image: my-fallback-sink
```

#### Explanation

- **Primary Sink Processing**: The main sink container (`UDSink`) processes the data. If a batch write operation fails, the system will retry up to 10 times.
- **Retry Behavior**: The first retry happens after 500 milliseconds. Each subsequent retry interval increases by multiplying the previous interval by 2.2, up to a maximum interval of 10 seconds.
- **Fallback Handling**: If all retries are exhausted and the operation still fails, the data is routed to a fallback sink.
