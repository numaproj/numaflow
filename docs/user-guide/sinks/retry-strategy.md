# Retry Strategy

### Overview

The `RetryStrategy` is used to configure the behavior for a sink after encountering failures during a write operation.
This structure allows the user to specify how Numaflow should respond to different fail-over scenarios for Sinks, ensuring that the writing can be resilient and handle
unexpected issues efficiently.

`RetryStrategy` ONLY gets applied to failed messages. To return a failed messages, use the methods provided by the SDKs.

- `ResponseFailure`for [Golang](https://github.com/numaproj/numaflow-go/blob/main/pkg/sinker/types.go)
- `responseFailure` for [Java](https://github.com/numaproj/numaflow-java/blob/main/src/main/java/io/numaproj/numaflow/sinker/Response.java#L40)
- `as_fallback` for [Python](https://github.com/numaproj/numaflow-python/blob/main/pynumaflow/sinker/_dtypes.py)

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
