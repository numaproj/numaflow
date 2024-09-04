# Retry Strategy

### Overview
The `RetryStrategy` is used to configure the behavior for a sink after encountering failures during a write operation. 
This structure allows the user to specify how Numaflow should respond to different fail-over scenarios for Sinks, ensuring that the writing can be resilient and handle 
unexpected issues efficiently.


### Struct Explanation


`retryStrategy` is optional, and can be added to the Sink spec configurations where retry logic is necessary.



```yaml
sink:
  retryStrategy:
      # Optional
      backoff:
        duration: 1s # Optional
        steps: 3 # Optional, number of retries (including the 1st try)
      # Optional
      onFailure: retry|fallback|drop 
```
Note: If no custom fields are defined for retryStrategy then the **default** values are used.

- `BackOff` - Defines the timing for retries, including the interval and the maximum attempts.
  - `duration`: the time interval to wait before retry attempts
    - Default: _1ms_
  - `steps`: the limit on the number of times to try the sink write operation including retries
    - Default: _Infinite_
- `OnFailure` - Specifies the action to be undertaken if number of retries are exhausted
  - retry: continue with the retry logic again
  - fallback: write the leftover messages to a [fallback](https://numaflow.numaproj.io/user-guide/sinks/fallback/) sink
  - drop: any messages left to be processed are dropped
    - Default: _retry_


### Constraints

1) If the `onFailure` is defined as fallback, then there should be a fallback sink specified in the spec.

2) The steps defined should always be `> 0`


## Example

```yaml
  sink:
    retryStrategy:
      backoff:
        interval: "500ms"
        steps: 10
      onFailure: "fallback"
    udsink:
      container:
        image: my-sink-image
    fallback:
      udsink:
        container:
          image: my-fallback-sink
```
### Explanation

- Normal Operation: Data is processed by the primary sink container specified by `UDSink`. 
The system retries up to 10 times for a batch write operation to succeed with an interval of 500 milliseconds between each retry.
- After Maximum Retries: If all retries fail, data is then routed to a fallback sink instead.
