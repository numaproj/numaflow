# Accumulator

Accumulator is a special kind of window similar to a [Session Window](session.md) designed for reordering 
and joining multiple ordered streams. Unlike other windowing strategies like fixed, sliding, or session windows, the 
Accumulator window maintains state for each key and allows for manipulation of the Datum by reordering messages before 
emitting them. Reordering is a different type of problem outside of both `map`/`flatmap` (one to ~one) and 
`reduce` (many to ~one) and instead of `Message`, we have to emit back the original `Datum`.

![plot](../../../../assets/accumulator.png)

Another difference between the Accumulator and the Session windows is that in Accumulator, there is no concept of 
[window merge](./session.md#window-merge).

## Configuration

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          accumulator:
            timeout: duration
```

NOTE: A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix,
such as "300ms", "1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

### timeout

The `timeout` is the duration of inactivity (no data flowing in for a particular key) after which the accumulator state 
is removed. This helps prevent memory leaks by cleaning up state for keys that are no longer active.

## How It Works

The Accumulator window works by:

1. Maintaining an ordered list of elements for each key
2. When the watermark progresses, it pops all sorted elements and writes them to the output stream
3. New elements are inserted into the ordered list based on their event time
4. If no new data arrives for a key within the specified timeout period, the window is closed

Unlike both `map` or `reduce` operations, where `Datum` is consumed and `Message` is returned, for reordering with the 
Accumulator, the `Datum` is kept intact.

## Data Retention

To ensure there is no data loss during pod restarts, the Accumulator window replays data from persistent storage. The 
system stores data until `Outbound(Watermark) - 1`, which means it keeps the minimum necessary data to ensure correctness
while managing resource usage.

### Constraints

1. For data older than `Outbound(Watermark) - 1`, users need to bring in an external store and implement replay on restart
2. Data deletion is based on the `Outbound(Watermark)`

## Use Cases

1. **Stream Joining**: Combining multiple ordered streams into a single ordered output
2. **Event Reordering**: Handling out-of-order events and ensuring they're processed in the correct sequence
3. **Time-based Correlation**: Correlating events from different sources based on their timestamps
4. **Custom Sorting**: Implementing user-defined sorting logic for event streams

## Considerations

1. The Accumulator window is powerful but should be used carefully as it can cause pipeline stalling if not configured properly
2. For high-throughput scenarios, ensure adequate storage is provisioned
3. The timeout should be set based on the expected data arrival patterns and latency requirements
4. Consider the trade-off between data completeness (longer timeout) and processing latency (shorter timeout)

## Example

Here's an [example](https://github.com/numaproj/numaflow/blob/main/examples/13-accumulator-window.yaml) of using an 
Accumulator window to join and sort two HTTP sources:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-accumulator
spec:
  limits:
    readBatchSize: 1
  vertices:
    - name: http-one
      scale:
        min: 1
        max: 1
      source:
        http: {}
    - name: http-two
      scale:
        min: 1
        max: 1
      source:
        http: {}
    - name: accum
      udf:
        container:
          # stream sorter example
          image: quay.io/numaio/numaflow-go/stream-sorter:stable
        groupBy:
          window:
            accumulator:
              timeout: 10s
          keyed: true
          storage:
            persistentVolumeClaim:
              volumeSize: 1Gi
    - name: sink
      scale:
        min: 1
        max: 1
      sink:
        log: {}
  edges:
    - from: http-one
      to: accum
    - from: http-two
      to: accum
    - from: accum
      to: sink
```

In this example:

1. We have two HTTP sources (`http-one` and `http-two`) that produce ordered streams
2. The `accum` vertex uses an Accumulator window with a timeout of 10 seconds
3. The accumulator joins and sorts the events from both sources based on their event time
4. The sorted output is sent to a log sink

Note: Setting `readBatchSize: 1` helps maintain the ordering of events in the input streams.

Check the links below to see the UDF examples for different languages:

- [Golang](https://github.com/numaproj/numaflow-go/tree/main/pkg/accumulator)

