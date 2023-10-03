# Watermarks

When processing an unbounded data stream, Numaflow has to materialize the results of the processing done on the data. 
The materialization of the output depends on the notion of time, e.g., the total number of logins served per minute. 
Without the idea of time inbuilt into the platform, we will not be able to determine the passage of time, which is 
necessary for grouping elements together to materialize the result. `Watermarks` is that notion of time which will help
us group unbounded data into discrete chunks. Numaflow supports watermarks out-of-the-box. 
Source vertices generate watermarks based on the event time, and propagate to downstream vertices.

Watermark is defined as _“a monotonically increasing timestamp of the oldest work/event not yet completed”_. In other words, 
if the watermark has advanced past some timestamp T, we are guaranteed by its monotonic property that no more processing 
will occur for on-time events at or before T.

## Configuration 
### Disable Watermark
Watermarks can be disabled with by setting `disabled: true`. 

### maxDelay
Watermark assignments happen at source. Sources could be out of order, so sometimes we want to extend the
window (default is `0s`) to wait before we start marking data as late-data.
You can give more time for the system to wait for late data with `maxDelay` so that the late data within the specified
time duration will be considered as data on-time. This means, the watermark propagation will be delayed by `maxDelay`.

### Example 
```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
spec:
  watermark:
    disabled: false # Optional, defaults to false.
    maxDelay: 60s # Optional, defaults to "0s".
```

## Watermark API

When processing data in [User Defined Functions](../user-guide/user-defined-functions/map/map.md), you can get the current watermark through
an API. Watermark API is supported in all our client SDKs.

### Example Golang

```go
// Go
func mapFn(context context.Context, keys []string, d mapper.Datum) mapper.Messages {
    _ = d.EventTime() // Event time
    _ = d.Watermark() // Watermark
    ... ...
}
```
