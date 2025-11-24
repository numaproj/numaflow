# Watermarks

When processing an unbounded data stream, Numaflow has to materialize the results of the processing done on the data. 
The materialization of the output depends on the notion of time, e.g., the total number of logins served per minute. 
Without the idea of time inbuilt into the platform, we will not be able to determine the passage of time, which is 
necessary for grouping elements together to materialize the result. `Watermarks` is that notion of time that will help
us group unbounded data into discrete chunks. Numaflow supports watermarks out-of-the-box. 
Source vertices generate watermarks based on the event time, and propagate to downstream vertices.

Watermark is defined as _“a monotonically increasing timestamp of the oldest work/event not yet completed”_. In other words, 
if the watermark has advanced past some timestamp T, we are guaranteed by its monotonic property that no more processing 
will occur for on-time events at or before T.

## Configuration 
### Disable Watermark
Watermarks can be disabled with by setting `disabled: true`. 

### Idle Detection

Watermark is assigned at the source; this means that the watermark will only progress if there is data coming into the source.
There are many cases where the source might not be getting data, causing the source to idle (e.g., data is periodic, say once
an hour). When the source is idling the reduce vertices won't emit results because the watermark is not moving. To detect source
idling and propagate watermark, we can use the idle detection feature. The idle source watermark progressor will make sure that
the watermark cannot progress beyond `time.now() - maxDelay` (`maxDelay` is defined below). 
To enable this, we provide the following setting:

#### Threshold

Threshold is the duration after which a source is marked as Idle due to a lack of data flowing into the source.

#### StepInterval
StepInterval is the duration between the subsequent increment of the watermark as long the source remains Idle.
 The default value is 0s, which means that once we detect an idle source, we will increment the watermark by
`IncrementBy` for the time we detect that our source is empty (in other words, this will be a very frequent update).

Default Value: 0s
    
#### IncrementBy

IncrementBy is the duration to be added to the current watermark to progress the watermark when the source is idling.

#### InitSourceDelay

InitSourceDelay is the duration after which, if the source doesn't produce any data (from the inception of the pipeline), the watermark is initialized with the current wall clock time.
If InitSourceDelay is not set, the watermark will not be initialized under the abovementioned condition.

This is always useful to initialize the pipeline which contains sources that don't emit data for a long time.

#### Example

The below example will consider the source as idle after there is no data at the source for 5s. After 5s, every other 2s
an idle watermark will be emitted which increments the watermark by 3s.

``` yaml
  watermark:
    idleSource:
      threshold: 5s # The pipeline will be considered idle if the source has not emitted any data for given threshold value.
      incrementBy: 3s # If source is found to be idle then increment the watermark by given incrementBy value.
      stepInterval: 2s # If source is idling then publish the watermark only when step interval has passed.
      initSourceDelay: 10s # The initial watermark will be progressed after 10s to prevent one inactive source from holding up the pipeline.  
```

### maxDelay
Watermark assignments happen at the source. Sources could be out of order, so sometimes we want to extend the
window (default is `0s`) to wait before we start marking data as late-data.
You can give more time for the system to wait for late data with `maxDelay` so that the late data within the specified
time duration will be considered as data on-time. This means the watermark propagation will be delayed by `maxDelay`.

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

When processing data in [user-defined functions](../user-guide/user-defined-functions/user-defined-functions.md), you can get the current watermark through
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
