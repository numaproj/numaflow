# Watermarks

`Watermarks` is a real-time streaming technology used to handle late arriving data, it tells how long the system should wait for the late data. Numaflow supports watermarks out-of-the-box. Source vertices generate watermarks based on the event time, and propagate to downstream vertices.


When processing data in [User Defined Functions](./user-defined-functions.md), you can get the current watermark through an API.

```go
// Go
func handle(ctx context.Context, key string, data funcsdk.Datum) funcsdk.Messages {
	_ = data.EventTime() // Event time
	_ = data.Watermark() // Watermark
	... ...
}
```

Watermarks can be disabled with by setting `disabled: true`. You also can give more time for the system to wait for late data with `maxDelay`.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
spec:
  watermark:
    disabled: false # Optional, defaults to false.
    maxDelay: 60s # Optional, defaults to "0s".
```
