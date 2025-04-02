# MonoVertex Tuning

Similar to [pipeline tuning](./pipeline-tuning.md), certain parameters can be fine-tuned for the data processing using [MonoVertex](../../core-concepts/monovertex.md).

Each [MonoVertex](../../core-concepts/monovertex.md) keeps running the cycle of reading data from a data source,
processing the data, and writing to a sink. There are some parameters can be adjusted for this cycle.

- `readBatchSize` - How many messages to read for each cycle, defaults to `500`. It works together with `readTimeout` during a read operation, concluding when either limit is reached first.
- `readTimeout` - Read timeout from the source, defaults to `1s`.

These parameters can be customized under `spec.limits` as below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  limits:
    readBatchSize: 100
    readTimeout: 500ms
```
