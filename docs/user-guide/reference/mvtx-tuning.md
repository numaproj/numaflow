# MonoVertex Tuning

Similar to [pipeline tuning](./pipeline-tuning.md), certain parameters can be fine-tuned for the data processing using [MonoVertex](../../core-concepts/monovertex.md).

Each [MonoVertex](../../core-concepts/monovertex.md) keeps running the cycle of reading data from a data source,
processing the data, and writing to a sink. There are some parameters can be adjusted for this cycle.

- `readBatchSize` - How many messages to read for each cycle, defaults to `500`. It works together with `readTimeout` during a read operation, concluding when either limit is reached first. **`readBatchSize` controls only how many messages are fetched in a single read call; it is not a cap on how many messages may be in-flight (use `concurrency` for that).**
- `readTimeout` - Read timeout from the source, defaults to `1s`.
- `concurrency` - Maximum number of messages that can be actively in-flight (read but not yet acknowledged) at any given time. Defaults to the value of `readBatchSize`. See [Concurrency and read-ahead](#concurrency-and-read-ahead) below.

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
    concurrency: 500
```

## Concurrency and read-ahead

Throughput on a MonoVertex is governed by two independent knobs:

- **`readBatchSize`** — the size of an individual read from the source. The data plane keeps using this batch size for every read; it does **not** cap how many messages may be in-flight at the same time.
- **`concurrency`** — the maximum number of messages that may be in-flight (read but not yet acknowledged). With `concurrency` slots free, the data plane is willing to process that many messages in parallel through the UDF / sink chain.

Because a MonoVertex always reads from a source, **read-ahead is disabled by default**. This keeps source ordering and the cost of re-reads low: the data plane fully drains the current batch (acks every message) before issuing the next read. The hard upper bound on in-flight messages is therefore **`min(concurrency, readBatchSize)`**.

If you want to overlap reads with processing — at the cost of more in-flight messages and looser ordering at the source — set `READ_AHEAD=true` on the container template:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  limits:
    readBatchSize: 100
    concurrency: 500
  containerTemplate:
    env:
      - name: READ_AHEAD
        value: "true"
```

With read-ahead enabled the upper bound on in-flight messages becomes **`min(concurrency, readBatchSize)`**.

### Strictly sequential processing

To force a MonoVertex to process one message at a time in source order, set `concurrency: 1`. Read-ahead is already off by default, so no other change is required.
