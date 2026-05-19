# Pipeline Tuning

In a data processing [pipeline](../../core-concepts/pipeline.md), certain parameters can be fine-tuned according to the specific use case of the data processing.

## Vertex Tuning

Each [vertex](../../core-concepts/vertex.md) keeps running the cycle of reading data from an Inter-Step Buffer (or data source),
processing the data, and writing to the next Inter-Step Buffers (or sinks). There are some parameters can be adjusted for this data
processing cycle.

- `readBatchSize` - The number of messages to read in each cycle, with a default value of `500`. It works together with `readTimeout` during a read operation, concluding when either limit is reached first. **`readBatchSize` controls only how many messages are fetched in a single read call from the source/buffer; it is not a cap on how many messages may be in-flight (use `concurrency` for that).**
- `readTimeout` - Read timeout from the source or Inter-Step Buffer, defaults to `1s`. It works in conjunction with `readBatchSize`.
- `bufferMaxLength` - How many unprocessed messages can be existing in the Inter-Step Buffer, defaults to `30000`.
- `bufferUsageLimit` - The percentage of the buffer usage limit, a valid number should be less than 100. Default value is `80`, which means `80%`.
- `concurrency` - Maximum number of messages that can be actively in-flight (read but not yet acknowledged) at any given time. Defaults to the value of `readBatchSize`. See [Concurrency and read-ahead](#concurrency-and-read-ahead) below.

These parameters can be customized under `spec.limits` as below, once defined, they apply to all the vertices and Inter-Step Buffers of the pipeline.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  limits:
    readBatchSize: 100
    readTimeout: 200ms
    bufferMaxLength: 30000
    bufferUsageLimit: 85
    concurrency: 500
```

They also can be defined in a vertex level, which will override the pipeline level settings.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  limits: # Default limits for all the vertices and edges (buffers) of this pipeline
    readBatchSize: 100
    readTimeout: 200ms
    bufferMaxLength: 30000
    bufferUsageLimit: 85
  vertices:
    - name: in
      source:
        generator:
          rpu: 5
          duration: 1s
    - name: cat
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable # A UDF which simply cats the message
          imagePullPolicy: Always
      limits:
        readBatchSize: 200 # It overrides the default limit "100"
        bufferMaxLength: 20000 # It overrides the default limit "30000" for the buffers owned by this vertex
        bufferUsageLimit: 70 # It overrides the default limit "85" for the buffers owned by this vertex
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: cat
    - from: cat
      to: out
```

## Concurrency and read-ahead

Throughput on a vertex is governed by two independent knobs:

- **`readBatchSize`** — the size of an individual read from the source or Inter-Step Buffer. The data plane keeps using this batch size for every read; it does **not** cap how many messages may be in-flight at the same time.
- **`concurrency`** — the maximum number of messages that may be in-flight (read but not yet acknowledged). With `concurrency` slots free, the data plane is willing to start processing that many messages in parallel.

The data plane runs in one of two modes:

| Mode | Default for | Behavior |
|---|---|---|
| Read-ahead **enabled** | Map / Sink / Reduce vertices | The data plane keeps reading new batches until the in-flight count hits `concurrency`. Once full, it may pre-fetch one more batch and hold it ready, so a completed message is replaced immediately. The hard upper bound on in-flight is therefore **`concurrency + readBatchSize`**. |
| Read-ahead **disabled** | Source vertices (and MonoVertex, which is always source-driven) | The data plane drains the current batch fully before issuing the next read. This keeps source ordering / re-read cost low. The upper bound on in-flight is **`min(concurrency, readBatchSize)`**. |

Read-ahead is controlled by the `READ_AHEAD` environment variable. The controller injects a sensible default per vertex type, but you can override it on a per-vertex basis via the container template:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: cat
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable
      limits:
        readBatchSize: 100
        concurrency: 500   # up to 500 messages may be processing in parallel
      containerTemplate:
        env:
          - name: READ_AHEAD
            value: "false" # turn read-ahead off for this Map vertex
```

### Strictly sequential processing

To force a vertex to process one message at a time in the order it was read, set:

- `concurrency: 1`, and
- `READ_AHEAD=false` (already the default for source vertices and MonoVertex).

For pipeline-wide ordered processing, see [Ordered Processing](ordered-processing.md).

## Edge Tuning

There is an edge level setting to drop the messages if the `buffer.isFull == true`. Even if the UDF or UDSink drops
a message due to some internal error in the user-defined code, the processing latency will spike up causing a natural
back pressure. A kill switch to drop messages can help alleviate/avoid any repercussions on the rest of the DAG.

This setting is an edge-level setting and can be enabled by `onFull` and the default is `retryUntilSuccess` (other option
is `discardLatest`).

This is a **data loss scenario** but can be useful in cases where we are doing user-introduced experimentations,
like A/B testing, on the pipeline. It is totally okay for the experimentation side of the DAG to have data loss while
the production is unaffected.

### discardLatest

Setting `onFull` to `discardLatest` will drop the incoming message on the floor if the edge is full.

```yaml
edges:
  - from: a
    to: b
    onFull: discardLatest
```

### retryUntilSuccess

The default setting for `onFull` in `retryUntilSuccess` which will make sure the incoming message is retried until successful.

```yaml
edges:
  - from: a
    to: b
    onFull: retryUntilSuccess
```
