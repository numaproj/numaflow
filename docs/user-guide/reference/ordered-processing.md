# Ordered Processing

> **Available from v1.8**

By default, Numaflow improves throughput by distributing work across any available processing unit, which results in 
out-of-order processing. However, some workflows require messages to be processed in a deterministic order — for 
example, a create-update-delete sequence where you cannot update a record before it has been created.

Ordered processing in Numaflow provides partitioned FIFO semantics: within a partition, the N-th message is processed 
only after the (N-1)-th message completes.

## How It Works

Ordered processing works differently depending on the vertex type:

| Vertex Type | Behavior                                                                                                                                                                                             |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Source**  | Always ordered by nature — no configuration needed.                                                                                                                                                  |
| **Map**     | Requires `partitions` to be set. Replicas are fixed to the partition count. Messages are routed to partitions by key hash, so all messages with the same key are processed by the same pod in order. |
| **Reduce**  | Already partitioned and ordered — no additional configuration needed.                                                                                                                                |
| **Sink**    | Same as Map — requires `partitions`. Replicas are fixed to the partition count.                                                                                                                      |

When ordered processing is enabled for a Map or Sink vertex:
- The number of replicas is automatically fixed to the partition count (one pod per partition).
- Autoscaling is disabled for that vertex — you must not set `scale.min` or `scale.max`.
- Messages are routed to partitions by hashing their keys, ensuring all messages with the same key go to the same pod in
  FIFO order.


## Pipeline Specification

Enable ordered processing by setting `ordered.enabled: true` at the pipeline level. For Map and Sink vertices, set
`partitions` to the number of ordered lanes you need — the controller will fix replicas to that count automatically.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  limits:
    readBatchSize: 1        # recommended for strict ordering
  ordered:
    enabled: true           # enable ordered processing pipeline-wide
  vertices:
    - name: my-source
      source: {}            # always ordered; no extra config needed
    - name: my-map
      partitions: N         # N replicas will be created, one per partition
      udf:
        container:
          image: my-map:stable
    - name: my-sink
      partitions: N         # N replicas will be created, one per partition
      sink: {}
  edges:
    - from: my-source
      to: my-map
    - from: my-map
      to: my-sink
```

## Per-Vertex Override

Ordered processing can also be enabled or disabled at the individual vertex level, which overrides the pipeline-level 
setting. This is useful when you want most vertices to run unordered for throughput, but need ordering for specific steps.

```yaml
spec:
  ordered:
    enabled: true   # pipeline-level default
  vertices:
    - name: high-throughput-map
      ordered:
        enabled: false  # override: disable ordered processing for this vertex
      udf:
        container:
          image: my-fast-map:stable
    - name: ordered-sink
      partitions: 3
      sink:
        log: {}
```

### `partitions` with Ordered Disabled

Setting `partitions` on a vertex that has ordered processing disabled (or overridden to `false`) is valid. The vertex
will still have N ISB buffer partitions created, giving you a [multi-partitioned edge](multi-partition.md) for higher
throughput. However, replicas are determined by normal autoscaling — they are not fixed to N — so multiple replicas may
read from the same partition, or one replica may handle multiple partitions. There is no ordering guarantee in this
case.

## Caveats and Limitations

- **Autoscaling is not supported** for Map and Sink vertices with ordered processing enabled. The replica count is fixed
  to the partition count. Setting `scale.min` or `scale.max` on such vertices will cause a validation error.
- **Reduce vertices** are already partitioned and ordered by design; the `ordered` setting is ignored for them.
- **Source vertices** are always ordered regardless of the `ordered` setting.
- **Key-based routing**: ordering is guaranteed per key. Messages with different keys may still be interleaved across 
  partitions. Ensure your UDF or SDK sets meaningful message keys to leverage per-key ordering.
- **`readBatchSize: 1`** is strongly recommended for strict ordering. With a larger batch size, multiple messages may 
  be in-flight simultaneously within a single pod.
- **Throughput trade-off**: ordered processing limits parallelism within a partition. Consider the number of partitions 
  carefully to balance ordering guarantees with throughput requirements.

## Example

To enable ordered processing, set `ordered.enabled: true` in the pipeline spec. For Map and Sink vertices, also set
`partitions` to the desired number of partitions (which will also be the fixed replica count).

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: ordered-pipeline
spec:
  limits:
    readBatchSize: 1
  ordered:
    enabled: true
  vertices:
    - name: in-1
      source:
        http: {}
    - name: in-2
      source:
        http: {}
    - name: cat
      partitions: 3
      udf:
        container:
          image: quay.io/numaio/numaflow-rs/map-cat:stable
          imagePullPolicy: IfNotPresent
    - name: out
      partitions: 3
      sink:
        log: {}
  edges:
    - from: in-1
      to: cat
    - from: in-2
      to: cat
    - from: cat
      to: out
```

In the example above:

- `ordered.enabled: true` enables ordered processing pipeline-wide.
- `limits.readBatchSize: 1` is recommended so that each pod processes one message at a time, which is required for
  strict in-order guarantees.
- The `cat` (Map) and `out` (Sink) vertices each have `partitions: 3`, so they will run with exactly 3 replicas.
- Source vertices (`in-1`, `in-2`) are always ordered and require no extra configuration.
