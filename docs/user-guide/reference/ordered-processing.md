# Ordered Processing

> **Available from v1.8**

By default, Numaflow improves throughput by distributing work across any available processing unit, which means
messages may be processed out of the order in which they arrived. However, some workflows require messages to be
processed in their arrival order - for example, a create-update-delete sequence where you cannot update a record
before it has been created.

The **ordered processing** feature provides **input-order preservation** with partitioned FIFO semantics: within a
partition, the N-th message is processed only after the (N-1)-th message completes. The ordering is based on the
position of each message in the Inter-Step Buffer (ISB), not on event timestamps.

## What Order Preservation Means

It is important to distinguish **order preservation** from **timestamp ordering**:

| Concept                               | Meaning                                                                                                                                                          |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Order preservation (this feature)** | Messages are processed in the same order they were written to the ISB - i.e., FIFO. Message N is processed only after message N-1 completes, within a partition. |
| **Timestamp ordering**                | Messages are sorted by event time so that earlier events are processed before later ones. This feature does **not** provide timestamp ordering.                  |

Sources may emit events whose event-time timestamps are not monotonically increasing (e.g., late-arriving data,
multiple producers). Order preservation guarantees that messages are processed in their **arrival order** (the order
the source emitted them into the pipeline), regardless of their event timestamps.

In short: if your source emits messages A, B, C in that sequence, order preservation guarantees they are processed in
the sequence A, B, C - even if B has an earlier event timestamp than A.

## How It Works

Ordered processing works differently depending on the vertex type:

| Vertex Type | Behavior                                                                                                                                                                                                       |
|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Source**  | Always preserves input order - messages are emitted into the pipeline in the order received from the external source. No configuration needed.                                                                 |
| **Map**     | Requires `partitions` to be configured. Replicas are fixed to the `partitions` count. Messages are routed to partitions by key hash, so all messages with the same key are processed by the same pod in order. |
| **Reduce**  | Already partitioned and order-preserving - no additional configuration needed.                                                                                                                                 |
| **Sink**    | Same as Map - requires `partitions`. Replicas are fixed to the partition count.                                                                                                                                |

When ordered processing is enabled for a Map or Sink vertex:

- The number of replicas is automatically fixed to the partition count (one pod per partition).
- Autoscaling is disabled for that vertex - you must not set `scale.min` or `scale.max`.
- Messages are routed to partitions by hashing their keys, ensuring all messages with the same key go to the same pod,
  preserving their arrival order.


## Pipeline Specification

Enable order-preserving processing by setting `ordered.enabled: true` at the pipeline level. For Map and Sink
vertices, set `partitions` to the number of ordered lanes you need - the controller will fix replicas to that count
automatically.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  limits:
    readBatchSize: 1        # recommended for strict ordering
  ordered:
    enabled: true           # enable order-preserving processing pipeline-wide
  vertices:
    - name: my-source
      source: {}            # always preserves input order; no extra config needed
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
throughput. However, replicas are determined by normal autoscaling - they are not fixed to N - so multiple replicas may
read from the same partition, or one replica may handle multiple partitions. There is no ordering guarantee in this
case.

## Caveats and Limitations

- **Autoscaling is not supported** for Map and Sink vertices with ordered processing enabled. The replica count is fixed
  to the partition count. Setting `scale.min` or `scale.max` on such vertices will cause a validation error.
- **Reduce vertices** are already partitioned and order-preserving by design; the `ordered` setting is ignored for them.
- **Source vertices** always preserve input order regardless of the `ordered` setting.
- **Key-based routing**: ordering is guaranteed per key. Messages with different keys may still be interleaved across
  partitions. Ensure your UDF or SDK sets meaningful message keys to leverage per-key ordering.
- **`readBatchSize: 1`** is required for strict ordering. With a larger batch size, multiple messages may
  be in-flight simultaneously within a single pod.
- **Throughput trade-off**: ordered processing limits parallelism within a partition. Consider the number of partitions
  carefully to balance ordering guarantees with throughput requirements.
- **Join vertices (multiple input edges)**: When a vertex receives messages from multiple upstream vertices
  (a [join](join-vertex.md)), order preservation holds independently for each input edge, _but not across edges_.
  Messages from different upstream vertices are interleaved in whatever order they arrive at the ISB. If you need a
  global ordering across multiple inputs, you must merge them at a single source or use application-level sequencing.
- **Cycles**: When a [cycle](join-vertex.md#cycles) re-injects a message back into an earlier vertex, that message is
  appended to the ISB after messages that have already been written. From the user's perspective, the re-injected
  message appears later in the processing order than it did in the original stream. This means order preservation
  relative to the original input sequence is disrupted for cycled messages.

## Behavior at Join Vertices

A [join vertex](join-vertex.md) receives messages from two or more upstream vertices. Each input edge has its own set
of ISB partitions, and order preservation applies **per edge**:

- Messages arriving from upstream vertex A are processed in FIFO order relative to each other.
- Messages arriving from upstream vertex B are processed in FIFO order relative to each other.
- However, messages from A and B are **interleaved** in whatever order they arrive at the join vertex. There is no
  cross-edge ordering guarantee.

The [example below](#example) demonstrates this: sources `in-1` and `in-2` both feed into the `cat` vertex. Within each source's
stream, order is preserved, but `cat` will interleave messages from `in-1` and `in-2` based on arrival time.

## Behavior in Cycles

A [cycle](join-vertex.md#cycles) sends a message from a vertex back to itself or to a previous vertex. When a message
is re-injected via a cycle:

1. The cycled message is appended to the ISB **after** messages that have already been written by the original upstream
   source.
2. From the perspective of the receiving vertex, the cycled message appears as a new, later message - not in its
   original position.
3. Therefore, order preservation relative to the original input stream is **not maintained** for cycled messages.

If your use case requires strict ordering and also uses cycles (e.g., retry logic), be aware that retried messages will
be processed after messages that arrived in the interim. Design your application logic accordingly.

## Example

To enable order-preserving processing, set `ordered.enabled: true` in the pipeline spec. For Map and Sink vertices,
also set `partitions` to the desired number of partitions (which will also be the fixed replica count).

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

- `ordered.enabled: true` enables order-preserving processing pipeline-wide.
- `limits.readBatchSize: 1` is required so that each pod processes one message at a time, which is essential for
strict in-order guarantees.
- The `cat` (Map) and `out` (Sink) vertices each have `partitions: 3`, so they will run with exactly 3 replicas.
- Source vertices (`in-1`, `in-2`) always preserve input order and require no extra configuration.
- Because `in-1` and `in-2` both feed into `cat` (a join), messages from the two sources are interleaved at `cat`.
  Order preservation holds within each source's stream, not across the two sources.


### Complete Example

For a more complete example that combines ordered processing with event-time sorting, see
[ordered-stream-processing](https://github.com/numaproj/numaflow-rs/tree/main/examples/ordered-stream-processing)
in the Rust SDK. That example uses an accumulator to sort out-of-order events by event time, then feeds them through
partitioned map and sink vertices with ordered processing enabled, demonstrating both temporal and spatial ordering
working together.
