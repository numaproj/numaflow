# MonoVertex Streaming Mode

> **Opt-in, off by default.**
> Streaming mode makes the **source side** fully per-message. 
> The sink still micro-batches, in a follow-up PR will complete the sink side.

## Overview

By default, a MonoVertex advances **one batch at a time**: it reads up to
`readBatchSize` messages, waits for the entire batch to finish processing and be
acknowledged by the sink, then reads the next batch.

Setting `spec.streaming: true` switches the source side to **per-message,
continuous reading**:

- Messages are read and acknowledged individually rather than as a whole batch.
- Reading continues without waiting for a batch boundary — new messages are
  admitted as soon as an in-flight slot becomes free.
- The number of in-flight (read-but-unacked) messages is bounded by
  `spec.limits.concurrency`.

This eliminates the source-side micro-batching stall and allows the pipeline to
stay full up to the `concurrency` ceiling rather than draining a complete batch
before proceeding.

### What changes in PR1

| Aspect        | Default (batch) mode              | Streaming mode            |
|---------------|-----------------------------------|---------------------------|
| Source read   | Whole batch, then wait            | Continuous, per-message   |
| Source ack    | One batched ack per batch         | Per-message, out-of-order |
| Sink write    | Micro-batched                     | Micro-batched (currently) |
| In-flight cap | `min(concurrency, readBatchSize)` | `concurrency` (see below) |

## Enabling streaming mode

Add `streaming: true` under the MonoVertex spec:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  streaming: true
  limits:
    concurrency: 200
  source:
    # ...
  sink:
    # ...
```

## Guarantees

- **At-least-once delivery.** A message is acknowledged at the source only after
  its terminal disposition (sink write) succeeds. On failure or crash, unacked
  messages are redelivered.
- **No ordering guarantee.** Messages are processed concurrently and may be
  acknowledged out of order. This matches the existing behavior of concurrent
  unary map inside a MonoVertex.
- **Backpressure cap = `spec.limits.concurrency`.** When all `concurrency` slots
  are occupied the data plane stops reading until a slot frees up.

## In-flight ceiling semantics

The data plane targets a maximum of `concurrency` messages in flight at any
instant. Because reads happen in batches at the source level (up to
`readBatchSize` messages per read call), there is a transient over-read of at
most one `readBatchSize` before the cap takes effect. The practical peak
in-flight count is therefore approximately:

```
peak in-flight ≈ concurrency + readBatchSize
```

Set `readBatchSize` low (e.g., `1`) if you need a hard ceiling, or rely on
`concurrency` alone for a soft cap with a one-batch overshoot.

See [MonoVertex Tuning](./mvtx-tuning.md) for full documentation of `concurrency`
and `readBatchSize`.

## Source compatibility

Streaming mode uses **per-message, out-of-order acking** at the source. Sources
whose ack API commits offsets cumulatively (highest-seen + 1) are **not safe**
under this model: a crash after acking a later message but before acking an
earlier one can permanently skip the earlier message.

| Source              | Ack model                                | Compatible with `streaming: true`?                                  |
|---------------------|------------------------------------------|---------------------------------------------------------------------|
| Built-in Kafka      | Cumulative commit (`highest_offset + 1`) | **No — rejected at validation time** (see warning below)            |
| Built-in Pulsar     | Individual `ack_with_id`                 | Yes — see [Pulsar constraint](#pulsar-concurrency-constraint) below |
| Built-in JetStream  | Per-message                              | Yes                                                                 |
| Built-in SQS        | Per-message                              | Yes                                                                 |
| Built-in Generator  | Per-message                              | Yes                                                                 |
| User-defined source | Depends on implementation                | **User's responsibility** (see note below)                          |

> **Note — User-defined sources:** A UD-source that wraps a cumulative-commit
> system (such as a Kafka consumer library) has the same data-loss risk as the
> built-in Kafka source. Do **not** enable `streaming: true` if your UD-source
> commits offsets cumulatively. The validation gate only covers the built-in
> Kafka source; UD-source compatibility cannot be detected automatically.

---

> **Warning — Kafka data loss**
>
> **Do not use `streaming: true` with the built-in Kafka source.**
>
> The built-in Kafka source commits offsets cumulatively: it always commits
> `highest_acked_offset + 1`. Under per-message, out-of-order acking, a later
> offset can be acked (and committed) before an earlier offset is acked. If the
> pod crashes at that point, the committed offset causes the earlier, un-acked
> message to be **silently skipped** — resulting in **message loss** with no
> error or warning.
>
> **Numaflow rejects this combination at validation time.** A MonoVertex spec
> with both `streaming: true` and a built-in Kafka source will fail admission
> with an explicit validation error. You must remove one or the other before the
> resource can be created or updated.

---

## Pulsar concurrency constraint

The built-in Pulsar source tracks unacknowledged messages using a Pulsar
`MaxUnacknowledgedMessages` policy. This is configured on the Pulsar source via
the `maxUnack` CRD field (default **1000**).

When `streaming: true` is enabled and `spec.limits.concurrency` exceeds
`maxUnack`, the Pulsar broker raises an `AckPendingExceeded` error and the
source stops delivering messages.

**Operator constraint (not enforced at runtime in PR1):** Keep
`spec.limits.concurrency` ≤ the Pulsar source's `maxUnack`. Example:

```yaml
spec:
  streaming: true
  limits:
    concurrency: 800   # must be <= pulsar source maxUnack (default 1000)
  source:
    pulsar:
      maxUnack: 1000
      # ...
```

No other built-in source has an equivalent cap. This constraint is
Pulsar-specific.

## Example spec

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: streaming-mvtx
spec:
  streaming: true
  limits:
    readBatchSize: 50
    concurrency: 200
  source:
    jetstream:
      # ...
  sink:
    udsink:
      container:
        image: my-sink:latest
```
