# Reduce UDF

## Overview

Reduce is one of the most commonly used abstractions in a stream processing pipeline to define
aggregation functions on a stream of data. It is the reduce feature that helps us solve problems like
"performs a summary operation(such as counting the number of occurrences of a key, yielding user login
frequencies), etc. "Since the input is an unbounded stream (with infinite entries), we need an additional
parameter to convert the unbounded problem to a bounded problem and provide results on that. That
bounding condition is "time", eg, "number of users logged in per minute". So while processing an
unbounded stream of data, we need a way to group elements into finite chunks using time. To build these
chunks, the reduce function is applied to the set of records produced using the concept of [windowing](./windowing/windowing.md).

## Reduce Pseudo code

Unlike in _map_ vertex where only an element is given to user-defined function, in _reduce_ since
there is a group of elements, an iterator is passed to the reduce function. The following is a generic
outlook of a reduce function. I have written the pseudo-code using the accumulator to show that very
powerful functions can be applied using this reduce semantics.

```python
# reduceFn func is a generic reduce function that processes a set of elements
def reduceFn(keys: List[str], datums: Iterator[Datum], md: Metadata) -> Messages:
    # initialize_accumalor could be any function that starts of with an empty
    # state. eg, accumulator = 0
    accumulator = initialize_accumalor()
    # we are iterating on the input set of elements
    for d in datums:
        # accumulator.add_input() can be any function.
        # e.g., it could be as simple as accumulator += 1
        accumulator.add_input(d)
    # once we are done with iterating on the elements, we return the result
    # acumulator.result() can be str.encode(accumulator)
    return Messages(Message(acumulator.result(), keys))
```

## Specification

The structure for defining a reduce vertex is as follows.

```yaml
- name: my-reduce-udf
  udf:
    container:
      image: my-reduce-udf:latest
    groupBy:
      window: ...
      keyed: ...
      storage: ...
```

The reduce spec adds a new section called `groupBy` and this how we differentiate a _map_ vertex
from _reduce_ vertex. There are two important fields, the [_window_](./windowing/windowing.md)
and [_keyed_](./windowing/windowing.md#non-keyed-vs-keyed-windows). These two fields play an
important role in grouping the data together and pass it to the user-defined reduce code.

The reduce supports parallelism processing by defining a `partitions` in the vertex. This is because auto-scaling is not supported in reduce vertex. If `partitions` is not defined default of one will be used.

```yaml
- name: my-reduce-udf
  partitions: integer
```

It is wrong to give a `partitions` > `1` if it is a _non-keyed_ vertex (`keyed: false`).

There are a couple of [examples](examples.md) that demonstrate Fixed windows, Sliding windows,
chaining of windows, keyed streams, etc.

## Time Characteristics

All windowing operations generate new records as an output of reduce operations. Event-time and Watermark
are two main primitives that determine how the time propagates in a streaming application. so for all new
records generated in a reduce operation, event time is set to the end time of the window.

For example, for a reduce operation over a keyed/non-keyed window with a start and end defined by
`[2031-09-29T18:47:00Z, 2031-09-29T18:48:00Z)`, event time for all the records generated will be set to
`2031-09-29T18:47:59.999Z` since millisecond is the smallest granularity (as of now) event time is set to
the last timestamp that belongs to a window.

Watermark is treated similarly, the watermark is set to the last timestamp for a given window.
So for the example above, the value of the watermark will be set to the last timestamp, i.e., `2031-09-29T18:47:59.999Z`.

This applies to all the window types regardless of whether they are keyed or non-keyed windows.

## Allowed Lateness

`allowedLateness` flag on the Reduce vertex will allow late data to be processed by slowing the down the close-of-book 
operation of the Reduce vertex. Late data will be included for the Reduce operation as long as the late data is not 
later than `(CurrentWatermark - AllowedLateness)`. Without `allowedLateness`, late data will be rejected and dropped. 
The key reason for introducing `allowedLateness` is to support further out-of-order data beyond the watermark 
withholding, esp. when there are multiple reducers in the pipeline. Each Reduce vertex can have its own `allowedLateness`,
but the watermark delay is additive.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        allowedLateness: 5s # Optional, allowedLateness is disabled by default
```

## Storage

Reduce unlike map requires persistence. To support persistence user has to define the
`storage` configuration. We replay the data stored in this storage on pod startup if there has
been a restart of the reduce pod caused due to pod migrations, etc.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        storage: ....
```

### Persistent Volume Claim (PVC)

`persistentVolumeClaim` supports the following fields, `volumeSize`, `storageClassName`, and`accessMode`.
As name suggests,`volumeSize` specifies the size of the volume. `accessMode` can be of many types, but for
reduce use case we need only `ReadWriteOncePod`. `storageClassName` can also be provided, more info on storage class
can be found [here](https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1). The default
value of `storageClassName` is `default` which is default StorageClass may be deployed to a Kubernetes
cluster by addon manager during installation.

#### Example

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        storage:
          persistentVolumeClaim:
            volumeSize: 10Gi
            accessMode: ReadWriteOncePod
```

### EmptyDir

We also support `emptyDir` for quick experimentation. We do not recommend this in production
setup. If we use `emptyDir`, we will end up in data loss if there are pod migrations. `emptyDir`
also takes an optional `sizeLimit`. `medium` field controls where emptyDir volumes are stored.
By default emptyDir volumes are stored on whatever medium that backs the node such as disk, SSD,
or network storage, depending on your environment. If you set the `medium` field to `"Memory"`,
Kubernetes mounts a tmpfs (RAM-backed filesystem) for you instead.

#### Example

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        storage:
          emptyDir: {}
```
