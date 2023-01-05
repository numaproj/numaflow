# Reduce UDF

Reduce is one of the most commonly used abstractions in a stream processing pipeline to define 
aggregation functions on a stream of data. It is the reduce feature that helps us solve problems like 
"performs a summary operation(such as counting the number of occurrence of a key, yielding user login 
frequencies), etc."Since the input an unbounded stream (with infinite entries), we need an additional
parameter to convert the unbounded problem to a bounded problem and provide results on that. That
bounding condition is "time", eg, "number of users logged in per minute". So while processing an 
unbounded stream of data, we need a way to group elements into finite chunks using time. To build these
chunks the reduce function is applied to the set of records produced using the concept of [windowing](./windowing/windowing.md).

Unlike in _map_ vertex where only an element is given to user-defined function, in _reduce_ since
there is a group of elements, an iterator is passed to the reduce function.

```python
# counter counts the number of elements in the array
# e.g. use-case of counter is counting number of users online per minute
def counter(key: str, datums: Iterator[Datum], md: Metadata) -> Messages:
    counter = 0
    for _ in datums:
        counter += 1
     return Messages(Message.to_vtx(key, str.encode(msg)))
```

The structure for defining a reduce vertex is as follows.
```yaml
    - name: my-reduce-udf
      udf:
        container:
          image: my-reduce-udf:latest
        groupBy:
          window:
            ...
          keyed: ...
```

The reduce spec adds a new section called `groupBy` and this how we differentiate a _map_ vertex
from _reduce_ vertex. There are two important fields, the [_window_](./windowing/windowing.md)
and [_keyed_](./windowing/windowing.md#non-keyed-vs-keyed-windows). These two fields play an
important role in grouping the data together and pass it to the user-defined reduce code.

The reduce supports a parallelism value while defining the edge. This is because auto-scaling is 
not supported in reduce vertex. If `parallelism` is not defined default of one will be used.

```yaml
    - from: edge1
      to: my-reduce-reduce
      parallelism: integer
```

It is wrong to give a `parallelism` > `1` if it is a _non-keyed_ vertex (`keyed: false`).


# Examples

## Prerequisites

Install the ISB

```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```

## sum pipeline using fixed window
This is a simple reduce pipeline that just does summation (sum of numbers) but uses fixed window.
The snippet for the reduce vertex is as follows. [6-reduce-fixed-window.yaml](....)  has the 
complete pipeline definition.

TODO: put in the image

```yaml
    - name: compute-sum
      udf:
        container:
          # compute the sum
          image: quay.io/numaio/numaflow-go/reduce-sum
        groupBy:
          window:
            fixed:
              length: 60s
          keyed: true
```

In this example we use a `parallelism` of `2`. We are setting a parallelism > 1 because it is a 
keyed window.

```yaml
    - from: atoi
      to: compute-sum
      parallelism: 2
```

```shell
kubectl apply -f https://github.com/numaproj/numaflow/blob/main/examples/examples/6-reduce-fixed-window.yaml
```

TODO: copy paste the log.. no need to put the command for getting logs.

TODO: explain the result.

## sum pipeline using sliding window
This is a simple reduce pipeline that just does summation (sum of numbers).

```shell
kubectl apply -f https://github.com/numaproj/numaflow/blob/main/examples/examples/6-reduce-fixed-window.yaml
```

## complex reduce pipeline

In the complex reduce example, we will 
* chain of reduce functions
* use both fixed and sliding windows
* use keyed and non-keyed windowing

TODO: draw the diagram, no need of snippets