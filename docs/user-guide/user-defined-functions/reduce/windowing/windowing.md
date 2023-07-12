# Windowing

## Overview

In the world of data processing on an unbounded stream, Windowing is a concept
of grouping data using temporal boundaries. We use event-time to discover
temporal boundaries on an unbounded, infinite stream and [Watermark](../../../../core-concepts/watermarks.md) to ensure
the datasets within the boundaries are complete. The [reduce](../reduce.md) is
applied on these grouped datasets.
For example, when we say, we want to find number of users online per minute, we use
windowing to group the users into one minute buckets.

The entirety of windowing is under the `groupBy` section.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window: ...
        keyed: ...
```

Since a window can be [Non-Keyed v/s Keyed](#non-keyed-vs-keyed-windows),
we have an explicit field called `keyed`to differentiate between both (see below).

Under the `window` section we will define different types of windows.

## Window Types

Numaflow supports the following types of windows

- [Fixed](fixed.md)
- [Sliding](sliding.md)

## Non-Keyed v/s Keyed Windows

### Non-Keyed

A `non-keyed` partition is a partition where the window is the boundary condition.
Data processing on a non-keyed partition cannot be scaled horizontally because
only one partition exists.

A non-keyed partition is usually used after aggregation and is hardly seen at
the head section of any data processing pipeline.
(There is a concept called Global Window where there is no windowing, but
let us table that for later).

### Keyed

A `keyed` partition is a partition where the partition boundary is a composite
key of both the window and the key from the payload (e.g., GROUP BY country,
where country names are the keys). Each smaller partition now has a complete
set of datasets for that key and boundary. The subdivision of dividing a huge
window-based partition into smaller partitions by adding keys along with the
window will help us horizontally scale the distribution.

Keyed partitions are heavily used to aggregate data and are frequently seen
throughout the processing pipeline. We could also convert and non-keyed problem
to a set of keyed problems and apply a non-keyed function at the end. This will
help solve the original problem in a scalable manner without affecting the
result's completeness and/or accuracy.

When a `keyed` window is used, an optional `partitions` can be specified in the
vertex for parallel processing.

### Usage

Numaflow support both Keyed and Non-Keyed windows. We set `keyed` to either
`true` (keyed) or `false` (non-keyed). Please note that the non-keyed windows
are not horizontally scalable as mentioned above.

```yaml
vertices:
  - name: my-reduce
    partitions: 5 # Optional, defaults to 1
    udf:
      groupBy:
        window: ...
        keyed: true # Optional, defaults to false
```
