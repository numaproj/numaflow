# Joins and Cycles

As of Numaflow v0.10, Pipeline Edges can be defined such that multiple Vertices send to a single vertex. This includes:

- UDF Map Vertices
- UDF Reduce Vertices
- Sink Vertices

Please see the following examples:

- [Join on Map Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-map.yaml)
- [Join on Reduce Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-reduce.yaml)
- [Join on Sink Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-sink.yaml)

## Cycles

A special case of a "Join" is a **Cycle** (a Vertex which can send either to itself or to a previous Vertex.) An example use of this is a Map UDF which does some sort of reprocessing of data under certain conditions such as a transient error.

Cycles are permitted, except in the case that there's a Reduce Vertex at or downstream of the cycle. (This is because a cycle inevitably produces late data, which would get dropped by the Reduce Vertex. For this reason, cycles should be used sparingly.)

The following examples are of Cycles:

- [Cycle to Self](https://github.com/numaproj/numaflow/blob/main/examples/10-cycle-to-self.yaml)
- [Cycle to Previous](https://github.com/numaproj/numaflow/blob/main/examples/10-cycle-to-prev.yaml)