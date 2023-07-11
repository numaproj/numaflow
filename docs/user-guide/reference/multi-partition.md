# Multi-partitioned Edges

To achieve higher throughput(> 10K but < 25K), users can create multi-partitioned edges.
Multi-partitioned edges are only supported for pipelines with JetStream as ISB. Please ensure
that the JetStream is provisioned with more nodes to support higher throughput.

Since partitions are owned by the vertex that reads from it, to create a multi-partitioned edge
we need to configure the vertex that reads from it to have multiple partitions.

Below is the example snippet to configure a vertex (`cat` vertex) to have multiple partitions,
this means vertex reading from `cat` can read at high TPS.

```yaml
    - name: cat
      partitions: 3
      udf:
        builtin:
          name: cat # A built-in UDF which simply cats the message
```



