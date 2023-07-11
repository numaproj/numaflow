# Multi-partitioned Edges

To achieve high throughput(>10k), users can create multi partitioned edges.
Multi partitioned edges are only supported for pipelines with jetstream as ISB. 

Since partitions are owned by the vertex that reads from it, to create multi partitioned edge
we need to configure the vertex that reads from it to have multiple partitions.

Below is the example snippet to configure a vertex to have multiple partitions.

```yaml
    - name: cat
      partitions: 3
      udf:
        builtin:
          name: cat # A built-in UDF which simply cats the message
```



