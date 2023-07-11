# Multi-partitioned Edges

To achieve high throughput(>10k), users can create a multi partitioned edges.
Multi partitioned edges are only supported for pipelines with jetstream as ISB. 

The following snippet shows how to configure a vertex with multiple partitions:

```yaml
    - name: cat
      partitions: 3
      udf:
        builtin:
          name: cat # A built-in UDF which simply cats the message
```



