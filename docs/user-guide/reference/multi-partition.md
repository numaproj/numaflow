# Multi-partitioned Edges

To achieve high throughput(>10k), users can create a vertex with multiple partitions. 
Multi partitioned edges are only supported for pipelines with jetstream as ISB. 

The following example shows how to create a pipeline with multi-partitioned edges.

```yaml
    - name: cat
      partitions: 3
      udf:
        builtin:
          name: cat # A built-in UDF which simply cats the message
```



