# Multi-partitioned Edges

To achieve higher throughput(> 10K but < 30K tps), users can create [pipelines](../../core-concepts/pipeline.md) with multi-partitioned edges. Multi-partitioned edges are only supported for [pipelines](../../core-concepts/pipeline.md) with JetStream as [Inter-Step Buffer](../../core-concepts/inter-step-buffer.md). Please ensure that the JetStream is provisioned with more nodes to support higher throughput.

Since partitions are owned by the vertex reading the data, to create a multi-partitioned edge we need to configure the vertex reading the data (to-vertex) to have multiple partitions.

The following code snippet provides an example of how to configure a vertex (in this case, the `cat` vertex) to have multiple partitions, which enables it (`cat` vertex) to read at a higher throughput.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: cat
      partitions: 3
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable # A UDF which simply cats the message
          imagePullPolicy: Always
```
