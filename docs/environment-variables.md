# Environment Variables

For the `numa` container of vertex pods, environment variable `NUMAFLOW_DEBUG` can be set to `true` for [debugging](./debugging.md).

In [`udf`](./user-defined-functions.md) and [`udsink`](./sinks/user-defined-sinks.md) containers, there are some preset environment variables that can be used directly.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

For setting environment variables on pods not owned by a vertex, see [Pipeline Customization](./pipeline-customization.md).

## Your Own Environment Variables

To add your own environment variables to `udf` or `udsink` containers, check the example below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      udf:
        container:
          image: my-function:latest
          env:
            - name: env01
              value: value01
            - name: env02
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: my-key
    - name: my-sink
      sink:
        udsink:
          container:
            image: my-sink:latest
            env:
              - name: env03
                value: value03
```
