# Environment Variables

For the `numa` container of vertex pods, environment variable `NUMAFLOW_DEBUG` can be set to `true` for [debugging](../../../development/debugging.md).

In [`udf`](../../user-defined-functions/map/map.md), [`udsink`](../../sinks/user-defined-sinks.md) and [`transformer`](../../sources/transformer/overview.md) containers, there are some preset environment variables that can be used directly.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.
- `NUMAFLOW_CPU_REQUEST` - `resources.requests.cpu`, roundup to N cores, `0` if missing.
- `NUMAFLOW_CPU_LIMIT` - `resources.limits.cpu`, roundup to N cores, use host cpu cores if missing.
- `NUMAFLOW_MEMORY_REQUEST` - `resources.requests.memory` in bytes, `0` if missing.
- `NUMAFLOW_MEMORY_LIMIT` - `resources.limits.memory` in bytes, use host memory if missing.

For setting environment variables on pods not owned by a vertex, see [Pipeline Customization](pipeline-customization.md).

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

Similarly, `envFrom` also can be specified in `udf` or `udsink` containers.

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
          envFrom:
            - configMapRef:
                name: my-config
    - name: my-sink
      sink:
        udsink:
          container:
            image: my-sink:latest
            envFrom:
              - secretRef:
                  name: my-secret
```
