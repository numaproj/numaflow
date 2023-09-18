# User Defined Sinks

A `Pipeline` may have multiple Sinks, those sinks could either be a pre-defined sink such as `kafka`, `log`, etc, or a `User Defined Sink`.

A pre-defined sink vertex runs single-container pods, a user defined sink runs two-container pods.

A user defined sink vertex looks like below.

```yaml
spec:
  vertices:
    - name: output
      sink:
        udsink:
          container:
            image: my-sink:latest
```

## Available Environment Variables

Some environment variables are available in the user defined sink container:

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.
