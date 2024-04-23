# User-defined Sinks

A `Pipeline` may have multiple Sinks, those sinks could either be a pre-defined sink such as `kafka`, `log`, etc., or a `User Defined Sink`.

A pre-defined sink vertex runs single-container pods, a user defined sink runs two-container pods.

## Build Your Own User-defined Sinks

You can build your own user-defined sinks in multiple languages.

Check the links below to see the examples for different languages.

- [Golang](https://github.com/numaproj/numaflow-go/tree/main/pkg/sinker/examples/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sink/simple/)
- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/sink/)

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

## User-defined Sink contributed from the open source community

If you're looking for examples and usages contributed by the open source community, head over to [the numaproj-contrib repositories](https://github.com/orgs/numaproj-contrib/repositories).

These user-defined sinks like AWS SQS, GCP Pub/Sub, provide valuable insights and guidance on how to use and write a user-defined sink.
