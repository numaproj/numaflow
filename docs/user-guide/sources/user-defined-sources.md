# User-defined Sources

A `Pipeline` may have multiple Sources, those sources could either be a pre-defined source such as `kafka`, `http`, etc., or a `user-defined source`.

With no source data transformer, A pre-defined source vertex runs single-container pods; a user-defined source runs two-container pods.

## Build Your Own User-defined Sources

You can build your own user-defined sources in multiple languages.

Check the links below to see the examples for different languages.

- [Golang](https://github.com/numaproj/numaflow-go/tree/main/examples/sourcer/simple_source/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/source/simple/)
- [Python](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/source/simple_source)

After building a docker image for the written user-defined source, specify the image as below in the vertex spec.

```yaml
spec:
  vertices:
    - name: input
      source:
        udsource:
          container:
            image: my-source:latest
```

## Available Environment Variables

Some environment variables are available in the user-defined source container:

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

## User-defined sources contributed from the open source community

If you're looking for examples and usages contributed by the open source community, head over to [the numaproj-contrib repositories](https://github.com/orgs/numaproj-contrib/repositories).

These user-defined sources like AWS SQS, GCP Pub/Sub, provide valuable insights and guidance on how to use and write a user-defined source.
