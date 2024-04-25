# Map UDF

Map in a Map vertex takes an input and returns 0, 1, or more outputs (also known as flat-map operation). Map is an element wise operator.

## Builtin UDF

There are some [Built-in Functions](builtin-functions/README.md) that can be used directly.

## Build Your Own UDF

You can build your own UDF in multiple languages.

Check the links below to see the UDF examples for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/map/)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/pkg/mapper/examples/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/map/)

After building a docker image for the written UDF, specify the image as below in the vertex spec.

```yaml
spec:
  vertices:
    - name: my-vertex
      udf:
        container:
          image: my-python-udf-example:latest
```

### Streaming Mode

In cases the map function generates more than one output (e.g., flat map), the UDF can be
configured to run in a streaming mode instead of batching, which is the default mode.
In streaming mode, the messages will be pushed to the downstream vertices once generated
instead of in a batch at the end. The streaming mode can be enabled by setting the annotation
`numaflow.numaproj.io/map-stream` to `true` in the vertex spec.

Note that to maintain data orderliness, we restrict the read batch size to be `1`.

```yaml
spec:
  vertices:
    - name: my-vertex
      metadata:
        annotations:
          numaflow.numaproj.io/map-stream: "true"
      limits:
        # mapstreaming won't work if readBatchSize is != 1      
        readBatchSize: 1
```

Check the links below to see the UDF examples in streaming mode for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/mapstream/flatmap_stream/)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/pkg/mapstreamer/examples/flatmap_stream/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/mapstream/flatmapstream/)

### Available Environment Variables

Some environment variables are available in the user-defined function container, they might be useful in your own UDF implementation.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

### Configuration

Configuration data can be provided to the UDF container at runtime multiple ways.

- [`environment variables`](../../reference/configuration/environment-variables.md)
- `args`
- `command`
- [`volumes`](../../reference/configuration/volumes.md)
- [`init containers`](../../reference/configuration/init-containers.md)
