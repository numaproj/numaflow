# Map UDF

Map in a Map vertex takes an input and returns 0, 1, or more outputs (also known as flat-map operation). Map is an element wise operator.

After building a docker image for the written UDF, specify the image as below in the vertex spec.

```yaml
spec:
  vertices:
    - name: my-vertex
      udf:
        container:
          image: my-python-udf-example:latest
```

Map supports three modes: [Unary](#unary-mode), [Streaming](#streaming-mode), and [Batch](#batch-mode).

## Unary Mode

Unary Map is the default mode where each input message is processed individually and returns 0, 1, or more outputs.

Check the links below to see the UDF examples for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/map/)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/examples/mapper/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/map/)

## Streaming Mode

In cases the map function generates more than one output (e.g., flat map), the UDF can be
configured to run in a streaming mode instead of batching, which is the default mode.
In streaming mode, the messages will be pushed to the downstream vertices once generated
instead of in a batch at the end.

Check the links below to see the UDF examples in streaming mode for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/mapstream/flatmap_stream/)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/examples/mapstreamer/flatmap_stream/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/mapstream/flatmapstream/)

## Batch Mode

BatchMap is an interface that allows developers to process multiple data items in a UDF single call,
rather than each item in separate calls.

The BatchMap interface can be helpful in scenarios where performing operations on a group of data can be more efficient.

### Important Considerations

When using BatchMap, there are a few important considerations to keep in mind:

- Ensure that the BatchResponses object is tagged with the correct request ID.
Each Datum has a unique ID tag, which will be used by Numaflow to ensure correctness.
- Ensure that the length of the BatchResponses list is equal to the number of requests received. This means that for
every input data item, there should be a corresponding response in the BatchResponses list.
- The total batch size can be up to `readBatchSize` long.

Check the links below to see the UDF examples in batch mode for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/batchmap/)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/examples/batchmapper/)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/batchmap/)
- [Rust](https://github.com/numaproj/numaflow-rs/tree/main/examples/batchmap-cat/)

## Available Environment Variables

Some environment variables are available in the user-defined function container, they might be useful in your own UDF implementation.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

## Configuration

To achieve ordering, please set `readBatchSize` to 1.

Configuration data can be provided to the UDF container at runtime multiple ways.

- [`environment variables`](../../reference/configuration/environment-variables.md)
- `args`
- `command`
- [`volumes`](../../reference/configuration/volumes.md)
- [`init containers`](../../reference/configuration/init-containers.md)
