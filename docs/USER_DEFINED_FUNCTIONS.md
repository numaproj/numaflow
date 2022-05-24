# User Defined Functions

A `Pipleline` consists of multiple vertices, `Source`, `Sink` and `UDF(User Defined Functions)`.

UDF runs as a container in a Vertex Pod, processes the received data.

Data processing in the UDF is supposed to be idempotent.

## Builtin UDF

There are some [builtin UDFs](./builtin-functions/) can be used directly.

**Cat**

A `cat` builtin UDF does nothing but return the same messages it receives.

```yaml
spec:
  vertices:
    - name: cat-vertex
      udf:
        builtin:
          name: cat
```

**Filter**

A `filter` builtin UDF does filter the message based on expression. `payload` keyword represents message object.
see documentation for expression [here](FILTER_EXPRESSION.md)

```yaml
spec:
  vertices:
    - name: filter-vertex
      udf:
        builtin:
          name: filter
          kwargs:
            expression: int(object(payload).id) > 100
```

## Build Your UDF

You can build your own UDF in different languages [[Python](../sdks/python) | [Golang](../sdks/golang/)].

Following yaml shows how to specify a customized UDF.

```yaml
spec:
  vertices:
    - name: my-vertex
      udf:
        container:
          image: my-python-udf-example:latest
```

## Available Environment Variables

Some environment variables are available in the user defined function Pods:

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.
