# User Defined Functions

A `Pipleline` consists of multiple vertices, `Source`, `Sink` and `UDF(User Defined Functions)`.

UDF runs as a container in a Vertex Pod, processes the received data.

Data processing in the UDF is supposed to be idempotent.

## Builtin UDF

There are some `Builtin Functions` that can be used directly.

## Build Your Own UDF

You can build your own UDF in multiple languages. A User Defined Function could be as simple as below in Golang.

```golang
package main

import (
	"context"

	funcsdk "github.com/numaproj/numaflow-go/function"
)

// Simply return the same msg
func handle(ctx context.Context, key, msg []byte) (funcsdk.Messages, error) {
	return funcsdk.MessagesBuilder().Append(funcsdk.MessageToAll(msg)), nil
}

func main() {
	funcsdk.Start(context.Background(), handle)
}
```

Check the links below to see the UDF examples for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/function)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/examples/function)

After building a docker image for the written UDF, specify the image as below in the vertex spec.

```yaml
spec:
  vertices:
    - name: my-vertex
      udf:
        container:
          image: my-python-udf-example:latest
```

### Available Environment Variables

Some environment variables are available in the user defined function Pods, they might be useful in you own UDF implementation.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.
