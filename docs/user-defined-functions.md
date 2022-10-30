# User Defined Functions

A `Pipeline` consists of multiple vertices, `Source`, `Sink` and `UDF(User Defined Functions)`.

UDF runs as a sidecar container in a Vertex Pod, processes the received data. The communication between the main container (platform code) and the sidecar container (user code) is through gRPC over Unix Domain Socket.

Data processing in the UDF is supposed to be idempotent.

## Builtin UDF

There are some [Built-in Functions](./builtin-functions/README.md) that can be used directly.

## Build Your Own UDF

You can build your own UDF in multiple languages. A User Defined Function could be as simple as below in Golang.

```golang
package main

import (
	"context"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/server"
)

func mapHandle(_ context.Context, key string, d functionsdk.Datum) functionsdk.Messages {
	// Directly forward the input to the output
	return functionsdk.MessagesBuilder().Append(functionsdk.MessageToAll(d.Value()))
}

func main() {
	server.New().RegisterMapper(functionsdk.MapFunc(mapHandle)).Start(context.Background())
}
```

Check the links below to see the UDF examples for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/function)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/pkg/function/examples)

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

### Configuration

Configuration data can be provided to the UDF container at runtime multiple ways.
* [`environment variables`](./environment-variables.md)
* `args`
* `command`
* [`volumes`](./volumes.md)
* [`init containers`](./init-containers.md)
