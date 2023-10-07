# Source Data Transformer

The Source Data Transformer is a feature that allows users to execute custom code to transform their data at source.

This functionality offers two primary advantages to users:

1. Event Time Assignment - It enables users to extract the event time from the message payload, providing a more precise and accurate event time than the default mechanisms like LOG_APPEND_TIME of Kafka for Kafka source, custom HTTP header for HTTP source, and others.
2. Early data processing - It pre-processes the data, or filters out unwanted data at source vertex, saving the cost of creating another UDF vertex and an inter-step buffer.

Source Data Transformer runs as a sidecar container in a Source Vertex Pod. Data processing in the transformer is supposed to be idempotent.
The communication between the main container (platform code) and the sidecar container (user code) is through gRPC over Unix Domain Socket.

## Built-in Transformers

There are some [Built-in Transformers](builtin-transformers/README.md) that can be used directly.

## Build Your Own Transformer

You can build your own transformer in multiple languages. A User Defined Transformer could be as simple as the example below in Golang.
In the example, the transformer extracts event times from `timestamp` of the JSON payload and assigns them to messages as new event times. It also filters out unwanted messages based on `filterOut` of the payload.

```golang
package main

import (
	"context"
	"encoding/json"
	"time"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/server"
)

func Handle(_ context.Context, keys []string, data functionsdk.Datum) functionsdk.MessageTs {
	/*
		Input messages are in JSON format. Sample: {"timestamp": "1673239888", "filterOut": "true"}.
		Field "timestamp" shows the real event time of the message, in format of epoch.
		Field "filterOut" indicates whether the message should be filtered out, in format of boolean.
	*/
	var jsonObject map[string]interface{}
	json.Unmarshal(data.Value(), &jsonObject)

	// event time assignment
	eventTime := data.EventTime()
	// if timestamp field exists, extract event time from payload.
	if ts, ok := jsonObject["timestamp"]; ok {
		eventTime = time.Unix(int64(ts.(float64)), 0)
	}

	// data filtering
	var filterOut bool
	if f, ok := jsonObject["filterOut"]; ok {
		filterOut = f.(bool)
	}
	if filterOut {
		return functionsdk.MessageTsBuilder().Append(functionsdk.MessageTToDrop())
	} else {
		return functionsdk.MessageTsBuilder().Append(functionsdk.NewMessageT(data.Value(), eventTime).WithKeys(keys))
	}
}

func main() {
	server.New().RegisterMapperT(functionsdk.MapTFunc(Handle)).Start(context.Background())
}
```

Check the links below to see another transformer example in various programming languages, where we apply conditional forwarding based on the input event time.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/sourcetransform/event_time_filter)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/pkg/sourcetransformer/examples/event_time_filter)
- [Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sourcetransformer/eventtimefilter)

After building a docker image for the written transformer, specify the image as below in the source vertex spec.

```yaml
spec:
  vertices:
    - name: my-vertex
      source:
        http: {}
        transformer:
          container:
            image: my-python-transformer-example:latest
```

### Available Environment Variables

Some environment variables are available in the source transformer container, they might be useful in you own source data transformer implementation.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

### Configuration

Configuration data can be provided to the transformer container at runtime multiple ways.

- [`environment variables`](../../reference/configuration/environment-variables.md)
- `args`
- `command`
- [`volumes`](../../reference/configuration/volumes.md)
- [`init containers`](../../reference/configuration/init-containers.md)
