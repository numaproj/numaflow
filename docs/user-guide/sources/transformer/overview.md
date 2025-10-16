# Source Data Transformer

The Source Data Transformer is a feature that allows users to execute custom code to transform their data at source.

This functionality offers two primary advantages to users:

1. Event Time Assignment - It enables users to extract the event time from the message payload, providing a more precise and accurate event time than the default mechanisms like LOG_APPEND_TIME of Kafka for Kafka source, custom HTTP header for HTTP source, and others.
2. Early data processing - It pre-processes the data, or filters out unwanted data at source vertex, saving the cost of creating another UDF vertex and an inter-step buffer.

Source Data Transformer runs as a sidecar container in a Source Vertex Pod. Data processing in the transformer is supposed to be idempotent.
The communication between the main container (platform code) and the sidecar container (user code) is through gRPC over Unix Domain Socket.

## Build Your Own Transformer

You can build your own transformer in multiple languages.
A user-defined transformer could be as simple as the example below in Golang.
In the example, the transformer extracts event times from `timestamp` of the JSON payload and assigns them to messages as new event times. It also filters out unwanted messages based on `filterOut` of the payload.

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/numaproj/numaflow-go/pkg/sourcetransformer"
)

func transform(_ context.Context, keys []string, data sourcetransformer.Datum) sourcetransformer.Messages {
	/*
		Input messages are in JSON format. Sample: {"timestamp": "1673239888", "filterOut": "true"}.
		Field "timestamp" shows the real event time of the message, in the format of epoch.
		Field "filterOut" indicates whether the message should be filtered out, in the format of boolean.
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
		return sourcetransformer.MessagesBuilder().Append(sourcetransformer.MessageToDrop(eventTime))
	} else {
		return sourcetransformer.MessagesBuilder().Append(sourcetransformer.NewMessage(data.Value(), eventTime).WithKeys(keys))
	}
}

func main() {
	err := sourcetransformer.NewServer(sourcetransformer.SourceTransformFunc(transform)).Start(context.Background())
	if err != nil {
		log.Panic("Failed to start source transform server: ", err)
	}
}
```

Below are complete examples in various programming languages, where we apply conditional forwarding based on the input event time:

=== "Go"
    ```go
    package main

    import (
        "context"
        "log"

        "event_time_filter/impl"

        "github.com/numaproj/numaflow-go/pkg/sourcetransformer"
    )

    func transform(_ context.Context, keys []string, d sourcetransformer.Datum) sourcetransformer.Messages {
        return impl.FilterEventTime(keys, d)
    }

    func main() {
        err := sourcetransformer.NewServer(sourcetransformer.SourceTransformFunc(transform)).Start(context.Background())
        if err != nil {
            log.Panic("Failed to start source transform server: ", err)
        }
    }
    ```
    [View full implementation on GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/sourcetransformer/event_time_filter)

=== "Python"
    ```python
    import datetime
    import logging
    from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformServer

    """
    This is a simple User Defined Function example which receives a message, applies the following
    data transformation, and returns the message.
    If the message event time is before year 2022, drop the message with event time unchanged.
    If it's within year 2022, update the tag to "within_year_2022" and 
    update the message event time to Jan 1st 2022.
    Otherwise, (exclusively after year 2022), update the tag to "after_year_2022" and update the
    message event time to Jan 1st 2023.
    """

    january_first_2022 = datetime.datetime.fromtimestamp(1640995200)
    january_first_2023 = datetime.datetime.fromtimestamp(1672531200)


    def my_handler(keys: list[str], datum: Datum) -> Messages:
        val = datum.value
        event_time = datum.event_time
        messages = Messages()

        if event_time < january_first_2022:
            logging.info("Got event time:%s, it is before 2022, so dropping", event_time)
            messages.append(Message.to_drop(event_time))
        elif event_time < january_first_2023:
            logging.info(
                "Got event time:%s, it is within year 2022, so forwarding to within_year_2022",
                event_time,
            )
            messages.append(
                Message(value=val, event_time=january_first_2022, tags=["within_year_2022"])
            )
        else:
            logging.info(
                "Got event time:%s, it is after year 2022, so forwarding to after_year_2022", event_time
            )
            messages.append(Message(value=val, event_time=january_first_2023, tags=["after_year_2022"]))
        return messages


    if __name__ == "__main__":
        grpc_server = SourceTransformServer(my_handler)
        grpc_server.start()
    ```
    [View full implementation on GitHub](https://github.com/numaproj/numaflow-python/tree/main/examples/sourcetransform/event_time_filter)

=== "Java"
    ```java
    package io.numaproj.numaflow.examples.sourcetransformer.eventtimefilter;

    import io.numaproj.numaflow.sourcetransformer.Datum;
    import io.numaproj.numaflow.sourcetransformer.Message;
    import io.numaproj.numaflow.sourcetransformer.MessageList;
    import io.numaproj.numaflow.sourcetransformer.Server;
    import io.numaproj.numaflow.sourcetransformer.SourceTransformer;

    import java.time.Instant;

    /**
     * This is a simple User Defined Function example which receives a message, applies the following
     * data transformation, and returns the message.
     * <p>
     * If the message event time is before year 2022, drop the message with the original event time.
     * If it's within year 2022, update the tag to "within_year_2022" and update the message event time to Jan 1st 2022.
     * Otherwise, (exclusively after year 2022), update the tag to "after_year_2022" and update the
     * message event time to Jan 1st 2023.
     */
    public class EventTimeFilterFunction extends SourceTransformer {
        private static final Instant januaryFirst2022 = Instant.ofEpochMilli(1640995200000L);
        private static final Instant januaryFirst2023 = Instant.ofEpochMilli(1672531200000L);

        public static void main(String[] args) throws Exception {
            Server server = new Server(new EventTimeFilterFunction());

            // Start the server
            server.start();

            // wait for the server to shut down
            server.awaitTermination();
        }

        public MessageList processMessage(String[] keys, Datum data) {
            Instant eventTime = data.getEventTime();

            if (eventTime.isBefore(januaryFirst2022)) {
                return MessageList.newBuilder().addMessage(Message.toDrop(eventTime)).build();
            } else if (eventTime.isBefore(januaryFirst2023)) {
                return MessageList
                        .newBuilder()
                        .addMessage(
                                new Message(
                                        data.getValue(),
                                        januaryFirst2022,
                                        null,
                                        new String[]{"within_year_2022"}))
                        .build();
            } else {
                return MessageList
                        .newBuilder()
                        .addMessage(new Message(
                                data.getValue(),
                                januaryFirst2023,
                                null,
                                new String[]{"after_year_2022"}))
                        .build();
            }
        }
    }
    ```
    [View full implementation on GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sourcetransformer/eventtimefilter)

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