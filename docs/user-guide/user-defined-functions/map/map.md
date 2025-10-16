# Map UDF

Map in a Map vertex takes an input and returns 0, 1, or more outputs (also known as flat-map operation). Map is an element wise operator.

## Build Your Own UDF

You can build your own UDF in multiple languages.

Check the links below to see the UDF examples for different languages.

- [Python](https://github.com/numaproj/numaflow-python/tree/main/examples/map/)
- [Golang](https://github.com/numaproj/numaflow-go/tree/main/examples/mapper/)
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
instead of in a batch at the end.

Note that to maintain data orderliness, we restrict the read batch size to be `1`.

```yaml
spec:
  vertices:
    - name: my-vertex
      limits:
        # mapstreaming won't work if readBatchSize is != 1      
        readBatchSize: 1
```

### Streaming Mode Examples

Below are examples showing how to implement map streaming for flat-map operations:

=== "Go"
    ```go
    package main

    import (
      "context"
      "log"
      "strings"

      "github.com/numaproj/numaflow-go/pkg/mapstreamer"
    )

    // FlatMap is a MapStreamer that split the input message into multiple messages and stream them.
    type FlatMap struct {
    }

    func (f *FlatMap) MapStream(ctx context.Context, keys []string, d mapstreamer.Datum, messageCh chan<- mapstreamer.Message) {
      // we have to close to indicate the end of the stream, otherwise the client will wait forever.
      defer close(messageCh)
      msg := d.Value()
      _ = d.EventTime() // Event time is available
      _ = d.Watermark() // Watermark is available
      // Split the msg into an array with comma.
      strs := strings.Split(string(msg), ",")
      for _, s := range strs {
        messageCh <- mapstreamer.NewMessage([]byte(s))
      }
    }

    func main() {
      err := mapstreamer.NewServer(&FlatMap{}).Start(context.Background())
      if err != nil {
        log.Panic("Failed to start map stream function server: ", err)
      }
    }
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/mapstreamer/flatmap_stream/)

=== "Python"
    ```python
    import os
    from collections.abc import AsyncIterable
    from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer, MapStreamer


    class FlatMapStream(MapStreamer):
        async def handler(self, keys: list[str], datum: Datum) -> AsyncIterable[Message]:
            """
            A handler that splits the input datum value into multiple strings by `,` separator and
            emits them as a stream.
            """
            val = datum.value
            _ = datum.event_time
            _ = datum.watermark
            strs = val.decode("utf-8").split(",")

            if len(strs) == 0:
                yield Message.to_drop()
                return
            for s in strs:
                yield Message(str.encode(s))


    async def map_stream_handler(_: list[str], datum: Datum) -> AsyncIterable[Message]:
        """
        A handler that splits the input datum value into multiple strings by `,` separator and
        emits them as a stream.
        """
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")

        if len(strs) == 0:
            yield Message.to_drop()
            return
        for s in strs:
            yield Message(str.encode(s))


    if __name__ == "__main__":
        invoke = os.getenv("INVOKE", "func_handler")
        if invoke == "class":
            handler = FlatMapStream()
        else:
            handler = map_stream_handler
        grpc_server = MapStreamAsyncServer(handler)
        grpc_server.start()
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-python/tree/main/examples/mapstream/flatmap_stream/)

=== "Java"
    ```java
    package io.numaproj.numaflow.examples.mapstream.flatmapstream;

    import io.numaproj.numaflow.mapstreamer.Datum;
    import io.numaproj.numaflow.mapstreamer.MapStreamer;
    import io.numaproj.numaflow.mapstreamer.Message;
    import io.numaproj.numaflow.mapstreamer.OutputObserver;
    import io.numaproj.numaflow.mapstreamer.Server;


    /**
    * This is a simple User Defined Function example which processes the input message
    * and produces more than one output messages(flatMap) in a streaming mode
    * example : if the input message is "dog,cat", it streams two output messages
    * "dog" and "cat"
    */

    public class FlatMapStreamFunction extends MapStreamer {

        public static void main(String[] args) throws Exception {
            Server server = new Server(new FlatMapStreamFunction());

            // Start the server
            server.start();

            // wait for the server to shutdown
            server.awaitTermination();
        }

        public void processMessage(String[] keys, Datum data, OutputObserver outputObserver) {
            String msg = new String(data.getValue());
            String[] strs = msg.split(",");

            for (String str : strs) {
                outputObserver.send(new Message(str.getBytes()));
            }
        }
    }
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/mapstream/flatmapstream/)

### Batch Map Mode

BatchMap is an interface that allows developers to process multiple data items in a UDF single call,
rather than each item in separate calls.

The BatchMap interface can be helpful in scenarios where performing operations on a group of data can be more efficient.

#### Important Considerations

When using BatchMap, there are a few important considerations to keep in mind:

- Ensure that the BatchResponses object is tagged with the correct request ID. 
Each Datum has a unique ID tag, which will be used by Numaflow to ensure correctness.
- Ensure that the length of the BatchResponses list is equal to the number of requests received. This means that for 
every input data item, there should be a corresponding response in the BatchResponses list.
- The total batch size can be up to `readBatchSize` long.

### Batch Mode Examples

Below are examples showing how to implement batch map operations:

=== "Go"
    ```go
    package main

    import (
      "context"
      "log"
      "strings"

      "github.com/numaproj/numaflow-go/pkg/batchmapper"
    )

    func batchMapFn(_ context.Context, datums <-chan batchmapper.Datum) batchmapper.BatchResponses {
      batchResponses := batchmapper.BatchResponsesBuilder()
      for d := range datums {
        msg := d.Value()
        _ = d.EventTime() // Event time is available
        _ = d.Watermark() // Watermark is available
        batchResponse := batchmapper.NewBatchResponse(d.Id())
        strs := strings.Split(string(msg), ",")
        for _, s := range strs {
          batchResponse = batchResponse.Append(batchmapper.NewMessage([]byte(s)))
        }

        batchResponses = batchResponses.Append(batchResponse)
      }
      return batchResponses
    }

    func main() {
      err := batchmapper.NewServer(batchmapper.BatchMapperFunc(batchMapFn)).Start(context.Background())
      if err != nil {
        log.Panic("Failed to start batch map function server: ", err)
      }
    }
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/batchmapper/)

=== "Python"
    ```python
    from collections.abc import AsyncIterable

    from pynumaflow.batchmapper import (
        Message,
        Datum,
        BatchMapper,
        BatchMapAsyncServer,
        BatchResponses,
        BatchResponse,
    )


    class Flatmap(BatchMapper):
        """
        This is a class that inherits from the BatchMapper class.
        It implements a flatmap operation over a batch of input messages
        """

        async def handler(
            self,
            datums: AsyncIterable[Datum],
        ) -> BatchResponses:
            batch_responses = BatchResponses()
            async for datum in datums:
                val = datum.value
                _ = datum.event_time
                _ = datum.watermark
                strs = val.decode("utf-8").split(",")
                batch_response = BatchResponse.from_id(datum.id)
                if len(strs) == 0:
                    batch_response.append(Message.to_drop())
                else:
                    for s in strs:
                        batch_response.append(Message(str.encode(s)))
                batch_responses.append(batch_response)

            return batch_responses


    if __name__ == "__main__":
        """
        This example shows how to use the Batch Map Flatmap.
        We use a class as handler, but a function can be used as well.
        """
        grpc_server = BatchMapAsyncServer(Flatmap())
        grpc_server.start()
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-python/tree/main/examples/batchmap/)

=== "Java"
    ```java
    package io.numaproj.numaflow.examples.batchmap.flatmap;

    import io.numaproj.numaflow.batchmapper.BatchMapper;
    import io.numaproj.numaflow.batchmapper.BatchResponse;
    import io.numaproj.numaflow.batchmapper.BatchResponses;
    import io.numaproj.numaflow.batchmapper.Datum;
    import io.numaproj.numaflow.batchmapper.DatumIterator;
    import io.numaproj.numaflow.batchmapper.Message;
    import io.numaproj.numaflow.batchmapper.Server;
    import lombok.extern.slf4j.Slf4j;

    @Slf4j
    public class BatchFlatMap extends BatchMapper {
        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            BatchResponses batchResponses = new BatchResponses();
            while (true) {
                Datum datum = null;
                try {
                    datum = datumStream.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                // null means the iterator is closed so we are good to break the loop.
                if (datum == null) {
                    break;
                }
                try {
                    String msg = new String(datum.getValue());
                    String[] strs = msg.split(",");
                    BatchResponse batchResponse = new BatchResponse(datum.getId());
                    for (String str : strs) {
                        batchResponse.append(new Message(str.getBytes()));
                    }
                    batchResponses.append(batchResponse);
                } catch (Exception e) {
                    batchResponses.append(new BatchResponse(datum.getId()));
                }
            }
            return batchResponses;
        }

        public static void main(String[] args) throws Exception {
            Server server = new Server(new BatchFlatMap());

            // Start the server
            server.start();

            // wait for the server to shutdown
            server.awaitTermination();
        }
    }
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/batchmap/)

=== "Rust"
    ```rust
    use numaflow::batchmap;
    use numaflow::batchmap::{BatchResponse, Datum, Message};

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        batchmap::Server::new(Cat).start().await
    }

    struct Cat;

    #[tonic::async_trait]
    impl batchmap::BatchMapper for Cat {
        async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
            let mut responses: Vec<BatchResponse> = Vec::new();
            while let Some(datum) = input.recv().await {
                let mut response = BatchResponse::from_id(datum.id);
                response.append(Message::new(datum.value).with_keys(datum.keys.clone()));
                responses.push(response);
            }
            responses
        }
    }
    ```
    [View full examples on GitHub](https://github.com/numaproj/numaflow-rs/tree/main/examples/batchmap-cat/)

## Available Environment Variables

Some environment variables are available in the user-defined function container, they might be useful in your own UDF implementation.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

## Configuration

Configuration data can be provided to the UDF container at runtime multiple ways.

- [`environment variables`](../../reference/configuration/environment-variables.md)
- `args`
- `command`
- [`volumes`](../../reference/configuration/volumes.md)
- [`init containers`](../../reference/configuration/init-containers.md)