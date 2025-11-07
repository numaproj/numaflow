# Side Inputs

For an unbounded [pipeline](../../core-concepts/pipeline.md) in Numaflow that never terminates, there are many cases where users want to update a configuration of the UDF without restarting the pipeline. Numaflow enables it by the `Side Inputs` feature where we can broadcast changes to vertices automatically.
The `Side Inputs` feature achieves this by allowing users to write custom UDFs to broadcast changes to the vertices that are listening in for updates.

### Using Side Inputs in Numaflow

The Side Inputs are updated based on a cron-like schedule,
specified in the pipeline spec with a trigger field.
Multiple side inputs are supported as well.

Below is an example of pipeline spec with side inputs, which runs the custom UDFs every 15 mins and broadcasts the changes if there is any change to be broadcasted.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  sideInputs:
    - name: s3
      container:
        image: my-sideinputs-s3-image:v1
      trigger:
        schedule: "0 */15 * * * *"
        # timezone: America/Los_Angeles
    - name: redis
      container:
        image: my-sideinputs-redis-image:v1
      trigger:
        schedule: "0 */15 * * * *"
        # timezone: America/Los_Angeles
  vertices:
    - name: my-vertex
      sideInputs:
        - s3
    - name: my-vertex-multiple-side-inputs
      sideInputs:
        - s3
        - redis
```

### Implementing User-defined Side Inputs

To use the `Side Inputs` feature, a User-defined function implementing an interface defined in the Numaflow SDK
([Go](https://github.com/numaproj/numaflow-go/blob/main/pkg/sideinput/),
[Python](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/pynumaflow/sideinput/),
[Java](https://github.com/numaproj/numaflow-java/tree/main/src/main/java/io/numaproj/numaflow/sideinput))
is needed to retrieve the data.

You can choose the SDK of your choice to create a
User-defined Side Input image which implements the
Side Inputs Update.

#### Example 

Here is an example of how to write a User-defined Side Input using our SDKs,

=== "Go"

    ```go
    // handle is the side input handler function.
    func handle(_ context.Context) sideinputsdk.Message {
        t := time.Now()
        // val is the side input message value. This would be the value that the side input vertex receives.
        val := "an example: " + string(t.String())
        // randomly drop side input message. Note that the side input message is not retried.
        // NoBroadcastMessage() is used to drop the message and not to
        // broadcast it to other side input vertices.
        counter = (counter + 1) % 10
        if counter%2 == 0 {
            return sideinputsdk.NoBroadcastMessage()
        }
        // BroadcastMessage() is used to broadcast the message with the given value to other side input vertices.
        // val must be converted to []byte.
        return sideinputsdk.BroadcastMessage([]byte(val))
    }
    ```
    [View full example on GitHub](https://github.com/numaproj/numaflow-go/blob/main/examples/sideinput/simple_sideinput/main.go)

=== "Python"

    ```python
    class ExampleSideInput(SideInput):
        def __init__(self):
            self.counter = 0

        def retrieve_handler(self) -> Response:
            """
            Called every time the side input is triggered.
            """
            time_now = datetime.datetime.now()
            val = f"an example: {str(time_now)}"
            self.counter += 1

            # broadcast every other time
            if self.counter % 2 == 0:
                return Response.no_broadcast_message()
            return Response.broadcast_message(val.encode("utf-8"))
    ```
    [View full example on GitHub](https://github.com/numaproj/numaflow-python/blob/main/examples/sideinput/simple_sideinput/example.py)

=== "Java"

    ```java
    public class SimpleSideInput extends SideInputRetriever {
        private final Config config;
        private final ObjectMapper jsonMapper = new ObjectMapper();

        public SimpleSideInput(Config config) {
            this.config = config;
        }

        @Override
        public Message retrieveSideInput() {
            byte[] val;
            if (0.9 > config.getSampling()) {
                config.setSampling(config.getSampling() + 0.01F);
            } else {
                config.setSampling(0.5F);
            }
            try {
                val = jsonMapper.writeValueAsBytes(config);
                log.info("Broadcasting side input message: {}", new String(val));
                return Message.createBroadcastMessage(val);
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize config: {}", e.getMessage());
                return Message.createNoBroadcastMessage();
            }
        }
    }
    ```
    [View full example on GitHub](https://github.com/numaproj/numaflow-java/blob/main/examples/src/main/java/io/numaproj/numaflow/examples/sideinput/simple/SimpleSideInput.java)

=== "Rust"

    ```rust
    struct SideInputHandler {
        counter: Mutex<u32>,
    }

    impl SideInputHandler {
        pub fn new() -> Self {
            SideInputHandler {
                counter: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl SideInputer for SideInputHandler {
        async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let message = format!("an example: {:?}", current_time);

            let mut counter = self.counter.lock().unwrap();
            *counter = (*counter + 1) % 10;
            if *counter % 2 == 0 {
                None
            } else {
                Some(message.into_bytes())
            }
        }
    }
    ```
    [View full example on GitHub](https://github.com/numaproj/numaflow-rs/blob/main/examples/sideinput/src/main.rs)

After performing the retrieval/update, the side input value is then broadcasted to all vertices that use the side input.

```golang
// BroadcastMessage() is used to broadcast the message with the given value.
sideinputsdk.BroadcastMessage([]byte(val))
```

In some cased the user may want to drop the message and not to broadcast the side input value further.

```go
// NoBroadcastMessage() is used to drop the message and not to broadcast it further
sideinputsdk.NoBroadcastMessage()
```

### UDF

Users need to add a watcher on the filesystem to fetch the
updated side inputs in their User-defined Source/Function/Sink
in order to apply the new changes into the data process.

For each side input there will be a file with the given path and after any update to the side input value the file will be updated.

The directory is fixed and can be accessed through a sideinput constant and the file name is the name of the side input.

```go
sideinput.DirPath -> "/var/numaflow/side-inputs"
sideInputFileName -> "/var/numaflow/side-inputs/sideInputName"
```

Here are some examples of watching the side input filesystem for changes in
[Golang](https://github.com/numaproj/numaflow-go/blob/main/examples/sideinput/simple_sideinput/udf/main.go),
[Python](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sideinput/simple_sideinput/udf/example.py) and
[Java](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/sideinput/udf).