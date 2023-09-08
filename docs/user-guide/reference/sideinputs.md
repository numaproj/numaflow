# Side Inputs

`Side inputs` in a data processing pipeline are useful for enriching original
data or providing additional context to the pipeline.

This allows accesses to slow updated data or configuration without needing
to retrieve it during each message processing.



### Using Side Inputs in Numaflow
The Side Inputs are updated based on a cron-like schedule, 
specified in the pipeline spec with a trigger field.
Multiple side inputs are supported as well. 
Below is an example of pipeline spec with side inputs.

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
        schedule: "*/15 * * * *"
        # timezone: America/Los_Angeles
    - name: redis
      container:
        image: my-sideinputs-redis-image:v1
      trigger:
        schedule: "*/15 * * * *"
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

### Implementing User Defined Side Inputs

To use Side Inputs feature, a User Defined function implementing an interface defined in the  Numaflow SDK
([Go](https://github.com/numaproj/numaflow-go/blob/main/pkg/sideinput/), 
[Python](https://github.com/numaproj/numaflow-python/blob/main/pynumaflow/sideinput/), 
[Java](https://github.com/numaproj/numaflow-java/tree/main/src/main/java/io/numaproj/numaflow/sideinput)) 
is needed to retrieve the data.

You can choose the SDK of your choice to create a
User Defined Side Input image which implements the
Side Inputs Update.

Here is an example of how to write a User Defined Side Input in Go:

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
Similarly,  this can be written in [Python](https://github.com/numaproj/numaflow-python/blob/main/examples/sideinput/simple-sideinput/example.py) 
and [Java](https://github.com/numaproj/numaflow-java/blob/main/examples/src/main/java/io/numaproj/numaflow/examples/sideinput/simple/SimpleSideInput.java) as well.

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
updated side inputs in their User Defined Source/Function/Sink 
in order to apply the new changes into the data process.

For each side input there will be a file with the given path and after any update to the side input value the file will be updated.

The directory is fixed and can be accessed through a sideinput constant and the file name is the name of the side input.

```go
sideinput.DirPath -> "/var/numaflow/side-inputs"
sideInputFileName -> "/var/numaflow/side-inputs/sideInputName"
```