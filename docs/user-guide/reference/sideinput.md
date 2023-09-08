# Side Inputs in Numaflow

Numaflow now supports `Side Inputs`, which allow for passing extra information into a pipeline as a supplementary input for data processing. This feature offers increased flexibility as the pipeline can now consider information beyond just the primary data stream.

## What are Side Inputs?

`Side inputs` in a data processing pipeline are useful for enriching original
data or providing additional context to the pipeline.

With a centralized strategy that allows for continuous updates to one
or more configurations, Numaflow broadcasts these changes to all
vertices requiring the input(s) for data processing.
It's particularly useful for users needing extra information during processing,
ensuring this additional data remains most-up-to-date
and synchronized across all vertices.

This allows accesses to slow updated data or configuration without needing 
to retrieve it during each message processing.


### How to Use Side Inputs in My Pipeline
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
The Side Input Manager in Numaflow will automatically run the
image based on the schedule defined in the pipeline spec and
update the file in a centralized data storage.

### Implementing User Defined Side Inputs

The Side Inputs feature is provided in all three SDKs:
- numaflow-go
- numaflow-python,
- numaflow-java.

You can choose the SDK of your choice to create a
User Defined Side Input image which implements the
Side Inputs Update.

Here is an example of how to write a User Defined Side Input:

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

After performing the retrieval/update for the side input value the user can choose to either broadcast the message to other side input vertices or drop the message. The side input message is not retried.

For each side input there will be a file with the given path and after any update to the side input value the file will be updated.

The directory is fixed and can be accessed through sideinput constants sideinput.DirPath. The file name is the name of the side input.

```go
sideinput.DirPath -> "/var/numaflow/side-inputs"
sideInputFileName -> "/var/numaflow/side-inputs/sideInputName"
```


### UDF
Users need to add a watcher on the filesystem to fetch the 
updated side inputs in their User Defined Source/Function/Sink 
in order to apply the new changes into the data process.