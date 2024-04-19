# Fallback Sink

A `Fallback` Sink functions as a `Dead Letter Queue (DLQ)` Sink and can be configured to serve as a backup when the primary sink is down, 
unavailable, or under maintenance. This is particularly useful when multiple sinks are in a pipeline; if a sink fails, the resulting 
back-pressure will back-propagate and stop the source vertex from reading more data. A `Fallback` Sink can beset up to prevent this from happening. 
This backup sink stores data while the primary sink is offline. The stored data can be replayed once the primary sink is back online.

Note: The `fallback` field is optional. 

Users are required to return a fallback response from the [user-defined sink](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) when the primary sink fails; only
then the messages will be directed to the fallback sink. 

Example of a fallback response in a user-defined sink: [here](https://github.com/numaproj/numaflow-go/blob/main/pkg/sinker/examples/fallback/main.go)

## CAVEATs
The `fallback` field can only be utilized when the primary sink is a `User Defined Sink.`


## Example

### Builtin Kafka
An example using builtin kafka as fallback sink:

```yaml
    - name: out
      sink:
        udsink:
          container:
            image: my-sink:latest
        fallback:
          kafka:
            brokers:
              - my-broker1:19700
              - my-broker2:19700
            topic: my-topic
```
### UD Sink
An example using custom user-defined sink as fallback sink.

User Defined Sink as a fallback sink:
```yaml
    - name: out
      sink:
        udsink:
          container:
            image: my-sink:latest
        fallback:
          udsink:
            container:
              image: my-sink:latest
```
