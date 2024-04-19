# Fallback Sink

A `Fallback` Sink functions as a `Dead Letter Queue (DLQ)` Sink and can be configured to serve as a backup when the primary sink is down, 
unavailable, or under maintenance. This is particularly useful when there are multiple sinks in a pipeline; if a sink fails, the resulting 
back-pressure will back-propagate and stop the source vertex from reading more data. To prevent this from happening, a `Fallback` Sink can be
set up. This backup sink stores data while the primary sink is offline. Once the primary sink is back online, the stored data can be replayed.


A fallback sink configured will look like below.

Kafka as a fallback sink:
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

Note: The `fallback` field is optional. Additionally, the `fallback` field can only be utilized when the primary sink is a `User Defined Sink.`
Users are required to return a fallback response from the user-defined sink when the primary sink fails, only then the messages will be directed
to the fallback sink. Example of a fallback response in a user-defined sink: [here](https://github.com/numaproj/numaflow-go/blob/main/pkg/sinker/examples/fallback/main.go)