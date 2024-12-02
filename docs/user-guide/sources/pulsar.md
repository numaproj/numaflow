# Pulsar Source

A `Pulsar` source is used to ingest the messages from a Pulsar topic.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pulsar
type: Opaque
data:
  token: ZXlKaGJHY2lPaUpJVXpJMU5pSjkuZXlKemRXSWlPaUowWlhOMExYVnpaWElpZlEuZkRTWFFOcEdBWUN4anN1QlZzSDRTM2VLOVlZdHpwejhfdkFZcUxwVHAybwo=

---
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: in
      source:
        pulsar:
          serverAddr: "pulsar+ssl://borker.example.com:6651"
          consumerName: my_consumer
          topic: my_topic
          subscriptionName: my_subscription
          auth: # Optional
            token: # Optional, pointing to a secret reference which contains the JWT Token.
              name: pulsar
              key: token
```

Currently, the implentation only supports JWT token based authentication. If `auth` field is not specified, Numaflow will connect to the Pulsar servers without authentication. 

More authentication mechanisms and ability to customize Pulsar consumer will be added in the future.

