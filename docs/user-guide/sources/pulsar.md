# Pulsar Source

#### NOTE: 1.5 Feature, not available Numaflow version < 1.5

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

We have only tested the 4.0.x LTS version of Pulsar. The implementation supports [JWT token](https://pulsar.apache.org/docs/4.0.x/security-jwt/) and [HTTP basic](https://pulsar.apache.org/docs/4.0.x/security-basic-auth/) authentication via the `auth` field (`auth.token` or `auth.basicAuth`). If `auth` is not specified, Numaflow will connect to the Pulsar servers without authentication.

More authentication mechanisms and the ability to customize Pulsar consumer will be added in the future.

## TLS

If the Pulsar broker's certificate is signed by a custom/internal CA (not in
the pod's default trust store), point `tls.caCertSecret` at a Secret
containing the CA certificate:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pulsar-ca
type: Opaque
data:
  ca.crt: <base64-encoded PEM CA certificate>

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
          serverAddr: "pulsar+ssl://broker.example.com:6651"
          consumerName: my_consumer
          topic: my_topic
          subscriptionName: my_subscription
          tls: # Optional.
            insecureSkipVerify: false # Optional, whether to skip TLS verification. Default to false.
            caCertSecret: # Optional, a secret reference which contains the CA certificate.
              name: pulsar-ca
              key: ca.crt
```

Only server-authentication (one-way TLS) is supported: `caCertSecret` lets the
client trust a custom CA. `certSecret`/`keySecret` (mutual TLS) are **not**
supported for Pulsar, unlike the [Kafka source](../sources/kafka.md)'s `tls`
block - the underlying Pulsar client does not currently support presenting a
client certificate to the broker.

