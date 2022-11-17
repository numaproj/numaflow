# Kafka Sink

A `Kafka` sink is used to forward the messages to a Kafka topic.

```yaml
spec:
  vertices:
    - name: kafka-output
      sink:
        kafka:
          brokers:
            - my-broker1:19700
            - my-broker2:19700
          topic: my-topic
          tls: # Optional.
            insecureSkipVerify: # Optional, where to skip TLS verification. Default to false.
            caCertSecret: # Optional, a secret reference, which contains the CA Cert.
              name: my-ca-cert
              key: my-ca-cert-key
            certSecret: # Optional, pointing to a secret reference which contains the Cert.
              name: my-cert
              key: my-cert-key
            keySecret: # Optional, pointing to a secret reference which contains the Private Key.
              name: my-pk
              key: my-pk-key
          # Optional, a yaml format string which could apply more configuration for the sink.
          # The configuration hierarchy follows the Struct of sarama.Config at https://github.com/Shopify/sarama/blob/main/config.go.
          config: |
            producer:
            compression: 2
```
