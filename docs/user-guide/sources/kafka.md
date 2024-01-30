# Kafka Source

A `Kafka` source is used to ingest the messages from a Kafka topic. Numaflow uses consumer-groups to manage offsets.

```yaml
spec:
  vertices:
    - name: input
      source:
        kafka:
          brokers:
            - my-broker1:19700
            - my-broker2:19700
          topic: my-topic
          consumerGroup: my-consumer-group
          config: | # Optional.
            consumer:
              offsets:
                initial: -2 # -2 for sarama.OffsetOldest, -1 for sarama.OffsetNewest. Default to sarama.OffsetNewest.
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
          sasl: # Optional
            mechanism: GSSAPI # PLAIN or GSSAPI, other mechanisms not supported
            gssapi: # Optional, for GSSAPI mechanism
              serviceName: my-service
              realm: my-realm
              # KRB5_USER_AUTH for auth using password
              # KRB5_KEYTAB_AUTH for auth using keytab
              authType: KRB5_KEYTAB_AUTH 
              usernameSecret: # Pointing to a secret reference which contains the username
                name: gssapi-username
                key: gssapi-username-key
              # Pointing to a secret reference which contains the keytab (authType: KRB5_KEYTAB_AUTH)
              keytabSecret: 
                name: gssapi-keytab
                key: gssapi-keytab-key
              # Pointing to a secret reference which contains the keytab (authType: KRB5_USER_AUTH)
              passwordSecret: 
                name: gssapi-password
                key: gssapi-password-key
              kerberosConfigSecret: # Pointing to a secret reference which contains the kerberos config
                name: my-kerberos-config
                key: my-kerberos-config-key
            plain: # Optional, for PLAIN mechanism
              userSecret: # Pointing to a secret reference which contains the user
                name: plain-user
                key: plain-user-key
              passwordSecret: # Pointing to a secret reference which contains the password
                name: plain-password
                key: plain-password-key
              # Send the Kafka SASL handshake first if enabled (defaults to true)
              # Set this to false if using a non-Kafka SASL proxy
              handshake: true 
```
