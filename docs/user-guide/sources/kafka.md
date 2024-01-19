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

## FAQ
### How to start the Kafka Source from a specific offset based on datetime?
In order to start the Kafka Source from a specific offset based on datetime, we need to reset the offset before we start the pipeline.

For example, we have a topic `quickstart-events` with 3 partitions and a consumer group `console-consumer-94457`.
```shell
➜  kafka_2.13-3.6.1 bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic quickstart-events            
Topic: quickstart-events	TopicId: WqIN6j7hTQqGZUQWdF7AdA	PartitionCount: 3	ReplicationFactor: 1	Configs: 
	Topic: quickstart-events	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
	Topic: quickstart-events	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
	Topic: quickstart-events	Partition: 2	Leader: 0	Replicas: 0	Isr: 0
```
```shell
➜  kafka_2.13-3.6.1 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list --all-groups                                                                                                     
console-consumer-94457
```
We have already consumed all the available messages in the topic `quickstart-events`, but we want to go back to some datetime and re-consume the data from that datetime.
```shell
➜  kafka_2.13-3.6.1 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group console-consumer-94457                          

GROUP                  TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
console-consumer-94457 quickstart-events 0          56              56              0               -               -               -
console-consumer-94457 quickstart-events 1          38              38              0               -               -               -
console-consumer-94457 quickstart-events 2          4               4               0               -               -               -
```
To achieve that, before the pipeline start, we need to first stop the consumers in the consumer group `console-consumer-94457` because offsets can only be reset if the group `console-consumer-94457` is inactive. Then, reset the offsets using the desired date and time. The example command below uses UTC time.
```shell
➜  kafka_2.13-3.6.1 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --execute --reset-offsets --group console-consumer-94457 --topic quickstart-events --to-datetime 2024-01-19T19:26:00.000

GROUP                          TOPIC                          PARTITION  NEW-OFFSET     
console-consumer-94457         quickstart-events              0          54             
console-consumer-94457         quickstart-events              1          26             
console-consumer-94457         quickstart-events              2          0 
```
Now, we can start the pipeline, and the Kafka source will start consuming the topic `quickstart-events` with consumer group `console-consumer-94457` from the `NEW-OFFSET`.