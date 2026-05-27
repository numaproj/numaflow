# Kafka Source

Two methods are available for integrating Kafka topics into your Numaflow pipeline:
using a user-defined Kafka Source or opting for the built-in Kafka Source provided by Numaflow.

## Option 1: User-Defined Kafka Source

Developed and maintained by the Numaflow contributor community,
the [Kafka Source](https://github.com/numaproj-contrib/kafka-java) offers a robust and feature-complete solution
for integrating Kafka as a data source into your Numaflow pipeline.

Key Features:

* **Flexibility:** Allows full customization of Kafka Source configurations to suit specific needs.
* **Kafka Java Client Utilization:** Leverages the Kafka Java client for robust message consumption from Kafka topics.
* **Schema Management:** Integrates seamlessly with the Confluent Schema Registry to support schema validation and manage schema evolution effectively.

More details on how to use the Kafka Source can be found [here](https://github.com/numaproj-contrib/kafka-java/blob/main/README.md#read-data-from-kafka).

## Option 2: Built-in Kafka Source

Numaflow provides a built-in `Kafka` source to ingest messages from a Kafka topic. The source uses consumer-groups to manage offsets.

```yaml
spec:
  vertices:
    - name: input
      source:
        kafka:
          brokers:
            - my-broker1:19700
            - my-broker2:19700
          topic: my-topic # Use comma-separated topic names to consume from multiple topics.
          consumerGroup: my-consumer-group
          # Optional, flat librdkafka client configuration. Use one `key: value`
          # pair per line.
          config: |
            auto.offset.reset: earliest
            session.timeout.ms: 45000
          tls: # Optional.
            insecureSkipVerify: false # Optional, whether to skip TLS verification. Default to false.
            caCertSecret: # Optional, a secret reference which contains the CA certificate.
              name: my-ca-cert
              key: my-ca-cert-key
            certSecret: # Optional, a secret reference which contains the client certificate.
              name: my-cert
              key: my-cert-key
            keySecret: # Required when certSecret is set; contains the client private key.
              name: my-pk
              key: my-pk-key
          sasl: # Optional. This example uses PLAIN; other mechanisms are listed below.
            mechanism: PLAIN
            plain:
              userSecret:
                name: plain-user
                key: plain-user-key
              passwordSecret:
                name: plain-password
                key: plain-password-key
              handshake: true # Required by the API; ignored by the Rust built-in source.
```

The `config` field accepts any supported [librdkafka client configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html) option as a flat `key: value` pair. By default,
the built-in source sets `auto.offset.reset: earliest`, `session.timeout.ms: 45000`, `heartbeat.interval.ms: 15000`,
`enable.partition.eof: false`, and `enable.auto.commit: false`.

The built-in Kafka source supports multiple SASL auth mechanisms. To enable SASL, configure one mechanism and include
only the matching mechanism-specific block.

### PLAIN

```yaml
sasl:
  mechanism: PLAIN
  plain:
    userSecret:
      name: plain-user
      key: plain-user-key
    passwordSecret:
      name: plain-password
      key: plain-password-key
    handshake: true # Required by the API; ignored by the Rust built-in source.
```

### SCRAM-SHA-256

```yaml
sasl:
  mechanism: SCRAM-SHA-256
  scramsha256:
    userSecret:
      name: scram-sha-256-user
      key: scram-sha-256-user-key
    passwordSecret:
      name: scram-sha-256-password
      key: scram-sha-256-password-key
    handshake: true # Required by the API; ignored by the Rust built-in source.
```

### SCRAM-SHA-512

```yaml
sasl:
  mechanism: SCRAM-SHA-512
  scramsha512:
    userSecret:
      name: scram-sha-512-user
      key: scram-sha-512-user-key
    passwordSecret:
      name: scram-sha-512-password
      key: scram-sha-512-password-key
    handshake: true # Required by the API; ignored by the Rust built-in source.
```

### GSSAPI

```yaml
sasl:
  mechanism: GSSAPI
  gssapi:
    serviceName: my-service
    realm: my-realm
    authType: KRB5_KEYTAB_AUTH
    usernameSecret:
      name: gssapi-username
      key: gssapi-username-key
    keytabSecret:
      name: gssapi-keytab
      key: gssapi-keytab-key
    kerberosConfigSecret: # Optional, contains the Kerberos kinit command.
      name: my-kerberos-config
      key: my-kerberos-config-key
```

### OAUTHBEARER

```yaml
sasl:
  mechanism: OAUTHBEARER # OAUTH is also accepted.
  oauth:
    clientID:
      name: kafka-oauth-client
      key: clientid
    clientSecret:
      name: kafka-oauth-client
      key: clientsecret
    tokenEndpoint: https://oauth-token.com/v1/token
```

## FAQ
### How to start the Kafka Source from a specific offset based on datetime?
In order to start the Kafka Source from a specific offset based on datetime, we need to reset the offset before we start the pipeline.

For example, we have a topic `quickstart-events` with 3 partitions and a consumer group `console-consumer-94457`. This example uses `Kafka 3.6.1` and localhost.
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

You may need to create a property file which contains the connectivity details and use it to connect to the clusters. Below are two example `config.properties` files: `SASL/PLAIN` and `TSL`.
```text
ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
request.timeout.ms=20000
bootstrap.servers=<BOOSTRAP_BROKER_LIST>
retry.backoff.ms=500
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
   username="<CLUSTER_API_KEY>" \
   password="<CLUSTER_API_SECRET>";
security.protocol=SASL_SSL
```
```text
request.timeout.ms=20000
bootstrap.servers=<BOOSTRAP_BROKER_LIST>
security.protocol=SSL
ssl.enabled.protocols=TLSv1.2
ssl.truststore.location=<JKS_FILE_PATH>
ssl.truststore.password=<PASSWORD>
```
Run the command with the `--command-config` option.
```shell
bin/kafka-consumer-groups.sh --bootstrap-server <BOOTSTRAP_BROKER_LIST> --command-config config.properties --execute --reset-offsets --group <GROUP_NAME> --topic <TOPIC_NAME> --to-datetime <DATETIME_STRING>
```

Reference:
- [How to Use Kafka Tools With Confluent Cloud](https://docs.confluent.io/kafka/operations-tools/use-kafka-tools-ccloud.html#create-a-configuration-file)
- [Apache Kafka Security](https://kafka.apache.org/41/security/)