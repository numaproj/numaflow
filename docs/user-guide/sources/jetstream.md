# Jetstream Source

A `Jetstream` source is used to ingest the messages from a [Jetstream stream](https://docs.nats.io/nats-concepts/jetstream).

```yaml
spec:
  vertices:
    - name: input
      source:
        jetstream:
          url: nats://demo.nats.io # Jetstream server url
          stream: my-stream # stream name
          consumer: my-consumer # Optional, Name of the consumer on the stream.
          deliver_policy: last_per_subject # Optional, The point in the stream from which to receive messages. Defaults to "all"
          filter_subjects: # Optional, A set of subjects that overlap with the subjects bound to the stream to filter delivery to subscribers
            - "abc.A.*"
            - "abc.B.*"
          tls: # Optional.
            insecureSkipVerify: # Optional, whether to skip TLS verification. Default to false.
            caCertSecret: # Optional, a secret reference, which contains the CA Cert.
              name: my-ca-cert
              key: my-ca-cert-key
            certSecret: # Optional, pointing to a secret reference which contains the Cert.
              name: my-cert
              key: my-cert-key
            keySecret: # Optional, pointing to a secret reference which contains the Private Key.
              name: my-pk
              key: my-pk-key
          auth: # Optional.
            basic: # Optional, pointing to the secret references which contain user name and password.
              user:
                name: my-secret
                key: my-user
              password:
                name: my-secret
                key: my-password
```

The `consumer` field represents the name of the consumer of the stream. If not specified, a consumer with name format `numaflow-<pipeline_name>-<vertex_name>-<stream_name>` will used. Numaflow will attempt to create this consumer on the stream if it doesn't exist.

The [valid values](https://docs.nats.io/nats-concepts/jetstream/consumers#deliverpolicy) for `deliver_policy` are:

- `all`
- `new`
- `last`
- `last_per_subject`
- `by_start_sequence <sequence_id>` eg. `by_start_sequence 42`
- `by_start_time <unix_epoch_time_milliseconds>` eg. `by_start_time 1753428483000`

## Auth

The `auth` strategies supported in `Jetstream` source include `basic` (user and password), `token` and `nkey`, check the [API](https://github.com/numaproj/numaflow/blob/main/docs/APIs.md#numaflow.numaproj.io/v1alpha1.NatsAuth) for the details.
