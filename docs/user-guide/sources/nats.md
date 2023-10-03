# Nats Source

A `Nats` source is used to ingest the messages from a nats subject.

```yaml
spec:
  vertices:
    - name: input
      source:
        nats:
          url: nats://demo.nats.io # Multiple urls separated by comma.
          subject: my-subject
          queue: my-queue # Queue subscription, see https://docs.nats.io/using-nats/developer/receiving/queues
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
          auth: # Optional.
            basic: # Optional, pointing to the secret references which contain user name and password.
              user:
                name: my-secret
                key: my-user
              password:
                name: my-secret
                key: my-password
```

## Auth

The `auth` strategies supported in `nats` source include `basic` (user and password), `token` and `nkey`, check the [API](https://github.com/numaproj/numaflow/blob/main/docs/APIs.md#numaflow.numaproj.io/v1alpha1.NatsAuth) for the details.
