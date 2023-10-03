# Maximum Message Size

The default maximum message size is `1MB`. There's a way to increase this limit in case you want to, but please think it through before doing so.

The max message size is determined by:

- Max messages size supported by gRPC (default value is `64MB` in Numaflow).
- Max messages size supported by the Inter-Step Buffer implementation.

If `JetStream` is used as the Inter-Step Buffer implementation, the default max message size for it is configured as `1MB`. You can change it by setting the `spec.jetstream.settings` in the `InterStepBufferService` specification.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    settings: |
      max_payload: 8388608 # 8MB
```

It's not recommended to use values over `8388608` (8MB) but `max_payload` can be set up to `67108864` (64MB).

Please be aware that if you increase the max message size of the `InterStepBufferService`, you probably will also need to change some other limits. For example, if the size of each messages is as large as 8MB, then 100 messages flowing in the pipeline will make each of the Inter-Step Buffer need at least 800MB of disk space to store the messages, and the memory consumption will also be high, that will probably cause the Inter-Step Buffer Service to crash. In that case, you might need to update the retention policy in the Inter-Step Buffer Service to make sure the messages are not stored for too long. Check out the [Inter-Step Buffer Service](../../../core-concepts/inter-step-buffer-service.md#buffer-configuration) for more details.
