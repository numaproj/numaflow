# Maximum Message Size

The default maximum message size is `1MB`. There's a way to increase this limit in case you want to, but please think it
through before doing so. The safest action might be to [enable compression](#enable-compression).

The max message size is determined by:

- Max messages size supported by gRPC (default value is `64MB` in Numaflow).
- Max messages size supported by the Inter-Step Buffer implementation.

If `JetStream` is used as the Inter-Step Buffer implementation, the default max message size for it is configured as `1MB`.
You can change it by setting the `spec.jetstream.settings` in the `InterStepBufferService` specification.

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

Please be aware that if you increase the max message size of the `InterStepBufferService`, you probably will also need to
change some other limits. For example, if the size of each messages is as large as 8MB, then 100 messages flowing in the 
pipeline will make each of the Inter-Step Buffer need at least 800MB of disk space to store the messages, and the memory
consumption will also be high, that will probably cause the Inter-Step Buffer Service to crash. In that case, you might 
need to update the retention policy in the Inter-Step Buffer Service to make sure the messages are not stored for too long.
Check out the [Inter-Step Buffer Service](../../../core-concepts/inter-step-buffer-service.md#buffer-configuration) for more details.

## Identify Oversized Messages

Pipeline vertex pods expose `isb_jetstream_publish_size_bytes`, a histogram of the NATS publish
body size after ISB compression and serialization. Unlike `forwarder_write_bytes_total`, it
includes the protobuf envelope and NATS headers and also observes attempts that exceed
`max_payload`.

For example, the following query returns the p99 publish size by pipeline and vertex. The
`namespace` and `pod` labels are available when they are added by the Prometheus Kubernetes scrape
configuration.

```promql
histogram_quantile(
  0.99,
  sum by (namespace, pipeline, vertex, pod, le) (
    rate(isb_jetstream_publish_size_bytes_bucket[5m])
  )
)
```

Use `isb_jetstream_message_too_large_total` to find exact limit violations:

```promql
sum by (namespace, pipeline, vertex, pod, partition_name) (
  increase(isb_jetstream_message_too_large_total[5m])
)
```

When a publish exceeds the server-advertised limit, the vertex logs the message ID, stream,
measured size, configured maximum, and compression type. This instrumentation does not change
publishing, retry, or acknowledgment behavior.

For older versions, the client IP in the NATS `maximum payload exceeded` log can be matched to a
vertex pod:

```shell
kubectl get pods -A -o wide --field-selector status.podIP=<client-ip>
```

## Enable Compression

Numaflow supports automatic compression while writing and reading the messages to and from the Inter-Step Buffer, this can help to 
reduce the storage and network cost to ISB. Enabling compression will help in ISB stability and should be used if the 
payload is large (e.g, > 1MB). This is transparent to the user-defined functions, compression and decompression is 
taken care by Numaflow before writing to the ISB and after reading from the ISB.

Available compression types are:
- `none` (default)
- `gzip`
- `zstd`
- `lz4`

### Performance Numbers

The tests were run with fixed CPU `300m` CPU using random `1KB` payload.

| Compression | Throughput (msg/s) | Disk Usage by ISB (GB) | 
|-------------|--------------------|------------------------|
| None        | 1000               | 7 ~ 7.2                |
| GZIP        | 132                | 1.2 ~ 1.4              |
| ZSTD        | 900                | 4.5 ~ 4.7              |
| LZ4         | 1000               | 2.8 ~ 3                |

Clearly the best compression (least disk usage) is `gzip`, but it has the lowest throughput. `lz4` has the best
throughput and `zstd` is in the middle. If you want to use `gzip`, you might need to increase the CPU of `numa` container
to get better performance.

### Configuration

You can enable it by setting the `compression` field in the `Pipeline` specification.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  interStepBuffer:
    compression:
      type: COMPRESSION_TYPE
``` 


