# Maximum Message Size

The default maximum message size is `32MB`. Large messages increase the storage and memory usage of the Inter-Step
Buffer Service, so the safest action might be to [enable compression](#enable-compression).

The max message size is determined by:

- Max messages size supported by gRPC (default value is `64MB` in Numaflow).
- Max messages size supported by the Inter-Step Buffer implementation.

If `JetStream` is used as the Inter-Step Buffer implementation, the max message size is limited by the streams created
for the buffers, using `stream.maxMsgSize` in `spec.jetstream.bufferConfig` of the `InterStepBufferService`
specification. It defaults to `33553408` (32MB - 1KB), which is also the max value allowed, since JetStream file storage does
not support messages larger than 32MB. The buffer creation fails if a bigger value is configured.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    bufferConfig: |
      stream:
        maxMsgSize: 8388608 # 8MB
```

The NATS server `max_payload` (configured in `spec.jetstream.settings`) defaults to `68157440` (65MB), so messages
larger than `stream.maxMsgSize` are rejected by the stream instead of the NATS connection being closed.

Writes rejected because a message exceeds the max message size of the stream are counted by the
`isb_jetstream_max_payload_exceeded_total` metric.

Please be aware that if you increase the max message size of the `InterStepBufferService`, you probably will also need to
change some other limits. For example, if the size of each messages is as large as 8MB, then 100 messages flowing in the 
pipeline will make each of the Inter-Step Buffer need at least 800MB of disk space to store the messages, and the memory
consumption will also be high, that will probably cause the Inter-Step Buffer Service to crash. In that case, you might 
need to update the retention policy in the Inter-Step Buffer Service to make sure the messages are not stored for too long.
Check out the [Inter-Step Buffer Service](../../../core-concepts/inter-step-buffer-service.md#buffer-configuration) for more details.

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


