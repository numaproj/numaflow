# Redis Streams Source

A Redis Streams source is used to ingest messages from [Redis Streams](https://redis.io/docs/data-types/streams-tutorial/).

Example:

```yaml
spec:
  vertices:
    - name: input
      source:
        redisStreams:
          url: redis:6379  # One URL, or multiple URLs separated by comma
          stream: test-stream
          consumerGroup: my-group
          readFromBeginning: true # Should we start from beginning of Stream or latest?

```

Please see [API](https://github.com/numaproj/numaflow/blob/main/docs/APIs.md#redisstreamssource) for details on how to optionally do the following:
- Define TLS
- Define username/password
- Connect to Redis Sentinel 