# Redis Streams Source

A Redis Streams source is used to ingest messages from [Redis Streams](https://redis.io/docs/data-types/streams-tutorial/).

It is recommended to use this with Redis versions >= 7.0 (in order for autoscaling to work).

## Example:

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
* Define TLS
* Define username/password
* Connect to Redis Sentinel 

# Published message
Incoming messages may have a single Key/Value pair or multiple. In either case, the published message will have Keys equivalent to the incoming Key(s) and Payload equivalent to the JSON serialization of the map of keys to values. 

## Example:
If you have this Incoming message: 
```
XADD * my-stream humidity 44 temperature 65
```

Then Outgoing message will be:
 Keys: `["humidity", "temperature"]`
 Payload: `{"humidity":"44","temperature":"65"}`