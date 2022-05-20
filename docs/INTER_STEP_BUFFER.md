# Inter-Step Buffer

A pipeline contains multiple vertices to ingest data from sources, processing data, and forward processed data to sinks. Vertices are not connected directly, but through Inter-Step Buffers.

Inter-Step Buffer can be implemented by a variety of data buffering technologies, those technologies should support:

- Durability
- Offsets
- Transactions for Exactly-Once forwarding
- Concurrent reading
- Ability to explicitly acknowledge each data or offset
- Claim pending messages (read but not acknowledge)
- Ability to trim data (buffer size control)
- Fast (high throughput low latency)
- Ability to query buffer information

Currently, there are 2 Inter-Step Buffer implementations:

- [Nats JetStream](https://docs.nats.io/nats-concepts/jetstream)
- [Redis Stream](https://redis.io/topics/streams-intro)
