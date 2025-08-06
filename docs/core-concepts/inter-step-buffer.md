# Inter-Step Buffer

A `Pipeline` contains multiple vertices that ingest data from sources, process data, and forward processed data to sinks.
Vertices are not connected directly, but through Inter-Step Buffers.

Inter-Step Buffer can be implemented by a variety of data buffering technologies. Those technologies should support:

- Durability
- Offsets
- Transactions for Exactly-Once forwarding
- Concurrent reading
- Ability to explicitly acknowledge each data or offset
- Claim pending messages (read but not acknowledge)
- Ability to trim data (buffer size control)
- Fast (high throughput low latency)
- Ability to query buffer information

Supported Inter-Step Buffer implementations:

- [Nats JetStream](https://docs.nats.io/nats-concepts/jetstream)
