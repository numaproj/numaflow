# Sources

Source vertex is responsible for reliable reading data from an unbounded source into Numaflow.
Source vertex may require [transformation](./transformer/overview.md) or formatting of data prior to sending it to the output buffers.
Source Vertex also does [Watermark](../../core-concepts/watermarks.md) tracking and late data detection.

In Numaflow, we currently support the following sources

* [Kafka](./kafka.md)
* [HTTP](./http.md)
* [Ticker](./generator.md)
* [Nats](./nats.md)
* [Jetstream](./jetstream.md)
* [User-defined Source](./user-defined-sources.md)

A user-defined source is a custom source that a user can write using Numaflow SDK when
the user needs to read data from a system that is not supported by the platform's built-in sources. User-defined source also supports custom acknowledge management for
exactly-once reading.
