# Sources

Source vertex is responsible for reliable reading data from an unbounded source into Numaflow.

In Numaflow, we currently support the following builtin sources

* [Kafka](./kafka.md)
* [HTTP](./http.md)
* [Ticker](./generator.md)
* [Nats](./nats.md)

Source Vertex also does [Watermark](../../core-concepts/watermarks.md) tracking and late data detection.
