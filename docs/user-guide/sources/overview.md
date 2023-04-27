# Sources

In an unbounded stream data processing platform such as Numaflow, a source is a component that continuously 
generates or provides input data to the platform. This input data is typically in the form of an unbounded stream 
of events, messages, or other data types that need to be processed in real-time.
A source component in an unbounded stream processing platform continuously receives input data from an external system,
such as a messaging system, HTTP source. The source component then sends this data to the processing platform for 
further analysis, in a way that ensures that the data is processed as it arrives and is not buffered or batched.

In addition to handling the continuous flow of data, sources in an unbounded stream processing platform also need to
be designed with fault tolerance and scalability in mind. For example, the source component may need to buffer data 
locally in case of temporary network outages, and it may need to be able to scale horizontally or vertically to handle
increased traffic volume.

In summary, a source in an unbounded stream data processing platform is a critical component that provides a 
continuous flow of input data to the platform in real-time, and its design and implementation can have a significant 
impact on the overall performance, reliability, and scalability of the platform.

In Numaflow we currently support the following as in built sources

* Kafka
* HTTP
* Redis Stream
* Tick generator

As an example, suppose we have a data pipeline that analyzes user behavior on a website, with the goal of 
identifying potential fraud or abuse. To do this, we need to continuously process a stream of user events, 
such as pageviews, clicks, and sign-ins.

We can use Kafka as a source to ingest the user events data, by creating a Kafka topic for each event type and 
configuring producers to send data to those topics. The producers can be configured to send data in real-time as the 
events occur, ensuring that the data is processed as soon as possible.
