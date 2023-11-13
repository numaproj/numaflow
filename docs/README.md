# Numaflow

Numaflow is a Kubernetes-native tool for running massively parallel stream processing. A Numaflow Pipeline is implemented
as a Kubernetes custom resource and consists of one or more source, data processing, and sink vertices.

Numaflow installs in a few minutes and is easier and cheaper to use for simple data processing applications than a full-featured
stream processing platforms.

## Use Cases

- Real-time data analytics applications.
- Event driven applications such as anomaly detection, monitoring, and alerting.
- Streaming applications such as data instrumentation and data movement.
- Workflows running in a streaming manner.
- [Learn more in our User Guide](./user-guide/use-cases/overview.md).

## Key Features

- Kubernetes-native: If you know Kubernetes, you already know how to use Numaflow.
- Language agnostic: Use your favorite programming language.
- Exactly-Once semantics: No input element is duplicated or lost even as pods are rescheduled or restarted.
- Auto-scaling with back-pressure: Each vertex automatically scales from zero to whatever is needed.

## Data Integrity Guarantees

- Minimally provide at-least-once semantics
- Provide exactly-once semantics for unbounded and near real-time data sources
- Preserving order is not required

## Roadmap

- Support for watermark progression even if the source is idling (1.1)
- Session Window (1.2)

## Demo

[![Numaflow Demo](https://img.youtube.com/vi/hJS714arX6Q/0.jpg)](https://youtu.be/hJS714arX6Q)

## Getting Started

For set-up information and running your first Numaflow pipeline, please see our [getting started guide](./quick-start.md).
