# Numaflow

Numaflow is a Kubernetes-native platform for running massive parallel data processing and streaming jobs. A Numaflow Pipeline is implemented as a Kubernetes custom resource, and consists of one or more source, data processing, and sink vertices.

## Key Features

- Kubernetes-native: If you know Kubernetes, you already know 90% of what you need to use Numaflow.
- Language agnostic: Use your favorite programming language.
- Exactly-Once semantics: No input element is duplicated or lost even as pods are rescheduled or restarted.
- Auto-scaling with back-pressure: Each vertex automatically scales from zero to whatever is needed.

## Roadmap

- Data aggregation (e.g. group-by)

## Getting Started

For set-up information and running your first Numaflow pipeline, please see our [getting started guide](./quick-start.md).
