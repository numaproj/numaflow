# NumaFlow

## Summary

NumaFlow is a Kubernetes-native platform to run massive parallel data processing or streaming jobs.

Each pipeline is specified as a Kubernetes custom resource which consists of one or more source vertices, data processing vertices and sink vertices. Each vertex runs zero or more pods with auto scaling.

NumaFlow targets to achieve `Exactly-Once` semantics, which means from the data from the source vertex to the sink vertex will be processed `Exactly-Once`.

## Core Principles

- Easy to use for an engineer in any language
- Install and up and running in < 1 min (minimal setup)
- Cheaper than Flink, Samza, etc. when TPS is < 10K TPS

## Quick Start

Check [QUICK START](docs/QUICK_START.md) to try it out.

## Development

Refer to [DEVELOPMENT](docs/DEVELOPMENT.md) to set up development environment.

## Contributing

Refer to [CONTRIBUTING](CONTRIBUTING.md) document.
