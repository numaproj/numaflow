# NumaFlow

[![Go Report Card](https://goreportcard.com/badge/github.com/numaproj/numaflow)](https://goreportcard.com/report/github.com/numaproj/numaflow)
[![GoDoc](https://godoc.org/github.com/numaproj/numaflow?status.svg)](https://godoc.org/github.com/numaproj/numaflow/pkg/apis)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Summary

NumaFlow is a Kubernetes-native platform to run massive parallel data processing or streaming jobs.

Each pipeline is specified as a Kubernetes custom resource which consists of one or more source vertices, data processing vertices and sink vertices. Each vertex runs zero or more pods with auto scaling.

NumaFlow targets to achieve `Exactly-Once` semantics, which means from the data from the source vertex to the sink vertex will be processed `Exactly-Once`.

## Core Principles

- Easy to use for an engineer in any language
- Install and up and running in < 1 min (minimal setup)
- Cheaper than Flink, Samza, etc. when TPS is < 10K TPS

## Resources
- Check out [QUICK START](docs/QUICK_START.md) to try it out.
- Take a look at [DEVELOPMENT](docs/DEVELOPMENT.md) to set up development environment.
- Refer to [CONTRIBUTING](CONTRIBUTING.md) to contribute to the project.
