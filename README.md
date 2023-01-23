# Numaflow

[![Go Report Card](https://goreportcard.com/badge/github.com/numaproj/numaflow)](https://goreportcard.com/report/github.com/numaproj/numaflow)
[![slack](https://img.shields.io/badge/slack-numaproj-brightgreen.svg?logo=slack)](https://join.slack.com/t/numaproj/shared_invite/zt-19svuv47m-YKHhsQ~~KK9mBv1E7pNzfg)
[![GoDoc](https://godoc.org/github.com/numaproj/numaflow?status.svg)](https://godoc.org/github.com/numaproj/numaflow/pkg/apis)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow?label=numaflow)](https://github.com/numaproj/numaflow/releases/latest)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6078/badge)](https://bestpractices.coreinfrastructure.org/projects/6078)

## Summary

Numaflow is a Kubernetes-native tool for running massively parallel stream processing. A Numaflow Pipeline is implemented 
as a Kubernetes custom resource and consists of one or more source, data processing, and sink vertices.

Numaflow installs in a few minutes and is easier and cheaper to use for simple data processing applications than a 
full-featured stream processing platforms.

## Use Cases

- Real-time data analytics applications. 
- Event driven applications such as anomaly detection, monitoring and alerting. 
- Streaming applications such as data instrumentation and data movement. 
- Workflows running in a streaming manner.

## Key Features

- Kubernetes-native: If you know Kubernetes, you already know how to use Numaflow.
- Language agnostic: Use your favorite programming language.
- Exactly-Once semantics: No input element is duplicated or lost even as pods are rescheduled or restarted.
- Auto-scaling with back-pressure: Each vertex automatically scales from zero to whatever is needed.

## Data Integrity Guarantees:

- Minimally provide at-least-once semantics
- Provide exactly-once semantics for unbounded and near real-time data sources
- Preserving order is not required

## Roadmap

- User Defined Transformer at Source for better deserialization and filter for cost reduction (v0.8)
- Multi partitioned edges for higher throughput (v0.9)

## Resources

- [QUICK_START](docs/quick-start.md)
- [EXAMPLES](examples)
- [DEVELOPMENT](docs/development/development.md)
- [CONTRIBUTING](https://github.com/numaproj/numaproj/blob/main/CONTRIBUTING.md)
