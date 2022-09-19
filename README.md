# Numaflow

[![Go Report Card](https://goreportcard.com/badge/github.com/numaproj/numaflow)](https://goreportcard.com/report/github.com/numaproj/numaflow)
[![GoDoc](https://godoc.org/github.com/numaproj/numaflow?status.svg)](https://godoc.org/github.com/numaproj/numaflow/pkg/apis)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow?label=numaflow)](https://github.com/numaproj/numaflow/releases/latest)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6078/badge)](https://bestpractices.coreinfrastructure.org/projects/6078)

## Summary

Numaflow is a Kubernetes-native tool for running massively parallel data and streaming processing.A Numaflow Pipeline is implemented as a Kubernetes custom resource, and consists of one or more source, data processing, and sink vertices.


Numaflow installs in a few minutes and is easier and cheaper to use for simple data processing applications than a full-featured stream processing platforms.

## Key Features

- Kubernetes-native: If you know Kubernetes, you already know how to use Numaflow.
- Language agnostic: Use your favorite programming language.
- Exactly-Once semantics: No input element is duplicated or lost even as pods are rescheduled or restarted.
- Auto-scaling with back-pressure: Each vertex automatically scales from zero to whatever is needed.

Numaflow does out-of-the-box quality checks after a message/event is read from the source and before it is committed to the sink. It is quintessential that the quality checks are passed for each event to be processed and committed.

## Data Integrity Guarantees:
- Minimally provide at-least-once semantics
- Provide exactly-once semantics for bounded and near-real time data sources 
- Preserving order is not required

## Roadmap

- Data aggregation (e.g. group-by)

## Resources

- [QUICK_START](docs/quick-start.md)
- [EXAMPLES](examples)
- [DEVELOPMENT](docs/development.md)
- [CONTRIBUTING](https://github.com/numaproj/numaproj/blob/main/CONTRIBUTING.md)
