# Numaflow

[![Go Report Card](https://goreportcard.com/badge/github.com/numaproj/numaflow)](https://goreportcard.com/report/github.com/numaproj/numaflow)
[![GoDoc](https://godoc.org/github.com/numaproj/numaflow?status.svg)](https://godoc.org/github.com/numaproj/numaflow/pkg/apis)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow?label=numaflow)](https://github.com/numaproj/numaflow/releases/latest)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6078/badge)](https://bestpractices.coreinfrastructure.org/projects/6078)

## Summary

Numaflow is a Kubernetes-native platform for running massively parallel data processing and streaming jobs.

A Numaflow Pipeline is implemented as a Kubernetes custom resource, and consists of one or more sources, data processing and sink vertices.

Numaflow installs in a few minutes and is easier and cheaper to use for simple data processing applications than a full-featured stream processing platforms.

## Key Features

- Kubernetes-native: If you know Kubernetes, you already know how to use Numaflow.
- Language agnostic: Use your favorite programming language.
- Exactly-Once semantics: No input element is duplicated or lost even as pods are rescheduled or restarted.
- Auto-scaling with back-pressure: Each vertex automatically scales from zero to whatever is needed.

## Roadmap

- Data aggregation (e.g. group-by)

## Resources

- [QUICK_START](docs/quick-start.md)
- [EXAMPLES](examples)
- [DEVELOPMENT](docs/development.md)
- [CONTRIBUTING](https://github.com/numaproj/numaproj/blob/main/CONTRIBUTING.md)
