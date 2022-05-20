# Development

This doc explains how to set up a development environment so you can get started.

## Prerequisites

Follow the instructions below to set up your development environment.

### Install requirements

These tools are required for development.

1. [`go`](https://golang.org/doc/install): 1.17+.
1. [`git`](https://help.github.com/articles/set-up-git/): For source control.
1. [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/): For managing development environments.
1. [`protoc`](https://github.com/protocolbuffers/protobuf): For compiling protocol buffers.

### Create a dev cluster

We recommend to use [`k3d`](https://k3d.io/) to create a cluster for development.

After installing `k3d`, use following command to create a kubernetes cluster with default name `k3s-default`.

```shell
k3d cluster create -i rancher/k3s:v1.21.7-k3s1

# kubeconfig
k3d kubeconfig get k3s-default
```

## Useful Commands

- `make build`
  Build binaries into `./dist` directory.

- `make codegen`
  If there's any model changes (`./pkg/api/`), this command need to be run to regenerate code.

- `make test`
  Run unit test cases.

- `make image`
  Build image, and import it to `k3d` cluster if corresponding `kubeconfig` is sourced.

- `make start`
  After you have a `k3d` cluster, run this command to build source code, image, and install the controller in `numaflow-system` namespace.
