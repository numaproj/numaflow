# Development

This doc explains how to set up a development environment for Numaflow.

### Install required tools

1. [`go`](https://golang.org/doc/install) 1.18+
1. [`git`](https://help.github.com/articles/set-up-git/)
1. [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
1. [`protoc`](https://github.com/protocolbuffers/protobuf) for compiling protocol buffers
1. [`Node.js®`](https://nodejs.org/en/) for running the UI
1. [`yarn`](https://classic.yarnpkg.com/en/)
1. [`k3d`](https://k3d.io/) for local development, if needed

### Create a k8s cluster with k3d if needed

```shell
# Create a cluster with default name k3s-default
k3d cluster create -i rancher/k3s:v1.21.7-k3s1

# Get kubeconfig for the cluster
k3d kubeconfig get k3s-default
```

### Useful Commands

- `make build`
  Binaries are placed in `./dist`.

- `make codegen`
  Run after making changes to `./pkg/api/`.

- `make test`
  Run unit tests.

- `make image`
  Build container image, and import it to `k3d` cluster if corresponding `kubeconfig` is sourced.

- `make start`
  Build the source code, image, and install the Numa controller in the `numaflow-system` namespace.
