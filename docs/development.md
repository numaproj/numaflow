# Development

This doc explains how to set up a development environment for Numaflow.

### Install required tools

1. [`go`](https://golang.org/doc/install) 1.19+
1. [`git`](https://help.github.com/articles/set-up-git/)
1. [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
1. [`protoc`](https://github.com/protocolbuffers/protobuf) 3.19 for compiling protocol buffers
1. [`pandoc`](https://pandoc.org/installing.html) 2.17 for generating API markdown
1. [`Node.js®`](https://nodejs.org/en/) for running the UI
1. [`yarn`](https://classic.yarnpkg.com/en/)
1. A local Kubernetes cluster - you need one of the following options as your local Kubernetes cluster for development usage:
   1. [`k3d`](https://k3d.io/)
   2. [`kind`](https://kind.sigs.k8s.io/)
   3. [`minikube`](https://minikube.sigs.k8s.io/docs/start/)

### Example: Create a k8s cluster with k3d

```shell
# Create a cluster with default name k3s-default
k3d cluster create -i rancher/k3s:v1.24.4-k3s1

# Get kubeconfig for the cluster
k3d kubeconfig get k3s-default
```

### Useful Commands

- `make start`
  Build the source code, image, and install the Numaflow controller in the `numaflow-system` namespace.

- `make build`
  Binaries are placed in `./dist`.

- `make codegen`
  Run after making changes to `./pkg/api/`.

- `make test`
  Run unit tests.

- `make image`
  Build container image, and import it to `k3d` or `minikube` cluster if corresponding `kubeconfig` is sourced.

- `make docs`
  Convert the docs to Github pages, check if there's any error.

- `make docs-serve`
  Start [an HTTP server](http://127.0.0.1:8000/) on your local to host the docs generated Github pages.
