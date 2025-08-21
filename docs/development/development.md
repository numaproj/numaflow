# Development

This doc explains how to set up a development environment for Numaflow.

### Install required tools

1. [`go`](https://golang.org/doc/install) 1.24+.
1. [`git`](https://help.github.com/articles/set-up-git/).
1. [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl).
1. [`protoc`](https://github.com/protocolbuffers/protobuf) 3.19 for compiling protocol buffers.
1. [`pandoc`](https://pandoc.org/installing.html) 3.2.1 for generating API markdown.
1. [`Node.js®`](https://nodejs.org/en/) for running the UI.
1. [`yarn`](https://classic.yarnpkg.com/en/).
1. A local Kubernetes cluster for development usage, pick either one of [`k3d`](https://k3d.io/), [`kind`](https://kind.sigs.k8s.io/), or [`minikube`](https://minikube.sigs.k8s.io/docs/start/).

### Example: Create a local Kubernetes cluster with `kind`

```shell
# Install kind on macOS
brew install kind

# Create a cluster with default name kind
kind create cluster

# Get kubeconfig for the cluster
kind export kubeconfig
```

#### Metrics Server

Please install the metrics server if your local Kubernetes cluster does not bring it by default (e.g., Kind).
Without the [metrics-server](https://github.com/kubernetes-sigs/metrics-server), we will not be able to see the pods in
the UI.

```shell
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl patch -n kube-system deployment metrics-server --type=json -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
```

### Useful Commands

- `make start`
  Build the source code, image, and install the Numaflow controller in the `numaflow-system` namespace.

- `make build`
  Binaries are placed in `./dist`.

- `make manifests`
  Regenerate all the manifests after making any base manifest changes. This is also covered by `make codegen`.

- `make codegen`
  Run after making changes to `./pkg/api/`.

- `make test`
  Run unit tests.

- `make test-*`
  Run one e2e test suite. e.g. `make test-kafka-e2e` to run the kafka e2e suite.

- `make Test*`
  Run one e2e test case. e.g. `make TestKafkaSourceSink` to run the `TestKafkaSourceSink` case in the kafka e2e suite.

- `make image`
  Build container image, and import it to `k3d`, `kind`, or `minikube` cluster if corresponding `KUBECONFIG` is sourced.

- `make docs`
  Convert the docs to GitHub pages, check if there's any error.

- `make docs-serve`
  Start [an HTTP server](http://127.0.0.1:8000/) on your local to host the docs generated Github pages.
