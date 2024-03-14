# Development

This doc explains how to set up a development environment for Numaflow.

### Install required tools

1. [`go`](https://golang.org/doc/install) 1.20+.
1. [`git`](https://help.github.com/articles/set-up-git/).
1. [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/).
1. [`protoc`](https://github.com/protocolbuffers/protobuf) 3.19 for compiling protocol buffers.
1. [`pandoc`](https://pandoc.org/installing.html) 2.17 for generating API markdown.
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

- `make image`
  Build container image, and import it to `k3d`, `kind`, or `minikube` cluster if corresponding `KUBECONFIG` is sourced.

- `make docs`
  Convert the docs to GitHub pages, check if there's any error.

- `make docs-serve`
  Start [an HTTP server](http://127.0.0.1:8000/) on your local to host the docs generated Github pages.

### Running end-to-end tests

The following end-to-end tests are available:
```bash
make test-e2e
make test-kafka-e2e
make test-http-e2e
make test-nats-e2e
make test-sdks-e2e
make test-reduce-one-e2e
make test-reduce-two-e2e
make test-api-e2e
make test-udsource-e2e
make test-transformer-e2e
make test-diamond-e2e
make test-sideinputs-e2e
```

In order to run them, you will need to configure your registry in this section near the beginning of the Makefile:
```Makefile
DOCKER_PUSH?=true
DOCKER_BUILD_ARGS?=
IMAGE_NAMESPACE?=my.registry.com/repository
VERSION?=latest
BASE_VERSION:=latest
```

To make it possible for the kubelet to authenticate to your image registry, you will need to create
a secret and to set it as the default one for the default service account of the `numaflow-system` namespace.
This can be done the following way (as per [the Kubernetes official doc](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#add-imagepullsecrets-to-a-service-account)):
```bash
kubectl -n numaflow-system create secret docker-registry numaflow-regcreds --docker-server=my.registry.com/repository \
        --docker-username=DUMMY_USERNAME --docker-password=DUMMY_DOCKER_PASSWORD \
        --docker-email=DUMMY_DOCKER_EMAIL
kubectl -n numaflow-system patch serviceaccount default -p '{"imagePullSecrets": [{"name": "numaflow-regcreds"}]}'
```

While the tests defaults to using *Jetstream* as ISBSVC, you can use a *Resis* one by setting the environnement variable
`ISBSVC` as `redis` (case insensitive).