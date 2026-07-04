# Prerequisites & Installation

Before deploying your first [MonoVertex](monovertex.md) or [Pipeline](pipeline.md), set up the required tools and install the Numaflow controller.

## Before You Begin: Prerequisites

To get started with Numaflow, ensure you have the following tools and setups ready:

### Container Runtime

You need a container runtime to run container images. Choose one of the following options:

- [Docker Desktop](https://docs.docker.com/get-docker/)
- [Podman](https://podman.io/)

### Local Kubernetes Cluster

Set up a local Kubernetes cluster using one of these tools:

- [Docker Desktop Kubernetes](https://docs.docker.com/desktop/kubernetes/)
- [k3d](https://k3d.io/)
- [kind](https://kind.sigs.k8s.io/)
- [minikube](https://minikube.sigs.k8s.io/docs/start/)

### Kubernetes CLI (`kubectl`)

Install `kubectl` to manage your Kubernetes cluster. Follow the [official guide](https://kubernetes.io/docs/tasks/tools/#kubectl) for installation instructions. If you're unfamiliar with `kubectl`, refer to the [kubectl Quick Reference Page](https://kubernetes.io/docs/reference/kubectl/quick-reference/) for a list of commonly used commands.

Once these prerequisites are in place, you're ready to install Numaflow.

## Installing Numaflow

Numaflow runs as a controller in your cluster. Installing it is a two-step process: create a namespace and apply the install manifest.

### Create a namespace for Numaflow

```shell
kubectl create ns numaflow-system
```

### Install Numaflow components

```shell
kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/main/config/install.yaml
```

That's all you need to deploy a MonoVertex. The [Inter-Step Buffer Service](../core-concepts/inter-step-buffer-service.md) is only required for [Pipelines](pipeline.md), so we install it later, on the Pipeline page.

## Next Step

With Numaflow installed, continue to the [MonoVertex](monovertex.md) guide to deploy the simplest possible workload.
