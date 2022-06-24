# NumaFlow UI

![Numaflow Image](../docs/images/Numa.svg)

A web-based UI for NumaFlow

The UI has the following features:
* View running pipelines in your namespace
* View Vertex and Edge Information of your pipeline
* View BackPressure and Pending Messages
* View Container (main/udf) Logs for a given vertex

# Development

This doc explains how to set up a development environment so you can get started with UI development

## Prerequisites

Follow the instructions below to set up your development environment.

### Install requirements

These tools are required for development.

1. [`NumaFlow`](https://github.com/numaproj/numaflow/blob/master/docs/DEVELOPMENT.md): Follow the document to prepare `Dataflow` development environment.
1. [`node`](https://nodejs.org/en/download/): NodeJS - `brew install node`.
1. [`yarn`](https://yarnpkg.com/): Package manager - `brew install yarn`

### Create a dev cluster

We recommend to use [`k3d`](https://k3d.io/) to create a cluster for development.

After installing `k3d`, use following command to create a kubernetes cluster with default name `k3s-default`.

```shell
k3d cluster create -i rancher/k3s:v1.21.7-k3s1

# kubeconfig
k3d kubeconfig get k3s-default
```

### Run Pipelines in the cluster

Following the [doc](https://github.com/numaproj/numaflow/blob/master/docs/QUICK_START.md) to create Pipelines in the cluster.

### Build and Run the UX Server

#### In the cluster

Run command below to start the ux server in the cluster.

```sh
make start
```

#### On the laptop

It's also possible to start the ux server on your laptop with following command (make sure your terminial is able to access the cluster where runs Dataflow pipelines).

```sh
make run
```

The web application is available at https://localhost:8443/.

## Useful Commands

- `make clean`
  Clean up `./dist` directory.

- `make build`
  Build binaries into `./dist` directory.

- `make test`
  Run unit test cases.

- `make lint`
  Lint the code.

- `make image`
  Build image, and import it to `k3d` cluster if corresponding `kubeconfig` is sourced.

- `make start`
  After you have a `k3d` cluster, run this command to build source code, image, and install ux server in `dataflow-system` namespace.

- `make run`
  Run the ux server on the laptop.

- `make manifests`
  Rebuild deployment manifests.
