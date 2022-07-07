# Quick Start

To see how NumaFlow works, you can install it, and run a simple pipeline, which contains a source vertex to generate messages, and a function vertex that echos the messages, and a log sink printing the messages.

## Prerequisites

A Kubernetes cluster is needed to try out NumaFlow. If needed, you can create a cluster locally using
[`k3d`](https://k3d.io/) as below. You also need to get [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed.

```shell
# Create a cluster with default name k3s-default
k3d cluster create -i rancher/k3s:v1.21.7-k3s1

# Get kubeconfig for the cluster
k3d kubeconfig get k3s-default
```

## NumaFlow Installation

```sh
kubectl create ns numaflow-system
kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/stable/config/install.yaml

```

Open a port-forward so you can access the UI on [https://localhost:8443](https://localhost:8443).

```sh
kubectl -n numaflow-system port-forward deployment/numaflow-server 8443:8443
```

## A Simple Pipeline

[Inter-Step Buffers](./INTER_STEP_BUFFER.md) are essential to run NumaFlow pipelines, to get Inter-Step Buffers in place, an [Inter-Step Buffer Service](./INTER_STEP_BUFFER_SERVICE.md) is required. We use [Nats JetStream](https://docs.nats.io/nats-concepts/jetstream) as an `Inter-Step Buffer Service` provider in this example.

```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```

After all the isbsvc pods are up, create a simple pipeline, and you can see the pipeline topology in the UI.

```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
```

Watch the `output` vertex pod log, you will see messages keep coming.

```
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"5dkN+42qwRY=","Createdts":1639779266118670821}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"L+QN+42qwRY=","Createdts":1639779266118673455}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"d+kN+42qwRY=","Createdts":1639779266118674807}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"lu4N+42qwRY=","Createdts":1639779266118676118}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"RoKqNo6qwRY=","Createdts":1639779267118793286}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"FUqrNo6qwRY=","Createdts":1639779267118844437}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"oFKrNo6qwRY=","Createdts":1639779267118846624}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"eVarNo6qwRY=","Createdts":1639779267118847609}
2021/12/17 22:14:35 (simple-pipeline-output) {"Data":"6FmrNo6qwRY=","Createdts":1639779267118848488}
```

You can delete the pipeline by

```shell
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
```

The `Inter-Step Buffer Service` can be deleted by

```shell
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
```

## What's Next

After exploring how a NumaFlow pipeline runs, you can check what data [Sources](./sources/GENERATOR.md) and [Sinks](./sinks/KAFKA.md) NumaFlow supports out of the box, or learn how to write [User Defined Functions](./USER_DEFINED_FUNCTIONS.md).
