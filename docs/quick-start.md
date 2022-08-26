# Quick Start

To see how Numaflow works, you can install it, and run a simple pipeline, which contains a source vertex to generate messages, and a function vertex that echos the messages, and a log sink printing the messages.

## Prerequisites

A Kubernetes cluster is needed to try out Numaflow. You can create a cluster locally using
[`k3d`](https://k3d.io/) as below. As the requirements for [`k3d`](https://k3d.io/), you also need to get [`docker`](https://docs.docker.com/get-docker/) and [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed.

```shell
# Create a cluster with default name k3s-default
k3d cluster create -i rancher/k3s:v1.21.7-k3s1

# Get kubeconfig for the cluster
k3d kubeconfig get k3s-default
```

## Installation

Run following command lines to install Numaflow.

```shell
kubectl create ns numaflow-system
```
```shell
kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/stable/config/install.yaml
```

Open a port-forward so that you can access the UI on [https://localhost:8443](https://localhost:8443).

```sh
kubectl -n numaflow-system port-forward deployment/numaflow-server 8443:8443
```

## A Simple Pipeline

[Inter-Step Buffers](./inter-step-buffer.md) are essential to run Numaflow pipelines. To get Inter-Step Buffers in place, an [Inter-Step Buffer Service](./inter-step-buffer-service.md) is required. We use [Nats JetStream](https://docs.nats.io/nats-concepts/jetstream) as an `Inter-Step Buffer Service` provider in this example.

```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```
You can run the following command to check if the `Inter-Step Buffer Service` is up and running. The `PHASE` for the `default` `Inter-Step Buffer Service` should be `Running`.
```shell
kubectl get isbsvc
```

After the`Inter-Step Buffer Service` is up and running, create a `simple pipeline`, and you can see the pipeline topology in the UI.
```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
```
You can run the following command to check if the pipeline is up and running. The `PHASE` for the `simple-pipeline` should be `Running`.
```shell
kubectl get pipeline # or "pl" as a short name
```
You can run the following command to check the isbsvc pods and the pipeline pods.
```shell
kubectl get pods
```
Here is an example output showing all the pods are up and running.
```shell
NAME                                         READY   STATUS      RESTARTS   AGE
isbsvc-default-js-0                          2/3     Running     0          19s
isbsvc-default-js-1                          2/3     Running     0          19s
isbsvc-default-js-2                          2/3     Running     0          19s
simple-pipeline-daemon-78b798fb98-qf4t4      1/1     Running     0          10s
simple-pipeline-out-0-xc0pf                  0/1     Running     0          10s
simple-pipeline-cat-0-kqrhy                  1/2     Running     0          10s
simple-pipeline-in-0-rhpjm                   1/1     Running     0          11s
```
Watch the `output` vertex pod log. Replace `xxxx` with the corresponding "out" vertex pod name.  
Use the above output as an example, the `output` vertex pod name is `simple-pipeline-out-0-xc0pf`.
```shell
kubectl logs -f simple-pipeline-out-0-xxxx main
```
You will see messages keep coming. Here is an example log output.
```
2022/08/25 23:59:38 (out) {"Data":"VT+G+/W7Dhc=","Createdts":1661471977707552597}
2022/08/25 23:59:38 (out) {"Data":"0TaH+/W7Dhc=","Createdts":1661471977707615953}
2022/08/25 23:59:38 (out) {"Data":"EEGH+/W7Dhc=","Createdts":1661471977707618576}
2022/08/25 23:59:38 (out) {"Data":"WESH+/W7Dhc=","Createdts":1661471977707619416}
2022/08/25 23:59:38 (out) {"Data":"YEaH+/W7Dhc=","Createdts":1661471977707619936}
2022/08/25 23:59:39 (out) {"Data":"qfomN/a7Dhc=","Createdts":1661471978707942057}
2022/08/25 23:59:39 (out) {"Data":"aUcnN/a7Dhc=","Createdts":1661471978707961705}
2022/08/25 23:59:39 (out) {"Data":"iUonN/a7Dhc=","Createdts":1661471978707962505}
2022/08/25 23:59:39 (out) {"Data":"mkwnN/a7Dhc=","Createdts":1661471978707963034}
2022/08/25 23:59:39 (out) {"Data":"jk4nN/a7Dhc=","Createdts":1661471978707963534}
```
The pipeline can be deleted by
```shell
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
```
The `Inter-Step Buffer Service` can be deleted by
```shell
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```

## Coming Up

Next, let's take a peek at [an advanced pipeline](./advanced-start.md), to learn some powerful features of Numaflow.
