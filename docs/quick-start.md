# Quick Start

Install Numaflow and run a simple pipeline, which contains a source vertex to generate messages, a processing vertex that echos the messages, and a sink vertex that logs the messages.

## Prerequisites

A Kubernetes cluster is needed to try out Numaflow. A simple way to create a local cluster is using Docker Desktop.
* https://www.docker.com/
* https://www.docker.com/blog/how-kubernetes-works-under-the-hood-with-docker-desktop/

You will also need `kubectl` to manage the cluster.
* [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

## Installation

Run the following command lines to install Numaflow and start the Inter-Step Buffer Service that handles communication between vertices.
```shell
kubectl create ns numaflow-system
kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/stable/config/install.yaml
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```

## A Simple Pipeline
Create a `simple pipeline`.
```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
kubectl get pipeline # or "pl" as a short name

# Wait for pods to be ready
kubectl get pods
NAME                                         READY   STATUS      RESTARTS   AGE
isbsvc-default-js-0                          3/3     Running     0          19s
isbsvc-default-js-1                          3/3     Running     0          19s
isbsvc-default-js-2                          3/3     Running     0          19s
simple-pipeline-daemon-78b798fb98-qf4t4      1/1     Running     0          10s
simple-pipeline-out-0-xc0pf                  1/1     Running     0          10s
simple-pipeline-cat-0-kqrhy                  2/2     Running     0          10s
simple-pipeline-in-0-rhpjm                   1/1     Running     0          11s

# Watch the log for the `output` vertex
kubectl logs -f simple-pipeline-out-0-xxxx main
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

# Port forward the UI to https://localhost:8443/
kubectl -n numaflow-system port-forward deployment/numaflow-server 8443:8443
```
![Numaflow UI](assets/numaflow-ui-simple-pipeline.png)

The pipeline can be deleted by
```shell
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/1-simple-pipeline.yaml
```

## Coming Up

Next, let's take a peek at [an advanced pipeline](./advanced-start.md), to learn some powerful features of Numaflow.
