# Quick Start

A Kubernetes cluster is needed to try it out, e.g. [Docker Desktop](https://www.docker.com/products/docker-desktop), or [K3d](https://k3d.io/).

Deploy into `numaflow-system` namespace:

```shell
kubectl create ns numaflow-system

kubectl apply -f ./config/install.yaml
```

Create an `ISBSvc (Inter-Step Buffer Service)` object.

```shell
kubectl apply -f ./examples/0-isbsvc-jetstream.yaml
```

After all the isbsvc pods are up, create a simple pipeline.

```shell
kubectl apply -f ./examples/1-simple-pipeline.yaml
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
