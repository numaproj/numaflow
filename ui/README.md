# Numaflow UI

![Numaflow Image](../docs/assets/numaproj.svg)

A web-based UI for Numaflow

The UI has the following features:
* View running pipelines in your namespace
* View Vertex and Edge Information of your pipeline
* View BackPressure and Pending Messages
* View Container Logs for a given vertex

## Development

See [Development Guide](../docs/development.md).

```shell
# Build image and deploy to a k3d cluster
make start
# Port-forward, and access https://localhost:8443
kubectl -n numaflow-system port-forward svc/numaflow-server 8443
```
