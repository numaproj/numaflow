# Numaflow UI

![Numaflow Image](../docs/assets/numaproj.svg)

A web-based UI for Numaflow

The UI has the following features:
* View running pipelines in your namespace
* View Vertex and Edge Information of your pipeline
* View BackPressure and Pending Messages
* View Container Logs for a given vertex

## Development

See [Development Guide](../docs/development/development.md).

```shell
# Build image and deploy to a k3d cluster
make start
# Port-forward, and access https://localhost:8443
kubectl -n numaflow-system port-forward svc/numaflow-server 8443
```

Import the App component the run the package as follows:
* CJS
```javascript
import App from 'numaflow-ui-test/dist/cjs/src/components/plugin/NumaflowMonitorApp/App';
```

* ESM
```javascript
import App from 'numaflow-ui-test/dist/esm/src/components/plugin/NumaflowMonitorApp/App';
```

* Pass the cluster hostname and namespace to get the UI running
```javascript
<App hostUrl={hostUrl} namespace={namespace} />
```
