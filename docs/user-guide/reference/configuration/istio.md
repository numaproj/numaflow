# Running on Istio

If you want to get pipeline vertices running on Istio, so that they are able to talk to other services with Istio enabled, one approach is to whitelist the ports that Numaflow uses.

To whitelist the ports, add `traffic.sidecar.istio.io/excludeInboundPorts` and `traffic.sidecar.istio.io/excludeOutboundPorts` annotations to your vertex configuration:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      metadata:
        annotations:
          sidecar.istio.io/inject: "true"
          traffic.sidecar.istio.io/excludeOutboundPorts: "4222" # Nats JetStream port
          traffic.sidecar.istio.io/excludeInboundPorts: "2469" # Numaflow vertex metrics port
      udf:
        container:
          image: my-udf-image:latest
    ...
```

If you want to apply same configuration to all the vertices, use [Vertex Template](./pipeline-customization.md#vertices) configuration:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    vertex:
      metadata:
        annotations:
          sidecar.istio.io/inject: "true"
          traffic.sidecar.istio.io/excludeOutboundPorts: "4222" # Nats JetStream port
          traffic.sidecar.istio.io/excludeInboundPorts: "2469" # Numaflow vertex metrics port
  vertices:
    - name: my-vertex-1
      udf:
        container:
          image: my-udf-1-image:latest
    - name: my-vertex-2
      udf:
        container:
          image: my-udf-2-image:latest
    ...
```
