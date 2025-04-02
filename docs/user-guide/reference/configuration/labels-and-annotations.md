# Labels And Annotations

Sometimes customized _Labels_ or _Annotations_ are needed for the vertices, for example, adding an annotation to enable or disable [Istio](https://istio.io/) sidecar injection. To do that, a `metadata` with labels or annotations can be added to the Pipeline Vertex or MonoVertex.

A `Pipeline` example.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      metadata:
        labels:
          key1: val1
          key2: val2
        annotations:
          key3: val3
          key4: val4
```

A `MonoVertex` example.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  metadata:
    labels:
      key1: val1
      key2: val2
    annotations:
      key3: val3
      key4: val4
```
