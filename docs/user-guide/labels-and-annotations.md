# Labels And Annotations

Sometimes customized Labels or Annotations are needed for the vertices, for example, adding an annotation to enable or disable [Istio](https://istio.io/) sidecar injection. To do that, a `metadata` with labels or annotations can be added to the vertex.

```yaml
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
