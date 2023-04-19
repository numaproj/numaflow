# Pipeline

The `Pipeline` is the most important concept in Numaflow, it represents a data processing job, it defines:

1. A list of [vertices](vertex.md), which define the data processing tasks;
1. A list of `edges`, which are used to describe the relationship between the vertices.

The `Pipeline` is abstracted as a [Kubernetes Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/). A `Pipeline` spec looks like below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: in
      source:
        generator:
          rpu: 5
          duration: 1s
    - name: cat
      udf:
        builtin:
          name: cat
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: cat
    - from: cat
      to: out
```

To query `Pipeline` objects with `kubectl`:

```sh
kubectl get pipeline # or "pl" as a short name
```
