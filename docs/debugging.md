# How To Debug

To enable debug logs in a Vertex Pod, set environment variable `NUMAFLOW_DEBUG` to `true` for the Vertex. For example:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: input
      source:
        generator:
          rpu: 100
          duration: 1s
    - name: p1
      udf:
        builtin:
          name: cat
      containerTemplate:
        env:
          - name: NUMAFLOW_DEBUG
            value: "true" # DO NOT forget the double quotes!!!
    - name: output
      sink:
        log: {}
  edges:
    - from: input
      to: p1
    - from: p1
      to: output
```

## Profiling

Setting `NUMAFLOW_DEBUG` to `true` also enables `pprof` in the Vertex Pod.

For example, run the commands like below to profile memory usage for a Vertex Pod, a web page displaying the memory information will be automatically opened.

```sh
# Port-forward
kubectl port-forward simple-pipeline-p1-0-7jzbn 2469

go tool pprof -http localhost:8081 https+insecure://localhost:2469/debug/pprof/heap
```
