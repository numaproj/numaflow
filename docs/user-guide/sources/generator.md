# Generator Source

Generator Source is mainly used for development purpose, where you want to have self-contained source to generate some messages.

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
          # How many messages to generate in the duration.
          rpu: 100
          duration: 1s
          # Optional, size of each generated message, defaults to 10.
          msgSize: 1024
    - name: p1
      udf:
        builtin:
          name: cat
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: p1
    - from: p1
      to: out
```
