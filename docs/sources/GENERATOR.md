# Generator Source

Generator Source is maily used for development purpuse, where you want to have self contained source to generate some messages.

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
          # How many messages to generate in the duration
          rpu: 100
          duration: 1s
          # Optional, size of each generated message
          msgSize: 1024
    - name: p1
      udf:
        builtin:
          name: cat
    - name: output
      sink:
        log: {}
  edges:
    - from: input
      to: p1
    - from: p1
      to: output
```
