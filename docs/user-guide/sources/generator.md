# Generator Source

Generator Source is mainly used for development purpose, where you want to have self-contained source to generate some messages. We mainly use generator for load testing and integration testing of Numaflow.
The load generated is per replica.

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

## User Defined Data
The default data created by the generator is not likely to be useful in testing user pipelines with specific business logic. To allow this to help with user testing, a user defined value can be provided which will be emitted for each of the generator.

```
- name: in
  source:
    generator:
      # How many messages to generate in the duration.
      rpu: 100
      duration: 1s
      # Base64 encoding of data to send. Can be example serialized packet to
      # run through user pipeline to exercise particular capability or path through pipeline
      valueBlob: "InlvdXIgc3BlY2lmaWMgZGF0YSI="
      # Note: msgSize and value will be ignored if valueBlob is set
```