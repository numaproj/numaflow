# Pipeline Tuning

For a data processing pipeline, each vertex keeps running the cycle of reading data from an Inter-Step Buffer (or data source), processing the data, and writing to next Inter-Step Buffers (or sinks). It is possible to make some tuning for this data processing cycle.

- `readBatchSize` - How many messages to read for each cycle, defaults to `500`.
- `bufferMaxLength` - How many unprocessed messages can be existing in the Inter-Step Buffer, defaults to `30000`.
- `bufferUsageLimit` - The percentage of the buffer usage limit, a valid number should be less than 100. Default value is `80`, which means `80%`.

These parameters can be customized under `spec.limits` as below, once defined, they apply to all the vertices and Inter-Step Buffers of the pipeline.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  limits:
    readBatchSize: 100
    bufferMaxLength: 50000
    bufferUsageLimit: 85
```

They also can be defined in a vertex or edge level, which will override the pipeline level settings.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  limits: # Default limits for all the vertices and edges (buffers) of this pipeline
    readBatchSize: 100
    bufferMaxLength: 50000
    bufferUsageLimit: 85
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
      limits:
        readBatchSize: 200 # It overrides the default limit "100"
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: cat
      limits:
        bufferMaxLength: 20000 # It overrides the default limit "50000"
        bufferUsageLimit: 70 # It overrides the default limit "85"
    - from: cat
      to: out
```
