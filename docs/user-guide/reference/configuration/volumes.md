# Volumes

`Volumes` can be mounted to [`udf`](../../user-defined-functions/map/map.md) or [`udsink`](../../sinks/user-defined-sinks.md) containers.

Following example shows how to mount a ConfigMap to an `udf` vertex and an `udsink` vertex.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      volumes:
        - name: my-udf-config
          configMap:
            name: udf-config
      udf:
        container:
          image: my-function:latest
          volumeMounts:
            - mountPath: /path/to/my-function-config
              name: my-udf-config
    - name: my-sink
      volumes:
        - name: my-udsink-config
          configMap:
            name: udsink-config
      sink:
        udsink:
          container:
            image: my-sink:latest
            volumeMounts:
              - mountPath: /path/to/my-sink-config
                name: my-udsink-config
```
