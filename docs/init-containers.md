# Init Containers

[Init Containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) can be provided for all the types of vertices.

The following example shows how to add an init-container to a `udf` vertex.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      initContainers:
        - name: my-init
          image: busybox:latest
          command: ["/bin/sh", "-c", "echo \"my-init is running!\" && sleep 60"]
      udf:
        container:
          image: my-function:latest
```

The following example shows how to use init-containers and [`volumes`](./volumes.md) together to provide a `udf` container files on startup.
```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      volumes:
        - name: my-udf-data
          emptyDir: {}
      initContainers:
        - name: my-init
          image: amazon/aws-cli:latest
          command: ["/bin/sh", "-c", "aws s3 sync s3://path/to/my-s3-data /path/to/my-init-data"]
          volumeMounts:
            - mountPath: /path/to/my-init-data
              name: my-udf-data
      udf:
        container:
          image: my-function:latest
          volumeMounts:
            - mountPath: /path/to/my-data
              name: my-udf-data
```
