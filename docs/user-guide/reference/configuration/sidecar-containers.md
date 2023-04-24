# Sidecar Containers

Additional "[sidecar](https://kubernetes.io/docs/concepts/workloads/pods/#how-pods-manage-multiple-containers)" containers can be provided for `udf` and `sink` vertices. `source` vertices do not currently support sidecars.

The following example shows how to add a sidecar container to a `udf` vertex.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      sidecars:
        - name: my-sidecar
          image: busybox:latest
          command: ["/bin/sh", "-c", "echo \"my-sidecar is running!\" && tail -f /dev/null"]
      udf:
        container:
          image: my-function:latest
```

There are various use-cases for sidecars. One possible use-case is a `udf` container that needs functionality
from a library written in a different language. The library's functionality could be made available through
gRPC over Unix Domain Socket. The following example shows how that could be accomplished using a shared [`volume`](volumes.md).

It is the sidecar owner's responsibility to come up with a protocol that can be used with the UDF. It could be volume, gRPC, TCP, HTTP 1.x, etc.,

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf-vertex
      volumes:
        - name: my-udf-volume
          emptyDir:
            medium: Memory
      sidecars:
        - name: my-sidecar
          image: alpine:latest
          command: ["/bin/sh", "-c", "apk add socat && socat UNIX-LISTEN:/path/to/my-sidecar-mount-path/my.sock - && tail -f /dev/null"]
          volumeMounts:
            - mountPath: /path/to/my-sidecar-mount-path
              name: my-udf-volume
      udf:
        container:
          image: alpine:latest
          command: ["/bin/sh", "-c", "apk add socat && echo \"hello\" | socat UNIX-CONNECT:/path/to/my-udf-mount-path/my.sock,forever - && tail -f /dev/null"]
          volumeMounts:
            - mountPath: /path/to/my-udf-mount-path
              name: my-udf-volume
```
