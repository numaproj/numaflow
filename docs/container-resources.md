# Container Resources

[Container Resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) can be customized for all the types of vertices.

For configuring container resources on pods not owned by a vertex, see [Pipeline Customization](./pipeline-customization.md).

## Numa Container

To specify `resources` for the `numa` container of vertex pods:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      containerTemplate:
        resources:
          limits:
            cpu: "3"
            memory: 6Gi
          requests:
            cpu: "1"
            memory: 4Gi
```

## UDF Container

To specify `resources` for `udf` container of vertex pods:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      udf:
        container:
          resources:
            limits:
              cpu: "3"
              memory: 6Gi
            requests:
              cpu: "1"
              memory: 4Gi
```

## UDSink Container

To specify `resources` for `udsink` container of vertex pods:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      sink:
        udsink:
          container:
            resources:
              limits:
                cpu: "3"
                memory: 6Gi
              requests:
                cpu: "1"
                memory: 4Gi
```

## Init Container

To specify `resources` for the `init` container of vertex pods:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      initContainerTemplate:
        resources:
          limits:
            cpu: "3"
            memory: 6Gi
          requests:
            cpu: "1"
            memory: 4Gi
```

Container resources for [user init-containers](./init-containers.md) are instead set at `.spec.vertices[*].initContainers[*].resources`.