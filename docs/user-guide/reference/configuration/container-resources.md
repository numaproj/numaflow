# Container Resources

[Container Resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) can be customized for all the types of vertices.

For configuring container resources on pods not owned by a vertex, see [Pipeline Customization](pipeline-customization.md).

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
          claims:
            - name: my-claim
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
            claims:
              - name: my-claim
```

## UDSource Container

To specify `resources` for `udsource` container of a source vertex pods:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      source:
        udsource:
          container:
            resources:
              limits:
                cpu: "3"
                memory: 6Gi
              requests:
                cpu: "1"
                memory: 4Gi
              claims:
                - name: my-claim
```

## Source Transformer Container

To specify `resources` for `transformer` container of a source vertex pods:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      source:
        transformer:
          container:
            resources:
              limits:
                cpu: "3"
                memory: 6Gi
              requests:
                cpu: "1"
                memory: 4Gi
              claims:
                - name: my-claim
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
      resourceClaims:
        - name: my-claim
          xxx
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
              claims:
                - name: my-claim
```

## Init Container

To specify `resources` for the `init` init-container of vertex pods:

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
            cpu: "2"
            memory: 2Gi
          requests:
            cpu: "1"
            memory: 1Gi
```

Container resources for [user init-containers](init-containers.md) are instead specified at `.spec.vertices[*].initContainers[*].resources`.
