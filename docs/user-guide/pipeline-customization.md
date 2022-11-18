# Pipeline Customization

There is an optional `.spec.templates` field in the `Pipeline` resource which may be used to customize kubernetes resources owned by the Pipeline.

Vertex customization is described separately in more detail (i.e. [Environment Variables](./environment-variables.md), [Container Resources](./container-resources.md), etc.).

## Daemon Deployment

The following example shows how to configure a Daemon Deployment with all currently supported fields.

The `.spec.templates.daemon` field and all fields directly under it are optional.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    daemon:
      # Deployment spec
      replicas: 3
      # Pod metadata
      metadata:
        labels:
          my-label-name: my-label-value
        annotations:
          my-annotation-name: my-annotation-value
      # Pod spec
      nodeSelector:
        my-node-label-name: my-node-label-value
      tolerations:
        - key: "my-example-key"
          operator: "Exists"
          effect: "NoSchedule"
      securityContext: {}
      imagePullSecrets:
        - name: regcred
      priorityClassName: my-priority-class-name
      priority: 50
      serviceAccountName: my-service-account
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app.kubernetes.io/component
                    operator: In
                    values:
                      - daemon
                  - key: numaflow.numaproj.io/pipeline-name
                    operator: In
                    values:
                      - my-pipeline
              topologyKey: kubernetes.io/hostname
      # Containers
      containerTemplate:
        env:
          - name: MY_ENV_NAME
            value: my-env-value
        resources:
          limits:
            memory: 500Mi
      initContainerTemplate:
        env:
          - name: MY_ENV_NAME
            value: my-env-value
        resources:
          limits:
            memory: 500Mi
```

## Jobs

The following example shows how to configure kubernetes Jobs owned by a Pipeline with all currently supported fields.

The `.spec.templates.job` field and all fields directly under it are optional.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    job:
      # Job spec
      ttlSecondsAfterFinished: 600 # numaflow defaults to 30
      backoffLimit: 5 # numaflow defaults to 20
      # Pod metadata
      metadata:
        labels:
          my-label-name: my-label-value
        annotations:
          my-annotation-name: my-annotation-value
      # Pod spec
      nodeSelector:
        my-node-label-name: my-node-label-value
      tolerations:
        - key: "my-example-key"
          operator: "Exists"
          effect: "NoSchedule"
      securityContext: {}
      imagePullSecrets:
        - name: regcred
      priorityClassName: my-priority-class-name
      priority: 50
      serviceAccountName: my-service-account
      affinity: {}
      # Container
      containerTemplate:
        env:
          - name: MY_ENV_NAME
            value: my-env-value
        resources:
          limits:
            memory: 500Mi
```
