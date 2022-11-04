# Pipeline Customization

There is an optional `.spec.templates` field in the `Pipeline` resource which may be used to customize kubernetes resources owned by the Pipeline.
This takes precedence over anything specified in the [Controller ConfigMap](./controller-configmap.md#pipeline-templates-configuration).

Vertex customization is described separately (i.e. [Environment Variables](./environment-variables.md), [Container Resources](./container-resources.md), etc.)
and takes precedence over any vertex templates.

## Component customization

The following example shows all currently supported fields. The `.spec.templates.<component>` field and all fields directly under it are optional.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    # can be "daemon", "job" or "vertex"
    daemon:
      # Pod metadata
      metadata:
        labels:
          my-label-key: my-label-value
        annotations:
          my-annotation-key: my-annotation-value
      # Pod spec
      nodeSelector:
        my-node-label-key: my-node-label-value
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

## Daemon customization

In addition to the `Component customization` described above, the Pipeline daemon has the following additional fields available.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    daemon:
      replicas: 3
```

## Job customization

In addition to the `Component customization` described above, Pipeline jobs have the following additional fields available.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    job:
      ttlSecondsAfterFinished: 600 # numaflow defaults to 30
      backoffLimit: 5 # numaflow defaults to 20
```

## Vertex customization

See `Component customization` described above.
