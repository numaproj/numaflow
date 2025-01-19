# Liveness and Readiness

`Liveness` and `Readiness` probes have been pre-configured in the pods orchestrated in Numaflow, including the containers of `Vertex` and `MonoVertex` pods. For these probes, the probe handlers are not allowed to be customized, but the other configurations are.

- `initialDelaySeconds`
- `timeoutSeconds`
- `periodSeconds`
- `successThreshold`
- `failureThreshold`

Here is an example for `Pipeline` customization, similar configuration can be applied to containers including `udf`, `udsource`, `transformer`, `udsink` and `fb-udsink`.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-source
      containerTemplate: # For "numa" container
        readinessProbe:
          initialDelaySeconds: 30
          periodSeconds: 60
        livenessProbe:
          initialDelaySeconds: 60
          periodSeconds: 120
      volumes:
        - name: my-udsource-config
          configMap:
            name: udsource-config
      source:
        udsource:
          container:
            image: my-source:latest
            volumeMounts:
              - mountPath: /path/to/my-source-config
                name: my-udsource-config
            # For User-Defined source
            livenessProbe:
              initialDelaySeconds: 40
              failureThreshold: 5
    - name: my-udf
      containerTemplate: # For "numa" container
        readinessProbe:
          initialDelaySeconds: 20
          periodSeconds: 60
        livenessProbe:
          initialDelaySeconds: 180
          periodSeconds: 60
          timeoutSeconds: 50
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
          # For "udf"
          livenessProbe:
            initialDelaySeconds: 40
            failureThreshold: 5
```

The customization for `numa` container is also available with a [Vertex Template](./pipeline-customization.md#vertices) defined in `spec.templates.vertex`, which is going to be applied to all the vertices of a pipeline.

A `MonoVertex` example is as below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: simple-mono-vertex
spec:
  containerTemplate: # For "numa" container
    readinessProbe:
      initialDelaySeconds: 20
      periodSeconds: 60
    livenessProbe:
      initialDelaySeconds: 180
      periodSeconds: 60
  source:
    udsource:
      container:
        image: quay.io/numaio/numaflow-java/source-simple-source:stable
        # For User-Defined source
        livenessProbe:
          initialDelaySeconds: 40
          failureThreshold: 5
          timeoutSeconds: 40
    transformer:
      container:
        image: quay.io/numaio/numaflow-rs/source-transformer-now:stable
        # For transformer
        livenessProbe:
          initialDelaySeconds: 40
          failureThreshold: 5
  sink:
    udsink:
      container:
        image: quay.io/numaio/numaflow-java/simple-sink:stable
        # For User-Defined Sink
        livenessProbe:
          initialDelaySeconds: 40
          failureThreshold: 5
    fallback:
      udsink:
        container:
          image: my-sink:latest
          # # For Fallback Sink
          livenessProbe:
            initialDelaySeconds: 40
            failureThreshold: 5
```
