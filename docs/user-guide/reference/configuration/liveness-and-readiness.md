# Liveness and Readiness

`Liveness` and `Readiness` probes have been pre-configured in the pods orchestrated in Numaflow, including the containers of `Vertex` and `MonoVertex` pods. For these probes, the probe handlers are not allowed to be customized, but the other configurations are.

- `initialDelaySeconds`
- `timeoutSeconds`
- `periodSeconds`
- `successThreshold`
- `failureThreshold`

A field you leave unset keeps the built-in value. `successThreshold` is left unset, so Kubernetes uses `1`.

The `numa` container has both probes:

| Probe | `initialDelaySeconds` | `periodSeconds` | `timeoutSeconds` | `failureThreshold` |
| --- | --- | --- | --- | --- |
| readiness | 5 | 10 | 30 | 6 |
| liveness | 20 | 60 | 30 | 5 |

User-defined containers have a liveness probe only:

| `initialDelaySeconds` | `periodSeconds` | `timeoutSeconds` | `failureThreshold` |
| --- | --- | --- | --- |
| 30 | 60 | 30 | 5 |

Here is an example for `Pipeline` customization, similar configuration can be applied to containers including `udf`, `udsource`, `transformer`, `udsink`, `fb-udsink` and `ons-udsink`.

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
          # For Fallback Sink
          livenessProbe:
            initialDelaySeconds: 40
            failureThreshold: 5
```

## Startup

A vertex that is slow to start — replaying a large WAL, warming a cache, or loading a model — has
to buy that time out of its liveness budget, which is `initialDelaySeconds + (failureThreshold - 1)
* periodSeconds`. Widening `failureThreshold` to cover a one-off boot window permanently blunts
steady-state failure detection.

A `startupProbe` on the `numa` container gives the first start its own budget instead. It runs the
same handler as the liveness probe, and Kubernetes holds the liveness and readiness probes off
until it succeeds, after which it never runs again. No startup probe is configured unless you set
one, and any field you leave out falls back to the liveness probe's value.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-reduce
      containerTemplate: # For "numa" container
        startupProbe:
          initialDelaySeconds: 10
          periodSeconds: 10
          failureThreshold: 60 # Allow up to 10 minutes to start
```

The same configuration is available on `MonoVertex`, and via a
[Vertex Template](./pipeline-customization.md#vertices) to apply it to every vertex in a pipeline.

A `startupProbe` is deliberately not offered on the user-defined containers (`udf`, `udsource`,
`transformer`, `udsink`, `fb-udsink` and `ons-udsink`). Those run as sidecars, and their probe endpoint
`/sidecar-livez` is served by the `numa` container, which Kubernetes does not start until every
sidecar reports started. A startup probe on a sidecar would therefore gate itself on an endpoint
that cannot exist yet, and the pod would never start. Use the liveness settings above for those
containers.

A reduce vertex with persistence enabled can wait for a stale fence file that a previous instance
left. During that wait, `/sidecar-livez` reports the user-defined containers as live, so the wait
does not use their liveness budget.
