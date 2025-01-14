# Update Strategy

When spec changes, the `RollingUpdate` update strategy is used to update pods in a `Pipeline` or `MonoVertex` by default, which means that the update is done in a rolling fashion. The default configuration is as below.

```yaml
updateStrategy:
  rollingUpdate:
    maxUnavailable: 25%
  type: RollingUpdate
```

- `maxUnavailable`: The maximum number of pods that can be unavailable during the update. Value can be an absolute number (ex: `5`) or a percentage of total pods at the start of update (ex: `10%`). Absolute number is calculated from percentage by rounding up. Defaults to `25%`.

## How It Works

The `RollingUpdate` strategy in Numaflow works more like the `RollingUpdate` strategy in `StatefulSet` rather than `Deployment`. It does not create `maxUnavailable` new pods and wait for them to be ready before terminating the old pods. Instead it replaces `maxUnavailable` number of pods with the new spec, then waits for them to be ready before updating the next batch.

For example, if there are 20 pods running, and `maxUnavailable` is set to the default `25%`, during the update, 5 pods will be unavailable at the same time. The update will be done in 4 batches. If your application has a long startup time, and you are sensitive to the unavailability caused tail latency, you should set `maxUnavailable` to a smaller value, and adjust the `scale.min` if it's needed.

During rolling update, [autoscaling](../autoscaling.md) will not be triggered for that particular Vertex or MonoVertex.

## Examples

A `Pipeline` example.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: my-vertex
      updateStrategy:
        rollingUpdate:
          maxUnavailable: 25%
        type: RollingUpdate
```

A `MonoVertex` example.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  source:
    udsource:
      container:
        image: my-image1
  sink:
    udsink:
      container:
        image: my-image2
  updateStrategy:
    rollingUpdate:
      maxUnavailable: 2
    type: RollingUpdate
```
