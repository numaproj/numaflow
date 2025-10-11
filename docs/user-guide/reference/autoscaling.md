# Autoscaling

Numaflow [Pipeline](../../core-concepts/pipeline.md) and
[MonoVertex](../../core-concepts/monovertex.md) are both able to run with
`Horizontal Pod Autoscaling` and `Vertical Pod Autoscaling`.

## Horizontal Pod Autoscaling

`Horizontal Pod Autoscaling` approaches supported in Numaflow include:

- Numaflow Autoscaling
- [Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- Third Party Autoscaling (such as [KEDA](https://keda.sh/))

### Numaflow Autoscaling

Numaflow provides `0 - N` autoscaling capability out of the box, it's available
for all the [MonoVertices](../../core-concepts/monovertex.md) and
[Pipeline](../../core-concepts/pipeline.md)
[vertices](../../core-concepts/vertex.md) including `UDF`, `Sink` and most of
the [`Source`](../sources/overview.md) types (please check each source for more
details).

Numaflow autoscaling is enabled by default, there are some parameters can be
fine-tuned to achieve better results.

```yaml
# A Pipeline example.
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      scale:
        disabled: false # Optional, defaults to false.
        min: 0 # Optional, minimum replicas, defaults to 0.
        max: 20 # Optional, maximum replicas, defaults to 50.
        lookbackSeconds: 120 # Optional, defaults to 120.
        scaleUpCooldownSeconds: 90 # Optional, defaults to 90.
        scaleDownCooldownSeconds: 90 # Optional, defaults to 90.
        zeroReplicaSleepSeconds: 120 # Optional, defaults to 120.
        targetProcessingSeconds: 20 # Optional, defaults to 20.
        targetBufferAvailability: 50 # Optional, defaults to 50.
        replicasPerScaleUp: 2 # Optional, defaults to 2.
        replicasPerScaleDown: 2 # Optional, defaults to 2.
---
# A MonoVertex example.
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  scale:
    disabled: false # Optional, defaults to false.
    min: 0 # Optional, minimum replicas, defaults to 0.
    max: 20 # Optional, maximum replicas, defaults to 50.
    lookbackSeconds: 120 # Optional, defaults to 120.
    scaleUpCooldownSeconds: 90 # Optional, defaults to 90.
    scaleDownCooldownSeconds: 90 # Optional, defaults to 90.
    zeroReplicaSleepSeconds: 120 # Optional, defaults to 120.
    targetProcessingSeconds: 20 # Optional, defaults to 20.
    replicasPerScaleUp: 2 # Optional, defaults to 2.
    replicasPerScaleDown: 2 # Optional, defaults to 2.
```

- `disabled` - Whether to disable Numaflow autoscaling, defaults to `false`.
- `min` - Minimum replicas, valid value could be an integer >= 0. Defaults to
  `0`, which means it could be scaled down to 0.
- `max` - Maximum replicas, positive integer which should not be less than
  `min`, defaults to `50`. if `max` and `min` are the same, that will be the
  fixed replica number.
- `lookbackSeconds` - How many seconds to lookback for average processing rate
  (tps) and pending messages calculation, defaults to `120`. Rate and pending
  messages metrics are critical for autoscaling, you might need to tune this
  parameter a bit to see better results. For example, your data source only have
  1 minute data input in every 5 minutes, and you don't want the vertices to be
  scaled down to `0`. In this case, you need to increase `lookbackSeconds` to
  overlap 5 minutes, so that the calculated average rate and pending messages
  won't be `0` during the silent period, in order to prevent from scaling down
  to 0. The max value allowed to be configured is `600`. On top of this, we have
  dynamic lookback adjustment which tunes this parameter based on the realtime
  processing data.
- `scaleUpCooldownSeconds` - After a scaling operation, how many seconds to wait
  for the same Vertex or MonoVertex, if the follow-up operation is a scaling up,
  defaults to `90`. Please make sure that the time is greater than the pod to be
  `Running` and start processing, because the autoscaling algorithm will divide
  the TPS by the number of pods even if the pod is not `Running`.
- `scaleDownCooldownSeconds` - After a scaling operation, how many seconds to
  wait for the same Vertex or MonoVertex, if the follow-up operation is a
  scaling down, defaults to `90`.
- `zeroReplicaSleepSeconds` - After scaling a Source Vertex (or MonoVertex)
  replicas down to `0`, how many seconds to wait before scaling up to 1 replica
  to peek, defaults to `120`. Numaflow autoscaler periodically scales up a
  source vertex (or MonoVertex) pod to "peek" the incoming data, this is the
  period of time to wait before peeking.
- `targetProcessingSeconds` - It is used to tune the aggressiveness of
  autoscaling for each Vertex (or MonoVertex), it measures how fast you want the
  vertex to process all the pending messages, defaults to `20`. Typically
  increasing the value leads to lower processing rate, thus less replicas.
- `targetBufferAvailability` - [[Pipeline](../../core-concepts/pipeline.md)
  Only] Targeted buffer availability in percentage, defaults to `50`. It is only
  effective for `UDF` and `Sink` vertices of a Pipeline, works together with
  `targetProcessingSeconds`. It determines how aggressive you want to do for
  autoscaling, increasing the value will bring more replicas.
- `replicasPerScaleUp` - Maximum number of replica change happens in one scale
  up operation, defaults to `2`. For example, if current replica number is 3,
  the calculated desired replica number is 8; instead of scaling up the vertex
  to 8, it only does 5.
- `replicasPerScaleDown` - Maximum number of replica change happens in one scale
  down operation, defaults to `2`. For example, if current replica number is 9,
  the calculated desired replica number is 4; instead of scaling down the vertex
  to 4, it only does 7.
- `replicasPerScale` - (Deprecated: Use `replicasPerScaleUp` and
  `replicasPerScaleDown` instead, will be removed in v1.5) Maximum number of
  replica change happens in one scale up or down operation, defaults to `2`.

To disable Numaflow autoscaling, set `disabled: true` as following.

```yaml
# A Pipeline example.
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      scale:
        disabled: true
---
# A MonoVertex example.
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  scale:
    disabled: true
```

**Notes**

Numaflow autoscaling does not apply to reduce vertices of a Pipeline, and the
source vertices which do not have a way to calculate their pending messages.

- Generator
- HTTP
- Nats

For User-defined Sources, if the function `Pending()` returns a negative value,
autoscaling will not be applied.

### Kubernetes HPA

[Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
is supported in Numaflow for any type of Vertex. To use HPA, remember to point
the `scaleTargetRef` to the vertex as below, and disable Numaflow autoscaling in
your Pipeline spec.

```yaml
# A Pipeline example.
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: my-vertex-hpa
spec:
  minReplicas: 1
  maxReplicas: 3
  metrics:
    - resource:
        name: cpu
        targetAverageUtilization: 50
      type: Resource
  scaleTargetRef:
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: Vertex
    name: my-vertex
---
# A MonoVertex example.
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: my-mvtx-hpa
spec:
  minReplicas: 1
  maxReplicas: 3
  metrics:
    - resource:
        name: cpu
        targetAverageUtilization: 50
      type: Resource
  scaleTargetRef:
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: MonoVertex
    name: my-mvtx
```

With the configuration above, Kubernetes HPA controller will keep the target
utilization of the pods of the Vertex at 50%.

Kubernetes HPA autoscaling is useful for those Source vertices not able to count
pending messages, such as [HTTP](../sources/http.md).

### Third Party Autoscaling

Third party autoscaling tools like [KEDA](https://keda.sh/) are also supported
in Numaflow, which can be used to autoscale any type of vertex with the scalers
it supports.

To use KEDA for vertex autoscaling, same as Kubernetes HPA, point the
`scaleTargetRef` to your vertex, and disable Numaflow autoscaling in your
Pipeline spec.

```yaml
# A Pipeline example.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: my-keda-scaler
spec:
  scaleTargetRef:
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: Vertex
    name: my-vertex
  ... ...
---
# A MonoVertex example.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: my-keda-scaler
spec:
  scaleTargetRef:
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: MonoVertex
    name: my-mvtx
  ... ...
```

## Vertical Pod Autoscaling

`Vertical Pod Autoscaling` can be achieved by setting the `targetRef` to
`Vertex` objects as following.

```yaml
# A Pipeline example.
spec:
  targetRef:
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: Vertex
    name: my-vertex
---
# A MonoVertex example.
spec:
  targetRef:
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: MonoVertex
    name: my-mvtx
```
