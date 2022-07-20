# Autoscaling

Numaflow is able to run with both `Horizontal Pod Autoscaling` and `Vertical Pod Autoscaling`.

## Horizontal Pod Autoscaling

`Horizontal Pod Autoscaling` approaches supported in Numaflow include:

- Numaflow Autoscaling
- [Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- Third Party Autoscaling (such as [KEDA](https://keda.sh/))

### Numaflow Autoscaling

Numaflow provides `0 - N` autoscaling capability out of the box, it's available for all the `UDF`, `Sink` and following `Source` vertices.

- Kafka

Numaflow autoscaling is enabled by default, there are some parameters can be tuned to achieve better results.

```yaml
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
        lookbackSeconds: 180 # Optional, defaults to 180.
        cooldownSeconds: 90 # Optional, defaults to 90.
        zeroReplicaSleepSeconds: 180 # Optional, defaults to 180.
        targetProcessingSeconds: 3 # Optional, defaults to 0.
        targetBufferUsage: 50 # Optional, defaults to 50.
        replicasPerScale: 2 # Optional, defaults to 2.
```

- `disabled` - Whether to disable Numaflow autoscaling, defaults to `false`.
- `min` - Minimum replicas, valid value could be an interger >= 0. Defaults to `0`, which means it could be scaled down to 0.
- `max` - Maximum replicas, positive interger which should not be less than `min`, defaults to `50`. if `max` and `min` are the same, that will be the fixed replica number.
- `lookbackSeconds` - How many seconds to lookback for vertex average processing rate (tps) and pending messages calculation, defaults to `180`. Rate and pending messages metrics are critical for autoscaling, you might need to tune this parameter a bit to see better results. For example, your data source only have 1 minute data input in every 5 minutes, and you don't want the vertices to be scaled down to `0`. In this case, you need to increase `lookbackSeconds` to cover all the 5 minutes, so that the calculated average rate and pending messages won't be `0` during the silent period, to prevent scaling down to 0 from happening.
- `zeroReplicaSleepSeconds` - How many seconds it will wait after scaling down to `0`, defaults to `180`. Numaflow autoscaler periodically scales up a vertex pod to "peek" the incoming data, this is the period of time to wait before peeking.
- `targetProcessingSeconds` - It is used to tune the aggressiveness of autoscaling for source vertices, it measures how fast you want the vertex to process all the pending messages. It defaults to `3`. This is only effective for the `Source` vertices which support autoscaling, typically increasing the value, which leads to lower processing rate, thus less replicas.
- `targetBufferUsage` - Targeted buffer usage percentage, defaults to `50`. It is only effective for `UDF` and `Sink` vertices, it also determines how aggressive you want to do for autoscaling, increasing the value will bring more replicas.
- `replicasPerScale` - How many maximum replicas will be scaled up or down at once, defaults to `2`.

To disable Numaflow autoscaling, set `disabled: true` as following.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      scale:
        disabled: true
```

### Kubernetes HPA

[Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) is supported in Numaflow for any type of Vertex. To use HPA, remember to point the `scaleTargetRef` to the vertex as below, and disable Numaflow autoscaling in your Pipeline spec.

```yaml
apiVersion: autoscaling/v2beta1
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
```

With the configuration above, Kubernetes HPA controller will keep the target utilization of the pods of the Vertex at 50%.

Kubernetes HPA autoscaling is usful for those Source vertice not able to count pending messages, such as [HTTP](sources/http.md).

### Third Party Autoscaling

Third party autoscaling tools like [KEDA](https://keda.sh/) are also supported in Numaflow, which can be used to autoscale any type of vertex with the scalers it supports.

To use KEDA for vertex autoscaling, same as Kubernetes HPA, point the `scaleTargetRef` to your vertex, and disable Numaflow autoscaling in your Pipeline spec.

```yaml
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
```

## Vertical Pod Autoscaling

TBD.
