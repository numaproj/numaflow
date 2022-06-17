# Auto Scaling

NumaFlow is able to run with both `Horizontal Pod Autoscaling` and `Vertical Pod Autoscaling`.

## Horizontal Pod Autoscaling

`Horizontal Pod Autoscaling` approaches supported in NumaFlow include:

- NumaFlow Autoscaling
- [Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- Third Party Autoscaling (such as [KEDA](https://keda.sh/))

### NumaFlow Autoscaling

NumaFlow provides autoscaling capability out of the box.

More detail is coming soon.

### Kubernetes HPA

[Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) is supported in NumaFlow for any type of Vertex. To use HPA, remember to point the `scaleTargetRef` to the vertex as below.

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

Kubernetes HPA autoscaling is usful for those Source vertice not able to count pending messages, such as [HTTP](sources/HTTP.md).

### Third Party Autoscaling

Third party autoscaling tools like [KEDA](https://keda.sh/) are also supported in NumaFlow, which can be used to auto scale any type of vertex with the scalers it supports.

To use KEDA for vertex auto scaling, same as Kubernetes HPA, point the `scaleTargetRef` to your vertex.

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
