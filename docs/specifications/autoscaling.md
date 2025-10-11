# Autoscaling

## Overview

Numaflow developed its own dedicated autoscaling mechanism because the native
Kubernetes Horizontal Pod Autoscaler (HPA) is fundamentally not suitable for
event-driven stream processing.

The need for a specialized solution stems from three core limitations of HPA:

- **Metric Mismatch**: HPA scales based on generic resource consumption like CPU
  and memory utilization. In a streaming pipeline, the true measure of load is
  the number of unprocessed messages (queue depth). A low CPU reading might just
  mean a pod is waiting for data, and a high CPU reading doesn't tell you
  whether the next stage can handle the output. We need a scaling mechanism that
  correctly scales based on the actual backlog of work.

- **Lack of Pipeline Awareness**: Unlike HPA, we need a holistic,
  backpressure-aware solution. It doesn't just look at a single queue depth; it
  analyzes the status of the entire pipeline. This is crucial for preventing
  cascading failures by throttling upstream stages when downstream components
  are overwhelmed, ensuring resilient and efficient data flow.

- **Architectural Independence**: To enable fast, accurate, and self-sufficient
  scaling, the autoscaler should not rely on external infrastructure like
  Prometheus or the Kubernetes Metrics Server. We need a solution that gathers
  all necessary data internally, providing immediate access to metrics and
  simplifying operations.

By addressing these issues, Numaflow's autoscaler provides a scaling strategy
that is precise, cost-efficient, and natively designed for the dynamics of
real-time event streams.

## Scale Subresource

[Scale Subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#scale-subresource)
is enabled in `Vertex`
[Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/),
which makes it possible to scale vertex pods. To be specifically, it is enabled
by adding following comments to `Vertex` struct model, and then corresponding
CRD definition is automatically generated.

```
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
```

Pods management is done by vertex controller.

![Vertex Controller Reconciliation Loop](../assets/vertex_controller_loop.png)

With `scale` subresource implemented, `vertex` object can be scaled by either
horizontal or vertical pod autoscaling.

Similar capability is implemented for `MonoVertex`.

## Numaflow Autoscaling

The out of box Numaflow autoscaling is done by a `scaling` component running in
the controller manager, you can find the source code
[here](https://github.com/numaproj/numaflow/tree/main/pkg/reconciler/vertex/scaling).
The autoscaling strategy is implemented according to different type of vertices.

## Source Vertices

For source vertices, we define a target processing time (in seconds) to finish
processing the pending messages based on the processing rate (tps) of the
vertex.

```
  targetProcessingRate = pendingMessages / targetProcessingSeconds
  singlePodProcessingRate = currentProcessingRate / currentReplicas
  desiredReplicas =  targetProcessingRate / singlePodProcessingRate
```

For example, if `targetProcessingSeconds` is 3, current replica number is `2`,
current `tps` is 10000/second, and the pending messages is 60000, so we
calculate the desired replica number as following:

```
  desiredReplicas = 60000 / (3 * (10000 / 2)) = 4
```

Numaflow autoscaling does not work for those source vertices that can not
calculate pending messages.

## UDF and Sink Vertices

Pending messages of a UDF or Sink vertex do not always represent the real number
because of the restrained writing caused by back pressure, so we use different
strategies. When the number of pending messages does not hit the
`targetAvailableBufferLength`, same model as source vertex autoscaling is used,
otherwise we use a different model described below to achieve autoscaling for
them.

For each of the vertices, we calculate the available buffer length, and consider
it is contributed by all the replicas, so that we can get each replica's
contribution.

```
  availableBufferLength = totalBufferLength * bufferLimit(%) - pendingMessages
  singleReplicaContribution = availableBufferLength / currentReplicas
```

We define a target available buffer length, and then calculate how many replicas
are needed to achieve the target.

```
  desiredReplicas = targetAvailableBufferLength / singleReplicaContribution
```

## Back Pressure Impact

Back pressure is considered during autoscaling (which is only available for
Source and UDF vertices).

We measure the back pressure by defining a threshold of the buffer usage. For
example, the total buffer length is 50000, buffer limit is 80%, and the back
pressure threshold is 90%, if in the past period of time, the average pending
messages is more than `36000 (50000 * 80% * 90%)`, we consider there's back
pressure.

When the calculated desired replicas is greater than current replicas:

1. For vertices which have back pressure from the directly connected vertices,
   instead of increasing the replica number, we decrease it by 1;
2. For vertices which have back pressure in any of its downstream vertices, the
   replica number remains unchanged.

## Autoscaling Tuning

Numaflow autoscaling can be tuned by updating some parameters, find the details
at the [doc](../user-guide/reference/autoscaling.md).
