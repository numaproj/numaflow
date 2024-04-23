# Vertex

The `Vertex` is a key component of Numaflow `Pipeline` where the data processing happens. `Vertex` is defined as a list in the [pipeline](pipeline.md) spec, each representing a data processing task.

There are 3 types of `Vertex` in Numaflow today:

1. `Source` - To ingest data from sources.
1. `Sink` - To forward processed data to sinks.
1. `UDF` - User-defined Function, which is used to define data processing logic.

We have defined a [Kubernetes Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) for `Vertex`. A `Pipeline` containing multiple vertices will automatically generate multiple `Vertex` objects by the controller. As a user, you should NOT create a `Vertex` object directly.

In a `Pipeline`, the vertices are not connected directly, but through [Inter-Step Buffers](inter-step-buffer.md).

To query `Vertex` objects with `kubectl`:

```sh
kubectl get vertex # or "vtx" as a short name
```
