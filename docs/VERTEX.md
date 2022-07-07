# Vertex

The `Vertex` is also a key concept of Numaflow, is is defined as a list in the [pipeline](./PIPELINE.md) spec, representing data processing tasks.

There are 3 types of `Vertex` in Numaflow today:

1. `Source` - To ingest data from sources.
1. `Sink` - To forward processed data to sinks.
1. `UDF` - User Defined Function, which is used to define data processing logic.

There's also a [Kubernetes Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) defined for `Vertex`. A `Pipeline` contains multiple vertices will automatically generate multiple `Vertex` objects by the controller. As a user you should NOT create a `Vertex` object directly.

In a `Pipeline`, the vertices are not connected directly, but through [Inter-Step Buffers](./INTER_STEP_BUFFER.md).

To query `Vertex` objects with `kubectl`:

```sh
kubectl get vertex # or "vtx" as a short name
```
