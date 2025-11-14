# Pipeline

The `Pipeline` represents a data processing job (a simpler version of this is called [MonoVertex](./monovertex.md)). The
most important concept in Numaflow, it defines:

1. A list of [vertices](vertex.md), which define the data processing tasks;
1. A list of `edges`, which are used to describe the relationship between the vertices. Note an edge may go from a vertex
   to multiple vertices, and an edge may also go from multiple vertices to a vertex. This many-to-one relationship is
   possible via [Join and Cycles](../user-guide/reference/join-vertex.md)

The `Pipeline` is abstracted as a [Kubernetes Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/). A `Pipeline` spec looks like below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: input-source
      source:
       ...
    - name: process
      udf:
        container:
         ...
    # ... other vertices
    - name: output-sink
      sink:
        ..
  edges:
    - from: input-source
      to: process
    # ... more edges and conditions
    - from: process
      to: output-sink
```

## Vertex Connections and Data Flow

Vertices in a Pipeline are connected through `edges`, which define the data flow path, and 
[Inter-Step Buffers](./inter-step-buffer.md) are responsible for passing data between vertices. Each edge specifies a 
`from` vertex and a `to` vertex, enabling complex topologies.

Data flows through these buffers asynchronously, providing durability and backpressure handling. Vertices can process 
data in parallel when multiple edges target the same destination vertex, and conditional routing is supported through 
message tagging for advanced use cases.

## Autoscaling and Performance

Each vertex in a Pipeline can be configured with independent autoscaling behavior through the `scale` configuration.
Numaflow provides built-in autoscaling that monitors buffer depth and processing rates, making scaling decisions based
on actual workload rather than generic CPU/memory metrics.

For high-throughput scenarios, vertices can be configured with multiple `partitions` to enable parallel processing withi
n a single vertex, distributing the workload across multiple buffer streams.

Learn more:
- [Autoscaling Guide](../user-guide/reference/autoscaling.md) - Detailed autoscaling configuration and best practices
- [Autoscaling Specification](../specifications/autoscaling.md) - Technical details of Numaflow's autoscaling mechanism

## Pipeline Configuration and Tuning

Pipelines support both global and vertex-specific configuration through the `limits` section. Global limits apply to all
vertices but can be overridden at the vertex level for fine-tuned performance control.

Additional pipeline-level configurations include watermark settings for event-time processing, graceful shutdown
timeouts, and custom metadata through labels and annotations for operational management.

Learn more:
- [Pipeline Tuning Guide](../user-guide/reference/pipeline-tuning.md) - Comprehensive guide to tuning pipeline performance
- [Pipeline Customization](../user-guide/reference/configuration/pipeline-customization.md) - Advanced configuration options
- [Container Resources](../user-guide/reference/configuration/container-resources.md) - CPU and memory resource configuration

## Operational Management

To query `Pipeline` objects with `kubectl`:

```sh
kubectl get pipeline # or "pl" as a short name
```

Pipelines automatically generate individual `Vertex` objects that are managed by the Numaflow controller. 

The Pipeline controller handles the complete lifecycle including buffer provisioning, vertex deployment, and cleanup, 
while maintaining the desired state and handling failures gracefully.
