## Using GPU Resources in a Numaflow Vertex

### What is a Vertex?

A vertex is a processing step in a Numaflow pipeline. See [Vertex documentation](../../core-concepts/vertex.md) for details.

> **Note:**
> All the guidance in this document (GPU resource requests, annotations, node selectors, etc.) applies to both `Pipeline` and `MonoVertex` specifications in Numaflow.

### GPU Resources

To learn more about GPUs, see [Wikipedia: Graphics Processing Unit](https://en.wikipedia.org/wiki/Graphics_processing_unit).

## Prerequisites

Your cluster must support GPU scheduling and have the appropriate device plugin installed.  
See the [Kubernetes device plugins documentation](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) and [NVIDIA device plugin guide](https://github.com/NVIDIA/k8s-device-plugin) for details.

## Adding Annotations (If Required)

In most clusters, specifying the GPU resource in the `limits` field is enough. Some environments may require additional annotations for GPU access or monitoring. Check with your administrator if unsure.

**Example:**

```yaml
vertices:
  - name: gpu-vertex
    metadata:
      annotations:
        mycompany.com/gpu-enabled: "true"   # Replace with your annotation key and value
    udf:
      container:
        image: my-ml-image:latest
        resources:
          limits:
            nvidia.com/gpu: 1
```

## Specifying GPU Resource Requests and Limits

Request a GPU by setting the `nvidia.com/gpu` resource in the `limits` field under the container section:

```yaml
resources:
  limits:
    nvidia.com/gpu: 1
```

> **Important:**
> For GPUs, Kubernetes requires that `requests` and `limits` must be the same (or specify only `limits`).

### Example: Vertex Requesting a GPU (with Annotations and Node Selector)

```yaml
vertices:
  - name: gpu-vertex
    metadata:
      annotations:
        mycompany.com/gpu-enabled: "true"  # Example annotation, use only if required by your cluster
    nodeSelector:
      nvidia.com/gpu.present: "true"       # Replace with your cluster's GPU node label
    udf:
      container:
        image: my-ml-image:latest
        resources:
          limits:
            nvidia.com/gpu: 1
        # securityContext is only needed if your workload or cluster policy requires it
        # securityContext:
        #   capabilities:
        #     add: ["IPC_LOCK"]
```

> Adjust the nodeSelector and annotations as required by your cluster setup.

## Dynamic Resource Allocation (Advanced)

For advanced GPU scheduling using Dynamic Resource Allocation (DRA), see [Dynamic Resource Allocation documentation](./configuration/dra.md).

## Troubleshooting

If your vertex is not using GPU resources as expected:
- **Pod Pending:** Check for available GPU nodes and device plugin status.
- **Pod does not detect GPU:** Ensure your container image includes necessary GPU drivers and libraries (e.g., CUDA).
- **Still having issues?** Consult your cluster documentation or administrator.

## References

- [Container Resources](./configuration/container-resources.md)
- [Dynamic Resource Allocation](./configuration/dra.md)
- [Kubernetes: Managing Resources for Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
