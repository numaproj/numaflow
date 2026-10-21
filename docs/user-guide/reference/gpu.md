# Configuring GPU

GPU resources can be configured on a Pipeline Vertex or a MonoVertex.

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
```

> Adjust the nodeSelector and annotations as required by your cluster setup.

## Using an Alternate Scheduler

Some clusters allocate GPUs with a scheduler other than the default Kubernetes scheduler, for example an AI workload scheduler such as KAI, Volcano or YuniKorn. Set `schedulerName` to have the vertex pods scheduled by it. Scheduler-specific settings, such as queue labels or annotations, can be added under `metadata`.

The following example uses [Volcano](https://volcano.sh):

```yaml
vertices:
  - name: gpu-vertex
    schedulerName: volcano        # Replace with the name of your cluster's scheduler
    udf:
      container:
        image: my-ml-image:latest
        resources:
          limits:
            nvidia.com/gpu: 1
```

To use the same scheduler for all vertices of a pipeline, set `schedulerName` under `spec.templates.vertex`. A value set on a vertex takes precedence. In the following example, `in` inherits `volcano` from the template, while `out` overrides it to use the default scheduler:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  templates:
    vertex:
      schedulerName: volcano
  vertices:
    - name: in
      source:
        generator:
          rpu: 5
          duration: 1s
    - name: out
      schedulerName: default-scheduler
      sink:
        log: {}
  edges:
    - from: in
      to: out
```

For a MonoVertex, set `schedulerName` under `spec`.

> If no scheduler with the given name is running in the cluster, the pods stay `Pending`.

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
