## Using GPU Resources in a Numaflow Vertex

### What is a Vertex?

In Numaflow, a **vertex** is a core component of a pipeline. Each vertex represents a processing step, such as:
- Reading data from a source
- Transforming data using a user-defined function (UDF)
- Aggregating or reducing data
- Writing data to a sink

By connecting multiple vertices, you can build complex data processing pipelines for streaming or batch workloads.

> **Note:**
> All the guidance in this document (GPU resource requests, annotations, node selectors, etc.) applies to both `Pipeline` and `MonoVertex` specifications in Numaflow.

### Why Use GPU Resources?

Some data processing tasks are highly computational and can be executed significantly faster on a **GPU (Graphics Processing Unit)** than on a CPU. Common scenarios where GPU acceleration is beneficial include:
- Running machine learning or deep learning models
- Processing large images or videos
- Performing scientific or mathematical computations

## Prerequisites

Before you can request GPU resources for a vertex in Numaflow, ensure the following requirements are met:

- **Kubernetes cluster with GPU nodes**  
  Your Kubernetes cluster must have one or more nodes equipped with compatible GPU hardware (such as NVIDIA GPUs).
- **NVIDIA drivers installed on GPU nodes**  
  Each GPU node must have the appropriate NVIDIA drivers installed at the OS level.
- **NVIDIA device plugin installed**  
  The [NVIDIA device plugin for Kubernetes](https://github.com/NVIDIA/k8s-device-plugin) (or another appropriate GPU device plugin for your hardware) must be installed and running in your cluster. This allows Kubernetes to discover and schedule GPU resources.
- **Sufficient GPU resource quota**  
  Make sure your namespace or project has permission and quota to use GPU resources. Some clusters restrict GPU usage to specific namespaces or users.
- **Numaflow installed**  
  Numaflow must be installed and running in your Kubernetes cluster.
- **(Optional) Proper container image**  
  The container image used in your vertex should include the necessary drivers, libraries, and frameworks (such as CUDA, cuDNN, TensorFlow, or PyTorch) required to utilize the GPU.

## Adding Annotations (If Required)

In most Kubernetes clusters, simply specifying the GPU resource in the `limits` field is enough to request a GPU for your vertex. However, in some environments, additional annotations may be required. These annotations can be used by the Kubernetes scheduler, device plugins, or monitoring tools to enable or customize GPU scheduling, resource tracking, or other advanced behaviors.

You may need to add annotations if:
- Your cluster administrator or cloud provider requires them for GPU access.
- The GPU device plugin in your cluster uses annotations to enable or configure GPU features.
- You want to integrate with monitoring or policy tools that rely on pod annotations.

If you are unsure whether annotations are needed, consult your cluster documentation or administrator.

**Example:**

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: gpu-pipeline
spec:
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

**Steps:**
1. Under the desired vertex, add a `metadata` section.
2. Within `metadata`, add an `annotations` field.
3. List your annotation(s) as key-value pairs under `annotations`.

> **Note:**  
> - Always place annotations under the `metadata` field of the vertex, not under the `container` section.
> - You can add multiple annotations by including additional key-value pairs.

## Specifying GPU Resource Requests and Limits

To enable GPU acceleration for a vertex in your Numaflow pipeline, specify GPU resource requests and limits in the vertex configuration using the standard Kubernetes resource specification.

The most common way to request an NVIDIA GPU is by setting the `nvidia.com/gpu` resource in the `limits` field. For example, to request one GPU:

```yaml
resources:
  limits:
    nvidia.com/gpu: 1
```

You can add this under the `container` section of your vertex specification.

> **Important:**  
> For GPUs, Kubernetes requires that the value for `requests` and `limits` must be the same (or you can specify only `limits`). Setting different values can cause scheduling issues. For most use cases, specifying only `limits` is sufficient.

### Example: Vertex Requesting a GPU (with Annotations and Node Selector)

Below is an example of a Numaflow pipeline YAML where a vertex requests one NVIDIA GPU, adds a pod-level annotation, and uses a node selector to ensure scheduling on GPU-enabled nodes:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: gpu-pipeline
spec:
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

> **Note:**
> - Place pod-level annotations under the `metadata` field of the vertex, not under the container.
> - The need for annotations and securityContext depends on your Kubernetes cluster setup and workload. Check with your cluster administrator or documentation.
> - Adjust the nodeSelector to match your cluster's GPU node label.
> - You can change the number of GPUs by modifying the value (e.g., `nvidia.com/gpu: 2`).
> - The `securityContext` field is only needed if your workload or cluster policy requires it (for example, some CUDA operations or custom security policies). If you are unsure, check with your cluster administrator or try without it first.

## Dynamic Resource Allocation (Advanced)

For advanced or dynamic GPU scheduling, Numaflow supports [Dynamic Resource Allocation (DRA)](./configuration/dra.md). This allows for more flexible and powerful GPU resource management.

**Minimal Example:**

```yaml
apiVersion: resource.k8s.io/v1alpha3
kind: ResourceClaimTemplate
metadata:
  name: my-gpu
spec:
  spec:
    devices:
      requests:
        - name: gpu
          deviceClassName: gpu.nvidia.com
---
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: dra-gpu-pipeline
spec:
  vertices:
    - name: inference
      udf:
        container:
          image: my-image
          resources:
            claims:
              - name: gpu
      resourceClaims:
        - name: gpu
          resourceClaimTemplateName: my-gpu
```

See the linked documentation for more details and advanced examples.

## Troubleshooting

If your Numaflow vertex is not using GPU resources as expected:

- **Pod not scheduled or stuck in Pending:**  
  Check that your Kubernetes cluster has available GPU nodes and that the NVIDIA device plugin is running.
- **Pod starts but does not detect GPU:**  
  Ensure your container image includes the necessary GPU drivers and libraries (e.g., CUDA).
- **Still having issues?**  
  Review your cluster documentation or contact your administrator to confirm GPU access and configuration.

## References

- [Container Resources](./configuration/container-resources.md)  
  Learn how to configure CPU, memory, and other resources for your vertex containers in Numaflow.
- [Dynamic Resource Allocation](./configuration/dra.md)  
  Advanced: Use resource claims for dynamic GPU allocation in complex scenarios.
- [Numaflow Production Best Practices](https://numaflow.numaproj.io/docs/production/best-practices/)  
  Guidance on running Numaflow in production, including performance tuning and resource management.
- [NVIDIA device plugin for Kubernetes](https://github.com/NVIDIA/k8s-device-plugin)  
  Official documentation for setting up GPU support in your Kubernetes cluster.
- [Kubernetes: Managing Resources for Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
