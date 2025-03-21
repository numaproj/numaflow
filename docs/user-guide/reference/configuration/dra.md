# Dynamic Resource Allocation

[Dynamic Resource Allocation](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/) can be used in either `Pipeline` or `MonoVertex`.

Look at the examples below.

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
  name: my-pipeline
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
---
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: my-mvtx
spec:
  source:
    udsource:
      container:
        image: my-source
        resources:
          claims:
            - name: gpu-1
  sink:
    udsink:
      container:
        image: my-sink
        resources:
          claims:
            - name: gpu-2
  resourceClaims:
    - name: gpu-1
      resourceClaimTemplateName: my-gpu
    - name: gpu-2
      resourceClaimTemplateName: my-gpu
```
