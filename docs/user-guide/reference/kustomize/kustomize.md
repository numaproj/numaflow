# Kustomize Integration

## Transformers

Kustomize [Transformer Configurations](https://github.com/kubernetes-sigs/kustomize/tree/master/examples/transformerconfigs) can be used to do lots of powerful operations such as ConfigMap and Secret generations, applying common labels and annotations, updating image names and tags. To use these features with Numaflow CRD objects, download [numaflow-transformer-config.yaml](numaflow-transformer-config.yaml) into your kustomize directory, and add it to `configurations` section.

```yaml
kind: Kustomization
apiVersion: kustomize.config.k8s.io/v1beta1

configurations:
  - numaflow-transformer-config.yaml
  # Or reference the remote configuration directly.
  # - https://raw.githubusercontent.com/numaproj/numaflow/main/docs/user-guide/reference/kustomize/numaflow-transformer-config.yaml
```

Here is an [example](https://github.com/numaproj/numaflow/tree/main/docs/user-guide/reference/kustomize/examples/transformer) to use transformers with a Pipeline.

## Patch

Starting from version 4.5.5, kustomize can use Kubernetes [OpenAPI schema](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/openapi/) to provide merge key and patch strategy information. To use that with Numaflow CRD objects, download [schema.json](https://raw.githubusercontent.com/numaproj/numaflow/main/api/json-schema/schema.json) into your kustomize directory, and add it to `openapi` section.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

openapi:
  path: schema.json
  # Or reference the remote configuration directly.
  # path: https://raw.githubusercontent.com/numaproj/numaflow/main/api/json-schema/schema.json
```

For example, given the following Pipeline spec:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: in
      source:
        generator:
          rpu: 5
          duration: 1s
    - name: my-udf
      udf:
        container:
          image: my-pipeline/my-udf:v0.1
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: my-udf
    - from: my-udf
      to: out
```

You can update the `source` spec via a patch in a kustomize file.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - my-pipeline.yaml

openapi:
  path: https://raw.githubusercontent.com/numaproj/numaflow/main/api/json-schema/schema.json

patchesStrategicMerge:
  - |-
    apiVersion: numaflow.numaproj.io/v1alpha1
    kind: Pipeline
    metadata:
      name: my-pipeline
    spec:
      vertices:
        - name: in
          source:
            generator:
              rpu: 500
```

See the full example [here](https://github.com/numaproj/numaflow/tree/main/docs/user-guide/reference/kustomize/examples/patch).
