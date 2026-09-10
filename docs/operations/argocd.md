# Using Numaflow with ArgoCD

Numaflow's autoscaler updates `spec.replicas` on `MonoVertex` and `Vertex`
objects directly (via the Kubernetes scale subresource), the same way a
Horizontal Pod Autoscaler updates `spec.replicas` on a `Deployment`.

If you manage Numaflow resources with ArgoCD, this can cause ArgoCD to
report the resource as `OutOfSync`, because the live `replicas` value no
longer matches what's committed in Git — even though the pipeline is
healthy and scaling correctly.

## Recommended fix

Add an `ignoreDifferences` entry to your ArgoCD `Application` so it stops
comparing `spec.replicas` for autoscaled resources:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: my-numaflow-app
spec:
  ignoreDifferences:
    - group: numaflow.numaproj.io
      kind: MonoVertex
      jsonPointers:
        - /spec/replicas
    - group: numaflow.numaproj.io
      kind: Vertex
      jsonPointers:
        - /spec/replicas
  syncPolicy:
    syncOptions:
      - RespectIgnoreDifferences=true
\`\`\`

This tells ArgoCD to treat autoscaler-driven replica changes as expected
drift instead of a sync error, while still tracking real spec changes
made in Git.
