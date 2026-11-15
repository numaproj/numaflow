# Pod Specifications

Most of the Kunernetes Pod specification fields are supported in the spec of `Pipeline`, `MonoVertex` and `InterStepBufferService`. Those fields include:

- `nodeSelector`
- `tolerations`
- `securityContext`
- `imagePullSecrets`
- `priorityClassName`
- `priority`
- `affinity`
- `serviceAccountName`
- `runtimeClassName`
- `automountServiceAccountToken`
- `dnsPolicy`
- `dnsConfig`
- `resourceClaims`
- `schedulerName`

All the fields above are optional, click [here](../../../APIs.md#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate) to see full list of supported fields.

These fields can be specified in the `Pipeline` spec under:

- `spec.vertices[*]`
- `spec.templates.daemon`
- `spec.templates.job`
- `spec.templates.sideInputsManager`
- `spec.templates.vertex`

Or in the `MonoVertex` spec under:

- `spec`
- `spec.daemonTemplate`

Or in `InterStepBufferService` spec at:

- `spec.jetstream`

> The InterStepBufferService usually doesn't need an alternate scheduler. Only set `spec.jetstream.schedulerName` if your cluster requires all pods to go through that scheduler. JetStream is stateful, so avoid schedulers that may preempt its pods.
