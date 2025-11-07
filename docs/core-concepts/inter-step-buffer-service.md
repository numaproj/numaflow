# Inter-Step Buffer Service

Inter-Step Buffer Service is the service to provide
[Inter-Step Buffers](inter-step-buffer.md).

An Inter-Step Buffer Service is described by a
[Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/).
It is required to be existing in a namespace before Pipeline objects are
created. A sample `InterStepBufferService` with JetStream implementation looks
like below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest # Do NOT use "latest" but a specific version in your real deployment
```

`InterStepBufferService` is a namespaced object. It can be used by all the
Pipelines in the same namespace. By default, Pipeline objects look for an
`InterStepBufferService` named `default`, so a common practice is to create an
`InterStepBufferService` with the name `default`. If you give the
`InterStepBufferService` a name other than `default`, then you need to give the
same name in the Pipeline spec.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  # Optional, if not specified, defaults to "default"
  interStepBufferServiceName: different-name
```

To query `Inter-Step Buffer Service` objects with `kubectl`:

```sh
kubectl get isbsvc
```

## JetStream

`JetStream` is one of the supported `Inter-Step Buffer Service` implementations.
A keyword `jetstream` under `spec` means a JetStream cluster will be created in
the namespace.

**For Production Setup**, please make sure you configure [replicas](#replicas),
[persistence](#persistence), [anti-affinity](#anti-affinity), and [PDB](#pdb).

### Version

Property `spec.jetstream.version` is required for a JetStream
`InterStepBufferService`. Supported versions can be found from the ConfigMap
[`numaflow-controller-config`](https://github.com/numaproj/numaflow/blob/main/config/base/controller-manager/numaflow-controller-config.yaml)
in the control plane namespace.

**Note**

The version `latest` in the ConfigMap should only be used for testing purpose.
It's recommended that you always use a fixed version in your real workload.

### Replicas

An optional property `spec.jetstream.replicas` (defaults to 3) can be specified,
which gives the total number of nodes.

### Persistence

Following example shows a JetStream `InterStepBufferService` with persistence.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest # Do NOT use "latest" but a specific version in your real deployment
    persistence:
      storageClassName: standard # Optional, will use K8s cluster default storage class if not specified
      accessMode: ReadWriteOncePod # Optional, defaults to ReadWriteOnce
      volumeSize: 10Gi # Optional, defaults to 20Gi
```

### Anti-Affinity

Anti-affinity is used to spread the ISB pods across different nodes.

#### Example Anti-Affinity

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest
    affinity:
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
          - podAffinityTerm:
              labelSelector:
                matchLabels:
                  app.kubernetes.io/component: isbsvc
                  numaflow.numaproj.io/isbsvc-name: default
              topologyKey: topology.kubernetes.io/zone
            weight: 100
```

### PDB

PDB (Pod Disruption Budget) is essential for running ISB in production to ensure
availability.

#### Example PDB Configuration

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: default
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/component: isbsvc
      numaflow.numaproj.io/isbsvc-name: default
```

### JetStream Settings

There are 2 places to configure JetStream settings:

- ConfigMap `numaflow-controller-config` in the control plane namespace.

  This is the default configuration for all the JetStream
  `InterStepBufferService` created in the Kubernetes cluster.

- Property `spec.jetstream.settings` in an `InterStepBufferService` object.

  This optional property can be used to override the default configuration
  defined in the ConfigMap `numaflow-controller-config`.

A sample JetStream configuration:

```
# https://docs.nats.io/running-a-nats-service/configuration#limits
# Only "max_payload" is supported for configuration in this section.
# Max payload size in bytes, defaults to 1 MB. It is not recommended to use values over 8MB but max_payload can be set up to 64MB.
max_payload: 1048576
#
# https://docs.nats.io/running-a-nats-service/configuration#jetstream
# Only configure "max_memory_store" or "max_file_store" in this section, do not set "store_dir" as it has been hardcoded.
#
# e.g. 1G. -1 means no limit, up to 75% of available memory. This only take effect for streams created using memory storage.
max_memory_store: -1
# e.g. 20G. -1 means no limit, Up to 1TB if available
max_file_store: 1TB
```

### Buffer Configuration

For the Inter-Step Buffers created in JetStream ISB Service, there are 2 places
to configure the default properties.

- ConfigMap `numaflow-controller-config` in the control plane namespace.

  This is the place to configure the default properties for the streams and
  consumers created in all the Jet Stream ISB

- Services in the Kubernetes cluster.

- Field `spec.jetstream.bufferConfig` in an `InterStepBufferService` object.

  This optional field can be used to customize the stream and consumer
  properties of that particular `InterStepBufferService`,

- and the configuration will be merged into the default one from the ConfigMap
  `numaflow-controller-config`. For example,
- if you only want to change `maxMsgs` for created streams, then you only need
  to give `stream.maxMsgs` in the field, all
- the rest config will still go with the default values in the control plane
  ConfigMap.

Both these 2 places expect a YAML format configuration like below:

```yaml
bufferConfig: |
  # The properties of the buffers (streams) to be created in this JetStream service
  stream:
    # 0: Limits, 1: Interest, 2: WorkQueue
    retention: 1
    maxMsgs: 30000
    maxAge: 168h
    maxBytes: -1
    # 0: File, 1: Memory
    storage: 0
    replicas: 3
    duplicates: 60s
  # The consumer properties for the created streams
  consumer:
    ackWait: 60s
    maxAckPending: 20000
```

**Note**

Changing the buffer configuration either in the control plane ConfigMap or in
the `InterStepBufferService` object does **NOT** make any change to the buffers
(streams) already existing.

### TLS

`TLS` is optional to configure through `spec.jetstream.tls: true`. Enabling TLS
will use a self signed CERT to encrypt the connection from Vertex Pods to
JetStream service. By default `TLS` is not enabled.

### Encryption At Rest

Encryption at rest can be enabled by setting `spec.jetstream.encryption: true`.
Be aware this will impact the performance a bit, see the detail at
[official doc](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest).

Once a JetStream ISB Service is created, toggling the `encryption` field will
cause problem for the exiting messages, so if you want to change the value,
please delete and recreate the ISB Service, and you also need to restart all the
Vertex Pods to pick up the new credentials.

### Other Configuration

Check [here](../APIs.md#numaflow.numaproj.io/v1alpha1.JetStreamBufferService)
for the full spec of `spec.jetstream`.
