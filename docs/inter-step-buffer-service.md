# Inter-Step Buffer Service

Inter-Step Buffer Service is the service to provide [Inter-Step Buffers](./inter-step-buffer.md).

An Inter-Step Buffer Service is describe by a [Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/), it is required to be existing in a namespace before Pipeline objects are created. A sample `InterStepBufferService` with JetStream implementation looks like below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest # Do NOT use "latest" but a specific version in your real deployment
```

`InterStepBufferService` is a namespaced object, it can be used by all the Pipelines in the same namespace. By default, Pipeline objects look for an `InterStepBufferService` named `default`, so a common practice is to create an `InterStepBufferService` with the name `default`. If you give the `InterStepBufferService` a name other than `default`, then you need to give the same name in the Pipeline spec.

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

`JetStream` is one of the supported `Inter-Step Buffer Service` implementations. A keyword `jetstream` under `spec` means a JetStream cluster will be created in the namespace.

### Version

Property `spec.jetstream.version` is required for a JetStream `InterStepBufferService`. Supported versions can be found from the ConfigMap `numaflow-controller-config` in the control plane namespace.

**Note**

The version `latest` in the ConfigMap should only be used for testing purpose, it's recommended to always use a fixed version in your real workload.

### Replicas

An optional property `spec.jetstream.replicas` (defaults to 3) can be specified, which gives the total number of nodes. An odd number 3 or 5 is suggested. If the given number < 3, 3 will be used.

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
      accessMode: ReadWriteOnce # Optional, defaults to ReadWriteOnce
      volumeSize: 10Gi # Optional, defaults to 20Gi
```

### JetStream Settings

There are 2 places to configure JetStream settings:

- ConfigMap `numaflow-controller-config` in the control plane namespace.

  This is the default configuration for all the JetStream `InterStepBufferService` created in the Kubernetes cluster.

- Property `spec.jetstream.settings` in an `InterStepBufferService` object.

  This optional property can be used to override the default configuration defined in the ConfigMap `numaflow-controller-config`.

A sample JetStream configuration:

```
# https://docs.nats.io/running-a-nats-service/configuration#jetstream
# Only configure "max_memory_store" or "max_file_store", do not set "store_dir" as it has been hardcoded.
#
# e.g. 1G. -1 means no limit, up to 75% of available memory. This only take effect for streams created using memory storage.
max_memory_store: -1
# e.g. 20G. -1 means no limit, Up to 1TB if available
max_file_store: 1TB
```

### Buffer Configuration

For the Inter-Step Buffers created in JetStream ISB Service, there are 2 places to configure the default properties.

- ConfigMap `numaflow-controller-config` in the control plane namespace.

  This is the place to configure the default properties for the streams and consumers created in all the Jet Stream ISB Services in the Kubernetes cluster.

- Field `spec.jetstream.bufferConfig` in an `InterStepBufferService` object.

  This optional field can be used to customize the stream and consumer properties of that particular `InterStepBufferService`, and the configuration will be merged into the default one from the ConfigMap `numaflow-controller-config`. For example, if you only want to change `maxMsgs` for created streams, then you only need to give `stream.maxMsgs` in the field, all the rest config will still go with the default values in the control plane ConfigMap.

Both these 2 places expect a YAML format configuration like below:

```yaml
bufferConfig: |
  # The properties of the buffers (streams) to be created in this JetStream service
  stream:
    # 0: Limits, 1: Interest, 2: WorkQueue
    retention: 1
    maxMsgs: 50000
    maxAge: 168h
    maxBytes: -1
    replicas: 3
    duplicates: 60s
  # The consumer properties for the created streams
  consumer:
    ackWait: 60s
    maxAckPending: 20000
```

**Note**

Changing the buffer configuration either in the control plane ConfigMap or in the `InterStepBufferService` object does **NOT** make any change to the buffers (streams) already existing.

### TLS

`TLS` is optional to configure through `spec.jetstream.tls: true`. Enabling TLS will use a self signed CERT to encrypt the connection from Vertex Pods to JetStream service. By default `TLS` is not enabled.

### Encryption At Rest

Encryption at rest can be enabled by setting `spec.jetstream.encryption: true`. Be aware this will impact the performance a bit, see the detail at [official doc](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest).

Once a JetStream ISB Service is created, toggling the `encryption` field will cause problem for the exiting messages, so if you want to change the value, please delete and recreate the ISB Service, and you also need to restart all the Vertex Pods to pick up the new credentials.

### Other Configuration

Check [here](APIs.md#numaflow.numaproj.io/v1alpha1.JetStreamBufferService) for the full spec of `spec.jetstream`.

## Redis

**NOTE** Today when using Redis, the pipeline will stall if Redis has any data loss, especially during failovers.

`Redis` is supported as an `Inter-Step Buffer Service` implementation. A keyword `native` under `spec.redis` means several Redis nodes with a [Master-Replicas](https://redis.io/topics/replication) topology will be created in the namespace.
We also support external redis.

#### External Redis
If you have a managed Redis, say in AWS, etc., we can make that Redis your ISB. All you need to do is provide the external Redis endpoint name.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  redis:
    external:
      url: "<external redis>"
      user: "default"
```

### Cluster Mode

We support [cluster mode](https://redis.io/docs/reference/cluster-spec/), only if the Redis is an external managed Redis. 
You will have to enter the `url` twice to indicate that the mode is cluster. This is because we use `Universal Client` which 
requires more than one address to indicate the Redis is in cluster mode.

```yaml
url: "numaflow-redis-cluster-0.numaflow-redis-cluster-headless:6379,numaflow-redis-cluster-1.numaflow-redis-cluster-headless:6379"
```


### Version

Property `spec.redis.native.version` is required for a `native` Redis `InterStepBufferService`. Supported versions can be found from the ConfigMap `numaflow-controller-config` in the control plane namespace.

### Replicas

An optional property `spec.redis.native.replicas` (defaults to 3) can be specified, which gives the total number of nodes (including master and replicas). An odd number >= 3 is suggested. If the given number < 3, 3 will be used.

### Persistence

Following example shows an `native` Redis `InterStepBufferService` with persistence.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  redis:
    native:
      version: 6.2.6
      persistence:
        storageClassName: standard # Optional, will use K8s cluster default storage class if not specified
        accessMode: ReadWriteOnce # Optional, defaults to ReadWriteOnce
        volumeSize: 10Gi # Optional, defaults to 20Gi
```

### Redis Configuration

Redis configuration includes:

- `spec.redis.native.settings.redis` - Redis configuration shared by both master and replicas
- `spec.redis.native.settings.master` - Redis configuration only for master
- `spec.redis.native.settings.replica` - Redis configuration only for replicas
- `spec.redis.native.settings.sentinel` - Sentinel configuration

A sample Redis configuration:

```
# Enable AOF https://redis.io/topics/persistence#append-only-file
appendonly yes
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
# Disable RDB persistence, AOF persistence already enabled.
save ""
maxmemory 512mb
maxmemory-policy allkeys-lru
```

A sample Sentinel configuration:

```
sentinel down-after-milliseconds mymaster 10000
sentinel failover-timeout mymaster 2000
sentinel parallel-syncs mymaster 1
```

There are 2 places to configure these settings:

- ConfigMap `numaflow-controller-config` in the control plane namespace.

  This is the default configuration for all the `native` Redis `InterStepBufferService` created in the Kubernetes cluster.

- Property `spec.redis.native.settings` in an `InterStepBufferService` object.

  This optional property can be used to override the default configuration defined in the ConfigMap `numaflow-controller-config`.

Here is the [reference](https://github.com/redis/redis/blob/unstable/redis.conf) to the full Redis configuration.

### Other Configuration

Check [here](APIs.md#numaflow.numaproj.io/v1alpha1.NativeRedis) for the full spec of `spec.redis.native`.
