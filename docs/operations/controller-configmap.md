# Controller ConfigMap

The controller ConfigMap is used for controller-wide settings.

For a detailed example, please see [`numaflow-controller-config.yaml`](./numaflow-controller-config.yaml).

## Configuration Structure

The configuration should be under `controller-config.yaml` key in the ConfigMap, as a string in `yaml` format:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-controller-config
data:
  controller-config.yaml: |
    defaults:
      containerResources: |
        ...
    isbsvc:
      jetstream:
        ...
```

### Default Controller Configuration

Currently, we support configuring the init and main container resources for steps across all the pipelines. The configuration is under `defaults` key in the ConfigMap.

For example, to set the default container resources for steps across all the pipelines:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-controller-config
data:
  controller-config.yaml: |
    defaults:
      containerResources: |
        limits:
          memory: "256Mi"
          cpu: "200m"
        requests:
          memory: "128Mi"
          cpu: "100m"
```


### ISB Service Configuration

One of the important configuration items in the ConfigMap is about [ISB Service](../core-concepts/inter-step-buffer-service.md). We currently use 3rd party technologies such as `JetStream` to implement ISB Services, if those applications have new releases, to make them available in Numaflow, the new versions need to be added in the ConfigMap.

For example, there's a new `Nats JetStream` version `x.y.x` available, a new version configuration like below needs to be added before it can be referenced in the `InterStepBufferService` spec.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-controller-config
data:
  controller-config.yaml: |
    isbsvc:
      jetstream:
        versions:
          - version: x.y.x   # Name it whatever you want, it will be referenced in the InterStepBufferService spec.
            natsImage: nats:x.y.x
            metricsExporterImage: natsio/prometheus-nats-exporter:0.9.1
            configReloaderImage: natsio/nats-server-config-reloader:0.7.0
            startCommand: /nats-server
```
