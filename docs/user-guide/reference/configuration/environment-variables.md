# Environment Variables

## Log level control

Numaflow exposes three env vars for controlling log verbosity across its pods:

- `NUMAFLOW_LOG_LEVEL` — sets the log level (`debug`, `info`, `warn`, `error`) for Numaflow-owned components. Overrides the level implied by `NUMAFLOW_DEBUG`.
- `RUST_LOG` — advanced override for data-plane pods (vertex `numa` container, MonoVertex `numa` container, serving pods). Accepts standard [`tracing-subscriber` EnvFilter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) (e.g. `warn`, `numaflow_core=debug,info`) and takes precedence over `NUMAFLOW_LOG_LEVEL`.
- `NUMAFLOW_DEBUG` — development shortcut; sets level to `debug` and may switch log output from JSON to human-readable text. **Note:** the format change may break log shippers expecting JSON — prefer `NUMAFLOW_LOG_LEVEL` when only the level needs changing.

See [Log Levels](log-levels.md) for a full pod inventory, per-component YAML examples, and common recipes.

In [`udf`](../../user-defined-functions/map/map.md), [`udsink`](../../sinks/user-defined-sinks.md) and [`transformer`](../../sources/transformer/overview.md) containers, there are some preset environment variables that can be used directly.

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.
- `NUMAFLOW_CPU_REQUEST` - `resources.requests.cpu`, roundup to N cores, `0` if missing.
- `NUMAFLOW_CPU_LIMIT` - `resources.limits.cpu`, roundup to N cores, use host cpu cores if missing.
- `NUMAFLOW_MEMORY_REQUEST` - `resources.requests.memory` in bytes, `0` if missing.
- `NUMAFLOW_MEMORY_LIMIT` - `resources.limits.memory` in bytes, use host memory if missing.

For setting environment variables on pods not owned by a vertex, see [Pipeline Customization](pipeline-customization.md).

## Your Own Environment Variables

To add your own environment variables to `udf` or `udsink` containers, check the example below.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      udf:
        container:
          image: my-function:latest
          env:
            - name: env01
              value: value01
            - name: env02
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: my-key
    - name: my-sink
      sink:
        udsink:
          container:
            image: my-sink:latest
            env:
              - name: env03
                value: value03
```

Similarly, `envFrom` also can be specified in `udf` or `udsink` containers.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-udf
      udf:
        container:
          image: my-function:latest
          envFrom:
            - configMapRef:
                name: my-config
    - name: my-sink
      sink:
        udsink:
          container:
            image: my-sink:latest
            envFrom:
              - secretRef:
                  name: my-secret
```
