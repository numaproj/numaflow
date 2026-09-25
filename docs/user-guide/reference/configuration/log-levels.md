# Log Levels

Numaflow-owned pods use `NUMAFLOW_LOG_LEVEL` as the standard log-level control. Data-plane pods also support `RUST_LOG` for advanced filtering.

## Quick reference

| Pod / container | Standard log-level env var | Default level |
|---|---|---|
| Pipeline daemon | `NUMAFLOW_LOG_LEVEL` | `info` |
| MonoVertex daemon | `NUMAFLOW_LOG_LEVEL` | `info` |
| ISB svc create / delete job | `NUMAFLOW_LOG_LEVEL` | `info` |
| ISB svc validate (init container) | `NUMAFLOW_LOG_LEVEL` | `info` |
| Controller (`numaflow-controller`) | `NUMAFLOW_LOG_LEVEL` | `info` |
| Webhook (`numaflow-webhook`) | `NUMAFLOW_LOG_LEVEL` | `info` |
| UX server (`numaflow-server`) | `NUMAFLOW_LOG_LEVEL` | `info` |
| Pipeline vertex `numa` container | `NUMAFLOW_LOG_LEVEL` | `info` |
| MonoVertex `numa` container | `NUMAFLOW_LOG_LEVEL` | `info` |
| Serving pod | `NUMAFLOW_LOG_LEVEL` | `info` |
| InterStepBufferService (JetStream / Redis) | n/a | n/a |

`NUMAFLOW_DEBUG=true` is also supported as a development shortcut (see [below](#numaflow_debug-interaction)).

---

## Standard log levels — `NUMAFLOW_LOG_LEVEL`

Numaflow-owned components read `NUMAFLOW_LOG_LEVEL` at startup.

**Accepted values:** `debug`, `info`, `warn`, `error`.

**Default:** `info`

**Precedence:** `NUMAFLOW_LOG_LEVEL` overrides the level implied by `NUMAFLOW_DEBUG`. Invalid values fall back to the level selected by `NUMAFLOW_DEBUG` or the default. For data-plane pods, `RUST_LOG` takes precedence over `NUMAFLOW_LOG_LEVEL` when set.

### Pipeline daemon pod

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
spec:
  templates:
    daemon:
      containerTemplate:
        env:
          - name: NUMAFLOW_LOG_LEVEL
            value: warn
```

### MonoVertex daemon pod

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
spec:
  daemonTemplate:
    containerTemplate:
      env:
        - name: NUMAFLOW_LOG_LEVEL
          value: warn
```

### Controller, webhook, and UX server

These are cluster-level components deployed via the install manifests. Set `NUMAFLOW_LOG_LEVEL` directly on the relevant Deployment:

```yaml
# numaflow-controller Deployment
env:
  - name: NUMAFLOW_LOG_LEVEL
    value: warn
```

### ISB service jobs

Init and finalizer jobs for pipeline ISB creation/deletion/validation:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
spec:
  templates:
    job:
      containerTemplate:
        env:
          - name: NUMAFLOW_LOG_LEVEL
            value: warn
```

---

## Pipeline, MonoVertex, and Serving pods

Pipeline vertex pods, MonoVertex pods, and Serving pods use `NUMAFLOW_LOG_LEVEL` for common log-level cases:

**Accepted values:** `debug`, `info`, `warn`, `error`.

**Default:** `info`

### Pipeline vertex pod

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
spec:
  vertices:
    - name: my-vertex
      containerTemplate:
        env:
          - name: NUMAFLOW_LOG_LEVEL
            value: warn
```

### MonoVertex pod

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
spec:
  containerTemplate:
    env:
      - name: NUMAFLOW_LOG_LEVEL
        value: warn
```

### Serving pod

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: ServingPipeline
spec:
  serving:
    containerTemplate:
      env:
        - name: NUMAFLOW_LOG_LEVEL
          value: warn
```

### Advanced data-plane filtering — `RUST_LOG`

Data-plane pods also support standard [`tracing-subscriber` EnvFilter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) via `RUST_LOG`. Use this only when you need fine-grained filtering. When `RUST_LOG` is set, it takes precedence over `NUMAFLOW_LOG_LEVEL`.

For example, to enable debug logs for a specific target only:

```yaml
# Pipeline.spec.vertices[].containerTemplate.env
- name: RUST_LOG
  value: "numaflow_core=debug,info"
```

---

## `NUMAFLOW_DEBUG` interaction

`NUMAFLOW_DEBUG=true` is a development shortcut. It lowers the default log level to `debug` and may switch log output from structured JSON to human-readable text.

**Important:** switching from JSON to text format may break log shippers or aggregators that expect structured JSON. Prefer `NUMAFLOW_LOG_LEVEL=debug` to lower the level without changing the output format.

`NUMAFLOW_LOG_LEVEL` overrides the level implied by `NUMAFLOW_DEBUG` without changing the format selected by `NUMAFLOW_DEBUG`. For data-plane pods, `RUST_LOG` takes precedence over `NUMAFLOW_LOG_LEVEL` when set.

---

## Common recipes

**Suppress idle-rater info noise on a MonoVertex daemon:**
```yaml
# MonoVertex.spec.daemonTemplate.containerTemplate.env
- name: NUMAFLOW_LOG_LEVEL
  value: warn
```

**Enable debug for a single data-plane target without flooding all logs:**
```yaml
# Pipeline.spec.vertices[].containerTemplate.env
- name: RUST_LOG
  value: "numaflow_core=debug,info"
```

**Enable full debug on a vertex pod:**
```yaml
# Pipeline.spec.vertices[].containerTemplate.env
- name: NUMAFLOW_DEBUG
  value: "true"
# Note: this also switches log output from JSON to text.
# To keep JSON format while lowering the level, use NUMAFLOW_LOG_LEVEL=debug instead.
```
