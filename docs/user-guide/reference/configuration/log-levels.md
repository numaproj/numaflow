# Log Levels

Numaflow pods use two separate logging systems depending on whether they run Go or Rust code. This page explains how to control the log level for each.

## Quick reference

| Pod / container | Runtime | Log-level env var | Default level |
|---|---|---|---|
| Pipeline daemon | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| MonoVertex daemon | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| ISB svc create / delete job | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| ISB svc validate (init container) | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| Controller (`numaflow-controller`) | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| Webhook (`numaflow-webhook`) | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| UX server (`numaflow-server`) | Go | `NUMAFLOW_LOG_LEVEL` | `info` |
| Pipeline vertex `numa` container | Rust | `RUST_LOG` | `info` |
| MonoVertex `numa` container | Rust | `RUST_LOG` | `info` |
| Serving pod | Rust | `RUST_LOG` | `info` |
| InterStepBufferService (JetStream / Redis) | upstream image | n/a | n/a |

`NUMAFLOW_DEBUG=true` is honored by **both** runtimes but has different effects (see [below](#numaflow_debug-interaction)).

---

## Go components — `NUMAFLOW_LOG_LEVEL`

All Go binaries (daemon, controller, webhook, UX server, ISB service jobs) use the shared `NewLogger()` helper, which reads `NUMAFLOW_LOG_LEVEL` at startup.

**Accepted values:** any level recognized by [go.uber.org/zap/zapcore](https://pkg.go.dev/go.uber.org/zap/zapcore#Level): `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal`. In practice `debug`, `info`, `warn`, and `error` are the useful operational values.

**Default:** `info`

**Precedence:** `NUMAFLOW_LOG_LEVEL` overrides the level implied by `NUMAFLOW_DEBUG`. Invalid values are silently ignored and the default level is used instead.

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

## Rust components — `RUST_LOG`

Pipeline vertex pods, MonoVertex pods, and Serving pods run the Numaflow Rust data-plane binary. These use the [`tracing-subscriber`](https://docs.rs/tracing-subscriber) `EnvFilter`, which reads `RUST_LOG` at startup.

**Accepted values:** standard `EnvFilter` syntax — simple level names (`debug`, `info`, `warn`, `error`) or per-crate directives (`numaflow_core=debug,h2=warn,info`).

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
          - name: RUST_LOG
            value: warn
```

To enable debug logs for a specific crate only:

```yaml
          - name: RUST_LOG
            value: "numaflow_core=debug,info"
```

### MonoVertex pod

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
spec:
  containerTemplate:
    env:
      - name: RUST_LOG
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
        - name: RUST_LOG
          value: warn
```

---

## `NUMAFLOW_DEBUG` interaction

`NUMAFLOW_DEBUG=true` is a development shortcut. Its effects differ by runtime:

| Effect | Go | Rust |
|---|---|---|
| Log level | Lowered to `debug` | Lowered to `debug` (plus `h2::codec=info`) |
| Log format | Switches from JSON to console (human-readable) | Switches from JSON to human-readable text |
| Stacktraces | Added at `warn`+ (vs `error`+ by default) | n/a |

**Important:** switching from JSON to text format on the Rust side (`NUMAFLOW_DEBUG=true`) may break log shippers or aggregators that expect structured JSON. Prefer `RUST_LOG=debug` to lower the level without changing the output format.

`NUMAFLOW_LOG_LEVEL` overrides the level for Go components regardless of `NUMAFLOW_DEBUG`. There is no equivalent override on the Rust side — use `RUST_LOG` instead.

---

## Common recipes

**Suppress idle-rater info noise on a MonoVertex daemon:**
```yaml
# MonoVertex.spec.daemonTemplate.containerTemplate.env
- name: NUMAFLOW_LOG_LEVEL
  value: warn
```

**Enable debug for a single Rust crate without flooding all logs:**
```yaml
# Pipeline.spec.vertices[].containerTemplate.env
- name: RUST_LOG
  value: "numaflow_core=debug,info"
```

**Enable full debug on a vertex pod (both Go sidecar and Rust data-plane):**
```yaml
# Pipeline.spec.vertices[].containerTemplate.env
- name: NUMAFLOW_DEBUG
  value: "true"
# Note: this also switches log output from JSON to text.
# To keep JSON format while lowering the Rust level, use RUST_LOG=debug instead.
```
