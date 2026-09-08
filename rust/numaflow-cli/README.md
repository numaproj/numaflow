# nfcli — local Numaflow UDF test CLI

`nfcli` tests a Numaflow user-defined function (UDF) locally by running the **real**
production vertex forwarder from `numaflow-core` against an **in-memory ISB**. It exercises
the identical code paths a deployed vertex runs — batching, ack/nack, retry, dedup, graceful
shutdown, watermark-driven window close — not a re-implementation of them.

```
nfcli ──writes file events──▶ [in-mem input buffer]
                                    │
                production read_isb → invoke_udf → write_isb → ack loop
                                    │
nfcli ◀──reads results──── [in-mem output buffer]
```

The engine is embedded via the `numaflow-core` `local-runner` feature; `nfcli` owns only the
user-facing surface (flags, YAML parsing, output rendering).

## Build

```bash
cargo build -p numaflow-cli          # produces target/debug/nfcli
```

## Connecting to a UDF

Every UDF subcommand connects over a **Unix domain socket** and reads the SDK **server-info**
file next to it (exactly what production does — version-compat problems surface here instead
of in the cluster).

```
--socket <path>          UDS path to the UDF server            (required)
--server-info <path>     server-info file path                 [default: derived]
--timeout <dur>          wait for socket + server-info ready   [default: 30s]
--max-message-size <n>   gRPC max send/recv size in bytes      [default: 64MiB]
```

**Derived `--server-info`:** when `--socket` is under `/var/run/numaflow`, the standard
container path (`/var/run/numaflow/<type>-server-info`) is used; otherwise
`<socket-dir>/<type>-server-info`.

## Subcommands

| Command | Notes |
|---|---|
| `map` | `--mode <unary\|batch\|stream>` is an optional assertion (server-info is authoritative). |
| `transform` | Runs a source transformer via an internal replay source. |
| `reduce` | `--window <fixed\|sliding> --length <dur>` (and `--slide` iff sliding), `--allowed-lateness`. |
| `session-reduce` | `--gap <dur>`. |
| `accumulator` | `--timeout <dur>`. |
| `sink` | `--fallback-socket`/`--fallback-server-info`, `--on-success-socket`/`--on-success-server-info`. |
| `source` | `--count <n>` / `--duration <dur>` / `--pending`; no input flags. |
| `side-input` | Single unary `RetrieveSideInput`. |
| `ready <type>` | Probe a server's `IsReady`. |

## Input

Data subcommands take either a YAML multi-doc file (`-f <path>`, `-` = stdin) **or** one inline
`--payload` / `--payload-file` / `--payload-base64` flag (plus `--key`, `--header K=V`,
`--event-time`, `--watermark`, `--id`, `--base-time`).

YAML document schema (strict — unknown fields error):

```yaml
payload: hello              # or payloadBase64: / payloadFile:
keys: [k1]
headers: {content-type: text/plain}
eventTime: "+5s"           # RFC3339 or "+dur" relative to base time
watermark: "+10s"          # reduce family only
id: my-id                  # unique per run — it is the ISB dedup key
userMetadata: {group1: {k: v}}
previousVertex: upstream
repeat: 3                  # emit 3 copies, each with a fresh unique id
```

## Output

`-o text` (default): one line per output event plus a `sent=… results=… elapsed=…` summary.
`-o json`: JSONL, one object per event then a summary object.
`-o raw`: concatenated payload bytes to stdout, diagnostics to stderr.

`-v` enables `numaflow_core=debug,nfcli=debug` — the window into wire-level behavior (window
ops, retries). `-q` silences everything but errors.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | Usage / validation error. |
| 2 | Socket / server-info / readiness not available within `--timeout`. |
| 3 | Drain timed out, or an internal error. |
| 4 | UDF / forwarder fatal error. |

## Example

```bash
# Go/Rust SDK example mapper serving over /var/run/numaflow/map.sock:
nfcli map --socket /var/run/numaflow/map.sock -f events.yaml
nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml --window fixed --length 60s
```

## Known limitations (accepted)

These come from the in-memory ISB backend and are fine for testing:

- **Tags/drops are not shown per-message** — tags are consumed inside the writer and never
  persisted to the ISB. A `sent > results` shortfall is reported as `dropped≈K` (design §9.1).
- **nack `delay` is ignored** (immediate redelivery) and there is no WIP-timeout auto-redelivery
  — a poison message spins until `--drain-timeout` expires, at which point the stuck count is
  reported (exit 3).
- **No WAL/fencing for reduce** — crash-replay is platform behavior, not UDF behavior.
- Buffers are non-durable; practical file sizes cap around ~10^5 messages.
