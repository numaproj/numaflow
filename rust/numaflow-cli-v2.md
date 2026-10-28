# Numaflow UDF Test CLI v2 — Embedded-Engine Design Plan

Supersedes `rust/numaflow-cli.md` (v1). The user-facing interface — subcommands, the YAML
message-file format, payload/key/time flags, batching, output formats — is carried over
from v1 unchanged except where explicitly listed in §8 (interface deltas). What changes is
**everything behind the flags**: instead of the CLI re-implementing the UDF gRPC
choreography, it embeds the production `numaflow-core` engine and runs the *real* vertex
forwarder against the **in-memory ISB** that now sits behind the `ISBReader`/`ISBWriter`/
`ISBFactory` traits.

```
v1:  nfcli ──(hand-rolled gRPC choreography)──▶ UDF server

v2:  nfcli ──writes file events──▶ [in-mem input buffer]
                                        │
                    production read_isb → invoke_udf → write_isb → ack loop
                    (ISBReaderOrchestrator → MapHandle/SinkWriter/Reducer →
                     ISBWriterOrchestrator, verbatim from numaflow-core)
                                        │
     nfcli ◀──reads results──── [in-mem output buffer]
```

The test therefore exercises the identical code paths a deployed vertex runs — batching,
ack/nack, retry, dedup, graceful shutdown, watermark-driven window close — not a parallel
re-implementation of them.

---

## 1. Why this is now possible (code inventory)

Everything load-bearing already exists; the CLI is a thin driver around it.

| Capability | Where it lives today |
|---|---|
| ISB behind traits | `ISBReader`/`ISBWriter` (`numaflow-core/src/pipeline/isb.rs:42,150`), dyn-erased as `ISBReaderRef`/`ISBWriterRef` (`isb/dyn_adapter.rs`) |
| In-memory ISB | `SimpleBuffer` + adapters (`pipeline/isb/inmemory/*`): shared-state buffers, offsets, ack/nack, dedup-by-id, pending counts, error injection |
| Name-keyed buffer registry | `InMemoryFactory` (`isb/inmemory/factory.rs:31`): `get_or_create_buffer` means the CLI's pre-population writer and the forwarder's reader share the same buffer; `buffer_stats(name) → (pending, in_flight)` exists explicitly "for drain detection by embedders" |
| Programmatic backend selection | `ISBClientConfig::InMemory` (`config/pipeline/isb.rs:17`) — deliberately unreachable from the k8s/env loader, programmatic-only |
| In-memory watermark KV | `InMemoryFactory::create_kv_store` returns a shared `SimpleKVStore` per bucket, so `ISBWatermarkPublisher`/fetcher work without NATS |
| Forwarder entry points | `pipeline::forwarder::start_forwarder` (`pipeline/forwarder.rs:59`) dispatching to `start_{source,map,sink,reduce}_forwarder`, all taking `Arc<dyn ISBFactory>` |
| Fully constructible config | `PipelineConfig` has `Default` (`config/pipeline.rs:91`); every vertex/edge/watermark sub-config is plain data |
| Working template | `test_reduce_over_inmemory_isb` (`pipeline/forwarder/reduce_forwarder.rs:617`) already does the entire v2 flow end-to-end: build `PipelineConfig` with `ISBClientConfig::InMemory`, pre-populate input buffer, run the real `start_reduce_forwarder`, publish watermarks through the in-memory KV, poll `buffer_stats` for drain, read reduced results from the output buffer |
| In-process source pattern | `source/test_utils.rs` shows how to stand up an SDK-backed source over a temp UDS socket and hand it to the production `Source` — the pattern for the CLI's replay source (§5.2) |

What does **not** exist yet and must be added: a public facade (everything above is
`pub(crate)`), the CLI binary itself, a replay source for transformer testing, and one
small production fix (§7.2).

---

## 2. Architecture

Two new pieces:

### 2.1 `numaflow-core::local` — a public facade module (feature `local-runner`)

`numaflow-core` internals stay `pub(crate)`. A new feature-gated module
`src/local.rs` (`pub mod local`, `#[cfg(feature = "local-runner")]`) exposes a minimal
embedding API and owns all config construction:

```rust
pub struct LocalRun { /* holds InMemoryFactory, CancellationToken, forwarder JoinHandle */ }

pub enum LocalUdf {
    Map    { socket: PathBuf, server_info: PathBuf },
    Sink   { socket: PathBuf, server_info: PathBuf },   // also fb-/ons- variants
    Reduce { socket: PathBuf, server_info: PathBuf, window: LocalWindow, keyed: bool },
    Transform { socket: PathBuf, server_info: PathBuf },   // replay source + transformer
    Source { socket: PathBuf, server_info: PathBuf },
}

pub enum LocalWindow { Fixed { length }, Sliding { length, slide },
                       Session { gap }, Accumulator { timeout } }

// Public mirror of the internal Message (which is pub(crate)):
pub struct InputEvent  { pub payload: Bytes, pub keys: Vec<String>,
                         pub event_time: DateTime<Utc>, pub watermark: Option<DateTime<Utc>>,
                         pub headers: HashMap<String, String>, pub id: String,
                         pub user_metadata: ..., pub previous_vertex: String }
pub struct OutputEvent { pub payload: Bytes, pub keys: Vec<String>,
                         pub event_time: DateTime<Utc>, pub headers: ...,
                         pub metadata: ..., pub id: String, pub offset: ... }

impl LocalRun {
    pub async fn start(udf: LocalUdf, opts: LocalRunOpts) -> Result<LocalRun>;
    pub async fn feed(&self, events: Vec<InputEvent>) -> Result<()>;   // paced by caller
    pub fn input_stats(&self) -> (usize, usize);                       // pending, in_flight
    pub async fn drain(&self, timeout: Duration) -> Result<DrainReport>;
    pub async fn stop(self) -> Result<()>;                             // cancel token + join
    pub async fn read_outputs(&self) -> Result<Vec<OutputEvent>>;      // fetch+ack out buffer
}
```

`LocalRunOpts`: batch size (default 500 = `PipelineConfig` default), read timeout,
graceful shutdown time, grpc max message size, buffer capacity. Internally `start`:

1. Builds a `PipelineConfig` (namespace/pipeline/vertex names = fixed `nfcli` literals,
   avoiding the `Box::leak` issue for `&'static str` fields; replica 0),
   `isb_client_config: ISBClientConfig::InMemory`, `metrics_server_listen_port: 0`,
   `callback_config: None`, `rate_limit: None`, `isb_config: None` (no compression),
   `wal_storage_config: None` for reduce (no WAL/fencing — both are optional today).
2. Creates one `Arc<InMemoryFactory>` and keeps it (this is the CLI's handle to the
   buffers), pre-creates the input writer / output stream names.
3. Spawns the corresponding production `start_*_forwarder(cln_token, factory, config, ...)`.
4. Does **not** call `numaflow_core::run()` — so the runtime-errors HTTPS server on :2470
   is never started, and no env vars are required.

The facade also hosts the reduce **watermark driver** (§5.3) and the **replay source**
(§5.2), both behind the same feature flag.

### 2.2 `rust/numaflow-cli` — the `nfcli` binary (new workspace member)

Owns everything user-facing and nothing engine-facing:

- clap definitions (same subcommands/flags as v1, deltas in §8),
- YAML multi-doc message-file parser (§4 of v1, unchanged: `payload`/`payloadBase64`/
  `payloadFile`, `keys`, `headers`, `eventTime`/`watermark` with `+dur` relative times,
  `id`, `userMetadata`, `previousVertex`, `repeat`, strict unknown-field errors),
- inline single-message builder,
- the feed loop (`--batch-size`/`--delay` pacing, §6),
- drain + timeout control,
- output rendering (`text`/`json`/`raw`, exit codes — §8 of v1, minor deltas here in §8).

Dependencies: `numaflow-core` (feature `local-runner`), `clap`, `serde_yaml`, `serde_json`,
`base64`, `humantime`, `chrono`, `tokio`. The v1 plan's `tonic`/`prost` protocol layer is
gone entirely.

---

## 3. Connecting to the UDF: sockets and server-info

The production gRPC clients are reused as-is (`shared/grpc.rs`,
`shared/create_components.rs`), which changes two v1 assumptions:

- **UDS only.** Every production client (`create_mapper_client`, `create_sink_client`,
  `create_transformer_client`, `create_source_client`, reduce channel) connects over a
  unix domain socket. The only TCP path is Python multiproc mode, negotiated via
  server-info, not via a flag. v1's `--tcp <[host:]port>` (Java local mode) is therefore
  **dropped for v2** (see Open Questions — it can return later as a small alternative
  channel builder, but it is not production-code reuse).
- **server-info is read — by design.** `sdk_server_info`
  (`numaflow-shared/src/server_info.rs:211`) runs on every client creation: it waits for
  the file, validates the `__END__` marker, and runs SDK/controller version-compat checks.
  v1 explicitly avoided this file; v2 embraces it because (a) it is exactly what
  production does, so compat problems surface in the test instead of in the cluster, and
  (b) every SDK serving over UDS writes the file next to the socket anyway. Locally-built
  dev binaries skip the controller-version check (version contains "latest"/matches git
  commit).

Flags:

```
--socket <path>              UDS path to the UDF server            (required)
--server-info <path>         server-info file path
                             [default: derived — see below]
--timeout <dur>              overall wait for socket + server-info + ready [default: 30s]
--max-message-size <bytes>   gRPC max send/recv size [default: 64MiB]
```

Default `--server-info`: the standard container path corresponding to the subcommand
(e.g. `/var/run/numaflow/mapper-server-info` for `map`) when `--socket` is a standard
`/var/run/numaflow/*.sock` path; otherwise `<socket-dir>/<container>-server-info`. The
production `read_server_info` polls forever; the facade wraps it with the CLI `--timeout`
via the cancellation token so a missing file fails fast with a clear message
("server-info not found at X — is the UDF server running? Override with --server-info").

**Map mode is no longer a flag.** v1 needed `--mode unary|batch|stream` because it refused
to read server-info; production `create_mapper` reads `MAP_MODE` from server-info and
picks the choreography itself. v2 inherits that. (`--mode` is kept as an optional
*assertion* — if given and it disagrees with server-info, fail with exit 1.) This needs
one production fix, §7.2.

---

## 4. Input: unchanged from v1

The YAML multi-document message file (v1 §4.3), inline single-message flags (v1 §4.2),
base-time / relative-time handling (v1 §4.4) are adopted verbatim. Only the mapping
target changes: each document now becomes a core `Message`
(`numaflow-core/src/message.rs:98`) written into the input buffer via the facade:

| YAML field | `Message` field | Notes |
|---|---|---|
| payload* | `value: Bytes` | |
| keys | `keys` | |
| headers | `headers` | |
| eventTime | `event_time` | drives window assignment inside the *real* windowers now |
| watermark | — (not persisted through ISB) | consumed by the reduce watermark driver (§5.3); ignored with a warning elsewhere |
| id | `id: MessageID { vertex_name: "nfcli-in", offset: <id>, index: 0 }` | `MessageID` string form is the **ISB dedup key** — `repeat` expansion must generate unique ids (`msg-<n>`), otherwise the in-memory writer silently dedups |
| userMetadata / previousVertex | `metadata` | note: production encode rewrites `previous_vertex` to the writing vertex's name; the facade sets the CLI's input-vertex name to the user's `previousVertex` value (or `nfcli-in`) so the UDF sees it |
| — | `offset` | dummy `Offset::Int(0,0)`; the buffer assigns real offsets on read |
| — | `typ` | `MessageType::Data` |

Late-data flag (`is_late`) is computed by production code paths, not user input.

---

## 5. Per-subcommand topology and flow

Every run follows the same five phases; only the topology differs.

```
1. START   facade builds PipelineConfig + InMemoryFactory, spawns the production forwarder
2. FEED    CLI writes file/inline events into the input buffer, --batch-size at a time,
           sleeping --delay between batches (forwarder consumes concurrently; buffer-full
           backpressure is handled by write retry)
3. DRAIN   after the last write: poll factory.buffer_stats(input) until pending==0 &&
           in_flight==0 (reduce: after the terminal watermark, also output stabilized),
           bounded by --drain-timeout [default 30s]
4. STOP    cancel the token → production graceful shutdown (reader stops, streams close,
           in-flight acks complete, mapper/reducer graceful window) → join
5. REPORT  fetch+ack everything in the output buffer(s), decode, render
```

Because input-buffer ack only happens *after* the downstream write succeeds (that's the
production loop), "input drained" implies "outputs written" — no separate output-side
completion tracking is needed for map/sink.

### 5.1 `map` — `[in] → map vertex → [out]`

`start_map_forwarder` with one `FromVertexConfig` (stream `nfcli-in-0`, 1 partition) and
one `ToVertexConfig` (stream `nfcli-out-0`, no conditions). Unary/batch/stream modes all
work through the same entry point since `MapHandle` picks choreography from server-info.
No watermark config (`watermark_config: None` — the orchestrators tolerate it).
Output events print keys, event time, headers, payload. **Tags are not printed** — see
§9.1 for why and what replaces them.

```bash
nfcli map --socket /var/run/numaflow/map.sock -f messages.yaml --batch-size 20 --delay 500ms
```

### 5.2 `transform` — replay source vertex with transformer → `[out]`

Transformers only run inside the source forwarder (`create_transformer` is source-only),
so the topology is: **replay source** (CLI-owned) + user's transformer →
`start_source_forwarder` → output buffer.

The replay source is a tiny `numaflow::source::Sourcer` implementation (Rust SDK, which
becomes an optional dep of `numaflow-core` under the `local-runner` feature — the SDK is
already a dev-dependency used by `source/test_utils.rs`): it serves the parsed file
events in order over a temp-dir UDS socket (SDK writes its own server-info there), acks
by offset, reports pending = remaining. Production `create_source` connects to it exactly
as it would to any UD source. FEED phase becomes "hand events to the replay source";
DRAIN becomes "replay source fully acked && output stable".

Output prints each result's (re-assigned) `event_time` prominently — the transformer's
defining feature — which survives the ISB round-trip since `event_time` is persisted.

### 5.3 `reduce` / `session-reduce` / `accumulator` — `[in] → reduce vertex → [out]`

Direct generalization of `test_reduce_over_inmemory_isb`:

- `ReduceVtxConfig` with Aligned (fixed/sliding from `--window fixed|sliding --length
  --slide`) or Unaligned (session `--gap`, accumulator `--timeout`) windower, `keyed:
  true`, `wal_storage_config: None` (no WAL, no fencing, no sliding state file).
- `watermark_config: Some(WatermarkConfig::Edge(...))` with buckets served by the
  factory's in-memory `SimpleKVStore` — reduce *requires* watermarks to close windows.
- **Watermark driver** (facade): plays the "previous vertex" — after writing each event
  it publishes that event's watermark (default: its event time; overridable per message
  via the YAML `watermark` field, e.g. to test late data — production drops late data
  before the UDF, and now that path really runs) to the OT bucket keyed by the written
  offset, mirroring what the upstream vertex's ISB writer would do.
- **End of input = watermark → +∞** (same semantics as v1): the driver publishes a
  terminal idle watermark (far-future WMB, as the existing test does), which makes the
  real windower close every open window through the real close path. Deterministic,
  always terminates.
- Windows, per-key fan-out, OPEN/APPEND/CLOSE/MERGE/EXPAND choreography, session merging
  — all production code now; v1's hand-ported windowers are gone.

Results are read from the output buffer; window bounds are reported from the result keys/
event times plus `-v` window-op tracing (facade taps the windower via log subscriber or
the tracker — best-effort, see §9.2).

Session-reduce and accumulator become their own subcommands mapping to
`ReducerConfig::Unaligned`, dropping v1's hand-rolled single-stream choreography.

### 5.4 `sink` — `[in] → sink vertex`

`start_sink_forwarder`; input buffer only, sink is terminal. Fallback and on-success
sinks are configured the production way: `--fallback-socket <p>` / `--on-success-socket
<p>` populate `SinkVtxConfig.fb_sink_config` / on-success config, and the **real**
fallback/on-success routing in `SinkWriter` runs (v1 could only point the whole run at
one sink server at a time). Retry behavior is the production behavior; a message the sink
keeps failing will be retried/nacked per `RetryUntilSuccess` — surface via `--drain-timeout`
expiry with a clear report of stuck offsets. Output is a summary (acked / retried /
routed-to-fallback counts from tracker + metrics registry), not per-message statuses (§9.3).

### 5.5 `source` — user source vertex → `[out]`

`start_source_forwarder` with the user's UD source and no transformer. No message file;
the CLI reads N results:

```
nfcli source --socket <p> [--count <n>]      # stop after n messages read  [default: 500]
             [--duration <dur>]              # or stop after a wall-clock duration
```

The production loop reads → writes to the output buffer → **acks the source** (real
`AckFn` traffic). The CLI fetches from the output buffer as messages arrive, prints them,
and cancels the token once `--count`/`--duration` is hit. `--pending` prints the source's
pending before/after via the source handle.

### 5.6 `side-input` and `ready`

Not part of the forwarder machinery. `side-input` stays as v1 designed it: a single unary
`RetrieveSideInput` call (via `numaflow-pb` client directly — 30 lines, nothing to reuse).
`ready` calls the production readiness helpers (`wait_until_*_ready`) for the given type.

---

## 6. Pacing, batching, termination

- `--batch-size <n>` [default 500]: both the CLI's feed-chunk size **and**
  `PipelineConfig.batch_size` (the forwarder's read batch) — matching numa's
  `limits.readBatchSize` semantics.
- `--delay <dur>` [default 0s]: sleep between feed chunks. Because the forwarder consumes
  concurrently, this now paces *arrival into the previous buffer* — closer to reality
  than v1's request pacing, and still useful for watching reduce streamers emit early
  results.
- `--drain-timeout <dur>` [default 30s]: upper bound on phase 3. The production loop has
  **no drain/exit concept — vertices run forever by design** — so the CLI owns
  termination. On expiry: report undrained offsets (pending/in_flight from
  `buffer_stats`, stuck entries from tracker) and exit 3. This is also the safety net for
  a UDF that nacks forever (in-memory nack = immediate redelivery, so a poison message
  spins — the report names it).

### 6.1 Failure and retry semantics

Production numaflow retries indefinitely on UDF failure. That "indefinitely" is two
different mechanisms, and the CLI bounds each differently:

**(a) In-process retry loops** — these genuinely never give up inside a running vertex:
sink `RetryUntilSuccess` (the default retry strategy), nacked messages redelivered from
the ISB (in-memory: immediately), buffer-full write retry, gRPC reconnect loops
(`UdfReconnectConfig`, retry every 1s), broker ack/nack retry (every 100ms,
`usize::MAX` attempts). The CLI does **not** alter any of them — they run verbatim, so
transient-failure-then-success scenarios exercise the real retry paths. The only bound
is `--drain-timeout`: if the input buffer hasn't drained by then, the CLI cancels the
token and reports *which* messages are stuck (ids/offsets still pending or in-flight,
redelivery observed) plus the last error seen in the logs, and exits 3. A permanently
failing message therefore turns into a named, bounded test failure instead of an
infinite spin.

**(b) Fatal errors** — a map/sink/reduce UDF error that production treats as critical
cancels the cancellation token internally (`map.rs`, `sink.rs`, writer paths), which
drains and stops the forwarder. In k8s, "retry indefinitely" for this class means *the
pod crash-loops and redelivers from the ISB after restart*. The CLI deliberately does
not emulate the restart loop: `LocalRun` `select!`s on the forwarder `JoinHandle`
throughout FEED/DRAIN, so the moment the forwarder task exits with an error the CLI
stops feeding, reports the error and the unprocessed backlog (fed vs. acked vs. still
pending), and exits 4 — fail-fast is the right semantic for a test tool. (A
`--restart-on-failure <n>` flag emulating the crash-loop is possible future work.)

**Reduce caveat:** with `wal_storage_config: None` (the CLI default), the PBQ acks ISB
messages *immediately* after handing them to the reducer (`read_isb_without_wal`), so a
mid-run reduce failure does not redeliver already-consumed messages — same as a
WAL-less reduce vertex in production. The run fails fast with the error; it does not
retry. Enabling `--wal <dir>` (future work, §9.5) would add replay-on-restart semantics,
which only matter if a restart loop is ever added.

Exit-code mapping: drain timeout → **3** (platform didn't converge); forwarder/UDF fatal
error → **4** (the UDF failed); both with a stuck/unprocessed-message report.

---

## 7. Required changes to `numaflow-core` (production code)

Kept deliberately minimal:

1. **`local` facade module** (feature `local-runner`, default off): the API in §2.1, the
   replay source, the reduce watermark driver, an `InputEvent`/`OutputEvent` ↔ `Message`
   mapping. Also make `pipeline::forwarder::start_forwarder` (or the four per-type
   functions) reachable from the facade — they're already `pub(crate)`, so the facade
   being in-crate needs **no visibility changes at all**.
2. **Fix the batch/stream socket override** in `create_mapper`
   (`shared/create_components.rs:388-401`): when server-info says Batch/Stream it
   unconditionally rewrites `socket_path` to the hardcoded
   `/var/run/numaflow/{batchmap,mapstream}.sock`. Fix: only override when the configured
   path is the default `map.sock` (or derive the sibling path from the configured
   socket's directory). This is arguably a latent production bug too — any non-default
   socket path config is silently ignored for batch/stream mappers.
3. **Cargo**: `local-runner` feature; `numaflow` SDK moves from dev-dependency to
   optional dependency gated on it.

Explicitly *not* changed: env loaders (InMemory stays programmatic-only), forwarder
logic, ISB traits, `run()`.

---

## 8. Interface deltas vs v1 (everything else is kept)

| v1 | v2 | Why |
|---|---|---|
| `--tcp <[host:]port>` | **dropped** | production clients are UDS-only; Java local-TCP mode isn't reachable through reused code |
| `--mode unary\|batch\|stream` | optional assertion only | mode comes from server-info via production `create_mapper` |
| "never read server-info" | server-info **is** read; `--server-info <path>` override added | it's what production does; version-compat issues surface at test time |
| `--socket` pointed at fb-/ons-sink to test them | `--fallback-socket` / `--on-success-socket` on `sink` | the real fallback/on-success *routing* now runs, not just the protocol |
| tags printed per result; `DROPPED` markers | summary counts; tags/drops not per-message (§9.1) | tags are consumed by routing inside the writer and never persisted to the ISB |
| `reduce` CLI emulates windowing | real windowers + watermark driver | that's the point of v2 |
| exit code 2 = connect/handshake failure | 2 = socket/server-info/ready not available within `--timeout` | same spirit, new mechanism |
| exit code 3 = protocol error | 3 = drain timeout / forwarder task error | protocol errors now manifest as forwarder errors |

Unchanged: subcommand set (`map`, `transform`, `reduce`, `session-reduce`, `accumulator`,
`sink`, `source`, `side-input`, `ready`), YAML file schema and `-f -` stdin, inline
payload flags, `--base-time`/relative times, `--batch-size`/`--delay`, `-o text|json|raw`,
`-v`/`-q`, exit codes 0/1/4 semantics.

---

## 9. Known fidelity gaps and limitations (accepted, documented)

1. **Tags are invisible in output.** Tags are not persisted through the ISB; they're
   consumed by conditional-forwarding logic inside `ISBWriteTask` (drops happen there
   too, before the writer is ever invoked). Consequences: per-message `tags=[...]` and
   `DROPPED` annotations from v1 are gone. Replacement: (a) summary accounting
   (`sent=N results=M` — a shortfall means drops, printed as `dropped≈K`); (b) *future
   work*: `--edge <name>[:when=tag,...]` to declare multiple conditional output edges and
   see real routing — which tests conditional forwarding better than printing tags ever
   did.
2. **`-v` wire-tracing** is now tracing-subscriber based (the CLI enables targeted
   `tracing` filters on numaflow-core modules) rather than v1's synthetic wire log.
   Fidelity depends on existing log lines; acceptable.
3. **Sink results are summary-level.** Per-message SUCCESS/FALLBACK/FAILURE visibility is
   internal to `SinkWriter` retry logic; the CLI reports aggregate counts and stuck
   messages. Testing "what status does my sink return for message X" at per-message
   granularity needs `-v` logs.
4. **In-memory ISB semantics** (inherent to the backend, all fine for testing): nack
   `delay` is ignored (immediate redelivery); no WIP-timeout auto-redelivery (explicit
   nack only); buffers are non-durable; `reclaim_acked` is O(n) — irrelevant at test
   volumes but caps practical file sizes around ~10^5 messages.
5. **No WAL/fencing for reduce** — deliberate (`wal_storage_config: None`); crash-replay
   is platform behavior, not UDF behavior. Could be optionally enabled later
   (`--wal <dir>`) since the code is there.
6. **Metrics server** starts on port 0 (fire-and-forget, bind failure non-fatal); the
   runtime-errors server never starts. On a *fatal* forwarder error, production code may
   try to persist under `/var/numaflow/runtime` — harmless failure locally, but worth a
   guard or env override if it proves noisy.

---

## 10. Milestones

1. **Facade + map happy path**: `local-runner` feature, `LocalRun` for `Map`, message
   loader, feed/drain/stop/report loop, text output.
   *Verify*: `nfcli map` against Go & Rust SDK example mappers (unary), plus a unit test
   that is essentially `test_reduce_over_inmemory_isb` reshaped for map — it becomes the
   facade's own regression test.
2. **Map modes + core fix**: batch/stream mappers via server-info; the
   `create_mapper` socket-override fix (own PR — it's a standalone production fix).
3. **Sink**: primary + `--fallback-socket`/`--on-success-socket`; summary reporting;
   drain-timeout reporting of stuck messages.
4. **Reduce family**: watermark driver + terminal-watermark close; `reduce` (fixed,
   sliding), then `session-reduce`, `accumulator`.
   *Verify*: SDK example counter/session/sorter; late-data test (per-message `watermark`
   in the file → message dropped before UDF, visible in accounting).
5. **Transform + source**: replay source; `transform` and `source` subcommands.
6. **Polish**: `json`/`raw` output, `side-input`, `ready`, `-v` tracing filters, exit
   codes, README, e2e script running the matrix against SDK example servers.

---

## 11. Resolved decisions (2026-08-04)

1. **server-info is mandatory** — confirmed. Servers that don't write it (Java
   `isLocal(true)`) fail with a clear error pointing at `--server-info`.
2. **`--tcp` / Java local mode is dropped** — confirmed.
3. **Tags/drops per-message visibility dropped** — confirmed; summary accounting now,
   `--edge` conditional-routing as future work.
4. **CLI shape: separate `nfcli` binary** (`rust/numaflow-cli` workspace member) —
   confirmed.
5. **Facade placement**: in-crate feature-gated `local` module (recommended default; no
   objection raised).
