# Numaflow UDF Test CLI v2 — Implementation Plan (LLM-ready)

Companion to the design doc `rust/numaflow-cli-v2.md` (read it first for the *why*; this
doc is the *how*). All paths are relative to `rust/` unless absolute. All line numbers
were verified against the working tree on 2026-08-04 — treat them as anchors, re-`grep`
if the file has drifted.

**Ground rules for the implementer:**

- Do not change forwarder logic, ISB traits, the env config loader, or `run()`. The only
  production-behavior change is Phase 1 (a bug fix).
- New facade code lives in `numaflow-core` behind a new cargo feature `local-runner`
  (default **off**) so the shipping binary is unaffected.
- Match existing code style: `pub(crate)` by default, `thiserror` errors, tracing macros,
  `trait_variant` is NOT needed anywhere new (facade is concrete types).
- The canonical template for almost everything in Phase 2 is the existing test
  `test_reduce_over_inmemory_isb` at
  `numaflow-core/src/pipeline/forwarder/reduce_forwarder.rs:617-862`. When in doubt,
  imitate it.
- Suggested PR sequence = phase sequence. Each phase compiles and has tests on its own.

---

## Phase 0 — Scaffolding

### 0.1 Workspace member

`Cargo.toml` (workspace root, members list at lines 3-20): add `"numaflow-cli"`.

### 0.2 Feature flag in numaflow-core

`numaflow-core/Cargo.toml`:

- `[features]` (lines 6-12): add `local-runner = ["dep:numaflow"]`.
- `[dependencies]`: add the Rust SDK as optional (same git pin as the existing
  dev-dependency at line 73):

  ```toml
  numaflow = { git = "https://github.com/numaproj/numaflow-rs", rev = "128dedad7ed3ccafc40ef23f9530a27be3ddfd0f", optional = true }
  ```

  Keep the `[dev-dependencies]` entry as-is (tests that don't enable the feature still
  need it). When the TODO at Cargo.toml:71-73 resolves (published crate), update both.

### 0.3 Facade module registration

`numaflow-core/src/lib.rs`: after `mod reduce;` (line 120), add:

```rust
/// Embeddable local runner for testing UDFs against the in-memory ISB (used by `nfcli`).
#[cfg(feature = "local-runner")]
pub mod local;
```

Verify: `cargo build -p numaflow-core` and `cargo build -p numaflow-core --features local-runner`.

---

## Phase 1 — Production fix: `create_mapper` socket-path override

**File:** `numaflow-core/src/shared/create_components.rs`, function `create_mapper`
(starts line 332). Inside the `Protocol::UDS` arm (~line 387) the code unconditionally
rewrites the configured socket path:

```rust
// current (buggy for any non-default socket path):
let config = match server_info.get_map_mode().unwrap_or(MapMode::Unary) {
    MapMode::Unary => config,
    MapMode::Batch => {
        config.socket_path = DEFAULT_BATCH_MAP_SOCKET.into();
        config
    }
    MapMode::Stream => {
        config.socket_path = DEFAULT_STREAM_MAP_SOCKET.into();
        config
    }
};
```

**Fix:** only override when the configured path is still the default map socket, so an
explicitly configured path (the CLI's, or any future custom mount) is honored:

```rust
let config = match server_info.get_map_mode().unwrap_or(MapMode::Unary) {
    MapMode::Unary => config,
    MapMode::Batch => {
        if config.socket_path == DEFAULT_MAP_SOCKET {
            config.socket_path = DEFAULT_BATCH_MAP_SOCKET.into();
        }
        config
    }
    MapMode::Stream => {
        if config.socket_path == DEFAULT_MAP_SOCKET {
            config.socket_path = DEFAULT_STREAM_MAP_SOCKET.into();
        }
        config
    }
};
```

- `DEFAULT_MAP_SOCKET` is a private const at `config/pipeline.rs:46`; widen to
  `pub(crate)` (its two siblings at lines 47-48 already are). Import it in
  `create_components.rs`.
- `server_info_path` is intentionally NOT touched (all map modes share
  `mapper-server-info`).
- Prefer extracting a pure helper so it's unit-testable without a gRPC server:

  ```rust
  fn resolve_map_socket_path(configured: String, mode: MapMode) -> String
  ```

  with tests: default+Batch → batchmap.sock; custom+Batch → custom; default+Unary →
  default; custom+Stream → custom.

This is a standalone PR — it fixes latent behavior for any non-default socket config.

---

## Phase 2 — `numaflow_core::local` facade

New directory `numaflow-core/src/local/` with:

```
local/
├── mod.rs          — public API surface, re-exports, LocalError
├── events.rs       — InputEvent / OutputEvent ⇄ Message conversion
├── config_builder.rs — LocalUdf → PipelineConfig construction
├── runner.rs       — LocalRun lifecycle (start/feed/drain/read_outputs/stop)
├── watermark_driver.rs — reduce watermark emulation
└── replay_source.rs — SDK-backed replay source (transform support)
```

Everything in `local/` is `pub` (it's the crate's only public API besides `run()`/
`monovertex`/`runtime_server`); internal helpers stay private to the module.

### 2.1 `mod.rs` — public types

```rust
pub use events::{InputEvent, OutputEvent};
pub use runner::{DrainReport, LocalRun, LocalRunOpts};

#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    #[error("configuration error: {0}")]
    Config(String),
    /// Socket / server-info / readiness not available within startup timeout → CLI exit 2
    #[error("UDF server not reachable: {0}")]
    Startup(String),
    /// Input did not drain within the drain timeout → CLI exit 3
    #[error("drain timed out: {0} of {1} messages still pending/in-flight")]
    DrainTimeout(usize, usize),
    /// The forwarder task ended with an error (fatal UDF failure) → CLI exit 4
    #[error("UDF/forwarder failed: {0}")]
    Forwarder(String),
    #[error("{0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, LocalError>;

pub enum LocalUdf {
    Map       { socket_path: PathBuf, server_info_path: PathBuf },
    Sink      { socket_path: PathBuf, server_info_path: PathBuf,
                fallback: Option<(PathBuf, PathBuf)>,
                on_success: Option<(PathBuf, PathBuf)> },
    Reduce    { socket_path: PathBuf, server_info_path: PathBuf,
                window: LocalWindow, keyed: bool, allowed_lateness: Duration },
    Transform { socket_path: PathBuf, server_info_path: PathBuf },
    Source    { socket_path: PathBuf, server_info_path: PathBuf },
}

pub enum LocalWindow {
    Fixed   { length: Duration },
    Sliding { length: Duration, slide: Duration },
    Session { gap: Duration },
    Accumulator { timeout: Duration },
}
```

Map internal `crate::error::Error` → `LocalError::Forwarder(e.to_string())` at the
facade boundary (`Error` is `pub(crate) use`d in lib.rs:56 and not nameable publicly —
never leak it in a signature).

### 2.2 `events.rs` — event types and `Message` conversion

Internal `Message` is at `numaflow-core/src/message.rs:98-128` (Default impl at
337-354); `MessageID` at 570-575; `Offset`/`IntOffset` at 358-361ff.

```rust
pub struct InputEvent {
    pub payload: bytes::Bytes,
    pub keys: Vec<String>,
    pub headers: std::collections::HashMap<String, String>,
    pub event_time: chrono::DateTime<chrono::Utc>,
    /// Only meaningful for reduce (watermark driver input); ignored elsewhere.
    pub watermark: Option<chrono::DateTime<chrono::Utc>>,
    /// Must be unique per run — it is the ISB dedup key (see gotcha G2).
    pub id: String,
    pub user_metadata: Option</* mirror of metadata::Metadata user groups */>,
    pub previous_vertex: Option<String>,
}

pub struct OutputEvent {
    pub payload: bytes::Bytes,
    pub keys: Vec<String>,
    pub headers: std::collections::HashMap<String, String>,
    pub event_time: chrono::DateTime<chrono::Utc>,
    pub id: String,          // MessageID display form "{vertex}-{offset}-{index}"
    pub offset: String,      // broker offset display form
    pub metadata_summary: Option<String>, // user metadata rendered, if present
}
```

Conversion `InputEvent → Message` (crate-internal fn in this module), modeled on the
test's write loop (`reduce_forwarder.rs:769-798`) and the factory test's
`test_message` (`pipeline/isb/inmemory/factory.rs:140-159`):

```rust
Message {
    typ: MessageType::Data,                       // Default
    keys: Arc::from(ev.keys),
    tags: None,
    value: ev.payload,
    offset: Offset::Int(IntOffset::new(0, 0)),    // dummy; buffer assigns real offsets
    event_time: ev.event_time,
    watermark: None,                              // not persisted through ISB anyway
    id: MessageID { vertex_name: INPUT_VERTEX.into(), offset: ev.id.into(), index: 0 },
    headers: Arc::new(ev.headers),
    metadata: /* build crate::metadata::Metadata from user_metadata/previous_vertex, else None */,
    is_late: false,
    nack_options: None,
}
```

`Message → OutputEvent` is a plain field copy (offset via `Display`).

Note on `previous_vertex`: the ISB proto encode (`message.rs:627-661`, rewrite at ~644)
overwrites `metadata.previous_vertex` with the *writing* vertex's name. For input events
that means the UDF sees the CLI's input-vertex name; to honor a user-supplied
`previousVertex`, the config builder names the input vertex accordingly (see G13 —
names must be `&'static str`, so this is `Box::leak`ed once per run; bounded).

### 2.3 `config_builder.rs` — `LocalUdf → PipelineConfig`

Key struct references (all `pub(crate)`, reachable because the facade is in-crate):

| Item | Location |
|---|---|
| `PipelineConfig` (+ `Default`) | `config/pipeline.rs:61-83`, `91-120` |
| `VertexConfig` enum | `config/pipeline.rs:192-197` |
| `SourceVtxConfig` | `config/pipeline.rs:123-126` |
| `SinkVtxConfig` | `config/pipeline.rs:184-189` |
| `MapVtxConfig` / `MapType` / map `UserDefinedConfig` (String paths) | `config/pipeline.rs:140-170` |
| `ReduceVtxConfig` | `config/pipeline.rs:200-204` |
| `FromVertexConfig` | `config/pipeline.rs:247-251` |
| `ToVertexConfig` | `config/pipeline.rs:289-299` |
| `VertexType` | `config/pipeline.rs:254-259` |
| `ISBClientConfig::InMemory` | `config/pipeline/isb.rs:18-24` |
| `Stream` (`&'static str` name!) | `config/pipeline/isb.rs:34-58` |
| `BufferWriterConfig` / `BufferReaderConfig` | `config/pipeline/isb.rs:67-83`, `114-128` |
| `MetricsConfig` | `config/components/metrics.rs:7-31` |
| `WatermarkConfig::Edge` / `EdgeWatermarkConfig` / `BucketConfig` | `config/pipeline/watermark.rs:8-11`, `196-199`, `183-191` |
| `ReducerConfig` & window configs | `config/components/reduce.rs:24-165` (reduce `UserDefinedConfig` has **`&'static str`** paths, line 42) |
| `SinkConfig` / `SinkType::UserDefined` / sink `UserDefinedConfig` | `config/components/sink.rs:30`, `36-44`, `160` |
| `SourceConfig` / `SourceType::UserDefined` / source `UserDefinedConfig` | `config/components/source.rs:26-40`, `84-93`, `405-419` |
| `TransformerConfig` | `config/components/transformer.rs:7-27` |

Fixed identity literals (module consts — `Stream`/`FromVertexConfig`/`BucketConfig`
need `&'static str`):

```rust
pub(crate) const INPUT_VERTEX: &str = "nfcli-in";
pub(crate) const UDF_VERTEX: &str = "nfcli-udf";
pub(crate) const OUTPUT_VERTEX: &str = "nfcli-out";
pub(crate) const INPUT_STREAM: &str = "nfcli-in-0";
pub(crate) const OUTPUT_STREAM: &str = "nfcli-out-0";
pub(crate) const INPUT_OT_BUCKET: &str = "nfcli-in-ot";
pub(crate) const OUTPUT_OT_BUCKET: &str = "nfcli-out-ot";
```

Common `PipelineConfig` skeleton (mirror the test literal at
`reduce_forwarder.rs:685-725`):

```rust
PipelineConfig {
    pipeline_name: "nfcli",
    vertex_name: UDF_VERTEX,
    replica: 0,
    batch_size: opts.batch_size,                  // default 500
    concurrency: opts.batch_size,
    read_timeout: Duration::from_millis(100),     // small: snappy drain detection
    graceful_shutdown_time: opts.graceful_shutdown, // default 5s for a test tool
    isb_client_config: ISBClientConfig::InMemory,
    from_vertex_config: vec![ /* per-kind, below */ ],
    to_vertex_config:   vec![ /* per-kind, below */ ],
    vertex_config:      /* per-kind */,
    vertex_type:        /* per-kind */,
    metrics_config: MetricsConfig { metrics_server_listen_port: 0, ..Default::default() },
    watermark_config: /* None except reduce */,
    ..Default::default()   // callback_config/isb_config/rate_limit None, ordered=false
}
```

Per-kind specifics:

- **Map** — `vertex_type: MapUDF`; `vertex_config: VertexConfig::Map(MapVtxConfig {
  concurrency: opts.batch_size, map_type: MapType::UserDefined(UserDefinedConfig {
  grpc_max_message_size, socket_path: <String>, server_info_path: <String> }) })`.
  `from_vertex_config`: one `FromVertexConfig { name: INPUT_VERTEX, reader_config:
  BufferReaderConfig { streams: vec![Stream::new(INPUT_STREAM, INPUT_VERTEX, 0)],
  ..Default::default() }, partitions: 1 }`. `to_vertex_config`: one `ToVertexConfig {
  name: OUTPUT_VERTEX, partitions: 1, writer_config: BufferWriterConfig { streams:
  vec![Stream::new(OUTPUT_STREAM, OUTPUT_VERTEX, 0)], max_length:
  opts.buffer_capacity, ..Default::default() }, conditions: None, to_vertex_type:
  VertexType::Sink, ordered_processing_enabled: false }` (the test uses exactly
  `to_vertex_type: Sink` for a terminal collector, line 711).
- **Sink** — `vertex_type: Sink`; `VertexConfig::Sink(Box::new(SinkVtxConfig {
  sink_config: SinkConfig { sink_type: SinkType::UserDefined(sink::UserDefinedConfig{..}),
  retry_config: None }, fb_sink_config: <same shape if fallback given>,
  on_success_sink_config: <ditto>, serving_store_config: None }))`. `from_vertex_config`
  as map; `to_vertex_config: vec![]` (terminal).
- **Reduce** — copy the test literal (`reduce_forwarder.rs:652-725`) exactly, swapping:
  the window config from `LocalWindow` (Fixed/Sliding → `ReducerConfig::Aligned`,
  Session/Accumulator → `ReducerConfig::Unaligned`; `streaming: false`;
  `allowed_lateness` from `LocalUdf::Reduce`); the reduce `UserDefinedConfig` paths via
  `Box::leak(path.into_boxed_str())` (its fields are `&'static str` — test lines
  667-680); `watermark_config: Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
  from_vertex_config: vec![input_bucket], to_vertex_config: vec![output_bucket] }))`
  with `BucketConfig { vertex: INPUT_VERTEX, partitions: vec![0], ot_bucket:
  INPUT_OT_BUCKET, delay: None }` (test lines 639-650).
- **Transform** — `vertex_type: Source`; `VertexConfig::Source(SourceVtxConfig {
  source_config: SourceConfig { source_type: SourceType::UserDefined(
  source::UserDefinedConfig { socket_path: <replay source socket>, server_info_path:
  <replay server-info>, .. }), .. }, transformer_config: Some(TransformerConfig{
  transformer_type: TransformerType::UserDefined(transformer::UserDefinedConfig {
  socket_path: <user's transformer>, .. }), .. }) })`. `from_vertex_config: vec![]`;
  `to_vertex_config` as map. `watermark_config: None`.
- **Source** — same as Transform with `transformer_config: None` and the user's socket
  in `SourceType::UserDefined`.

### 2.4 `runner.rs` — `LocalRun`

```rust
pub struct LocalRunOpts {
    pub batch_size: usize,              // default 500
    pub buffer_capacity: usize,         // default 30_000 (DEFAULT_MAX_LENGTH)
    pub grpc_max_message_size: usize,   // default 64 MiB
    pub startup_timeout: Duration,      // default 30s
    pub graceful_shutdown: Duration,    // default 5s
}

pub struct LocalRun {
    factory: Arc<InMemoryFactory>,
    input_writer: Option<ISBWriterRef>,          // None for Transform/Source kinds
    replay: Option<replay_source::ReplayHandle>, // Some for Transform kind
    watermark: Option<watermark_driver::WatermarkDriver>, // Some for Reduce kinds
    cln_token: CancellationToken,
    forwarder: tokio::task::JoinHandle<crate::error::Result<()>>,
    kind: /* enum discriminant */,
}
```

**`start(udf, opts)` sequence:**

1. **Pre-flight** (gives clean `LocalError::Startup` / CLI exit-2 semantics instead of
   hanging inside the forwarder): for each UDF socket in play, wait until the socket
   file exists, then call `numaflow_shared::server_info::sdk_server_info(server_info_path,
   preflight_token)` (`numaflow-shared/src/server_info.rs:211` — it polls forever, so
   wrap in `tokio::time::timeout(opts.startup_timeout)` and cancel `preflight_token` on
   expiry; `read_server_info` at :441 exits promptly on token cancellation). On timeout
   → `LocalError::Startup("server-info not found at <path> — is the UDF server
   running?")`. Keep the returned `ServerInfo` — the CLI uses it to report
   language/version and to validate a user-passed `--mode` assertion (map mode via
   `server_info.get_map_mode()`).
2. `let factory = Arc::new(InMemoryFactory::new());`
   (`pipeline/isb/inmemory/factory.rs:38`).
3. **Create the input writer FIRST** (kinds Map/Sink/Reduce), before the forwarder can
   race to create the buffer with default capacity — `get_or_create_buffer` is
   first-creation-wins (`factory.rs:59-83`):
   `factory.create_writer(input_stream, BufferWriterConfig { streams:
   vec![input_stream], max_length: opts.buffer_capacity, ..Default::default() }, None,
   cln_token.clone()).await` (test lines 729-739).
4. Reduce only: build the `WatermarkDriver` (§2.5) — it creates the OT KV store and the
   `ISBWatermarkPublisher`, and publishes the initial `base_time - 1` watermark (test
   lines 740-751).
5. Transform/Source-with-replay only: start the replay source server (§2.6).
6. Build the `PipelineConfig` (§2.3).
7. **Spawn the per-type forwarder directly, passing the shared factory** (gotcha G1 —
   see box below):

   ```rust
   let f: Arc<dyn ISBFactory> = Arc::clone(&factory) as _;
   let forwarder = tokio::spawn(match kind {
       Map    => start_map_forwarder(cln.clone(), f, cfg, map_cfg),        // map_forwarder.rs:97
       Sink   => start_sink_forwarder(cln.clone(), f, cfg, sink_cfg),      // sink_forwarder.rs:83
       Reduce => start_reduce_forwarder(cln.clone(), f, cfg, reduce_cfg),  // reduce_forwarder.rs:490
       Transform | Source => start_source_forwarder(cln.clone(), f, cfg, src_cfg), // source_forwarder.rs:72
   });
   ```

> **G1 — the single most important trap:** do **not** call the top-level
> `pipeline::forwarder::start_forwarder` (`pipeline/forwarder.rs:59`). Its first line
> calls `create_isb_factory(...)` (line 63) which constructs a **new, empty
> `InMemoryFactory`** — a different buffer registry than the one the CLI writes into,
> so the forwarder would read from an eternally-empty buffer and the run would hang
> until drain timeout. Call the four per-type `start_*_forwarder` functions, which take
> `Arc<dyn ISBFactory>` as a parameter — exactly what the template test does at
> `reduce_forwarder.rs:753-759`.

**`feed(events)`** (Map/Sink/Reduce): for each event, convert to `Message` and
`input_writer.write(msg).await`. On `WriteError::BufferFull` (returned eagerly by the
in-memory adapter — `inmemory/adapter.rs:218-224`), sleep 10ms and retry (the
orchestrator-side retry doesn't apply to this direct write path); check
`cln_token.is_cancelled()` and the forwarder handle between retries. Reduce only: after
each successful write, `watermark_driver.on_written(write_result.offset, event)` (§2.5).
For Transform: `feed` instead pushes events into the replay source's queue.

**`drain(timeout) -> Result<DrainReport>`**: the completion signal is the input buffer
fully acked — poll `factory.buffer_stats(INPUT_STREAM)` (`factory.rs:52-57`; note it
currently carries `#[allow(dead_code)]` — remove that attribute now that it has a
non-test caller) until `pending == 0 && in_flight == 0`. The test polls only
`pending == 0` (line 800-812) because it verifies via output counts; the facade needs
`in_flight == 0` too, since ack (which requires the downstream write to have completed —
that's the production loop's guarantee) is the real "done". Poll loop shape:

```rust
tokio::select! {
    res = &mut self.forwarder => return Err(LocalError::Forwarder(/* from res */)),
    _ = tokio::time::sleep(POLL_INTERVAL) => { /* check buffer_stats; reduce: also
        publish terminal watermark + check output stability, §2.5 */ }
}
```

bounded by `timeout` → `LocalError::DrainTimeout(stuck, total)` (the CLI renders stuck
ids from what it fed minus outputs). For Transform/Source-replay: drain = replay source
reports all offsets acked (its own counter) `&&` input side n/a. For plain Source there
is no drain; the CLI stops by count/duration (§3.4).

**`read_outputs()`**: `factory.create_reader(output_stream, None).await` once (cache
it), then loop `reader.fetch(batch, Duration::from_millis(100))` until an empty fetch;
**ack every fetched message before returning** (test lines 814-837; un-acked messages
would be redelivered by the next fetch — G8). Convert to `OutputEvent`. Callable
repeatedly (source kind polls it while running).

**`stop()`**: `cln_token.cancel()`, then `tokio::time::timeout(graceful_shutdown + 5s,
forwarder)`; surface a forwarder `Err` as `LocalError::Forwarder`, a timeout as
`Internal`. Also shuts down the replay-source server if any (test's server shutdown
pattern, line 859).

**Env-var identity (optional nicety):** `config::get_vertex_name()` etc.
(`config.rs:34-94`) read env with `OnceLock` and default to `"default"`. In `nfcli`'s
`main()` (not the facade), set `NUMAFLOW_VERTEX_NAME=nfcli-udf`,
`NUMAFLOW_PIPELINE_NAME=nfcli` before anything touches config, purely for readable
metric/tracing labels. `NUMAFLOW_REPLICA` stays unset (defaults 0 — reduce reads stream
index = replica, `reduce_forwarder.rs:157-163`, so single-partition input at index 0
just works).

### 2.5 `watermark_driver.rs` — reduce watermark emulation

Wraps `ISBWatermarkPublisher` (`watermark/isb/wm_publisher.rs:78`, `new` at :98,
`publish_watermark(stream, offset, watermark_ms, is_idle)` at :133). Construction —
copy test lines 740-746:

```rust
let ot_store = factory.create_kv_store(INPUT_OT_BUCKET.to_string()).await?;
let publisher = ISBWatermarkPublisher::new(
    "nfcli-in-0".to_string(),                      // processor name
    HashMap::from([(INPUT_VERTEX, ot_store)]),
    std::slice::from_ref(&input_bucket_config),
    false,
);
```

Behavior:

- `new(...)`: publish `base_time.timestamp_millis() - 1` at offset 0, `is_idle=false`
  (test lines 749-751) so the fetcher has a floor before data arrives.
- `on_written(offset, event)`: `wm = event.watermark.unwrap_or(event.event_time)`;
  maintain `high = max(high, wm)` and publish `high` at the written offset
  (watermarks are monotonic; publishing raw per-event values for out-of-order input
  would regress). Late-data testing works naturally: a user sets an early message's
  `watermark` far ahead, then later messages with older `event_time` are flagged late
  by the real windower.
- `finish(last_offset)`: enter terminal mode — publish
  `TERMINAL_WATERMARK_MS = 253_402_300_799_000` (9999-12-31, test line 619) at
  `last_offset + 1` with `is_idle=true`, and **keep re-publishing it inside the drain
  poll loop** (the test does this in its output loop, lines 818-823, with the comment
  that re-publishing keeps the synthetic upstream processor alive while the forwarder
  processes the idle watermark — G9). Reduce drain completes when input buffer is
  drained **and** an output `fetch` returns empty twice in a row after at least one
  result (or immediately on double-empty if the user expects none — the CLI knows the
  expected minimum is ≥0, so "stable" = two consecutive empty fetches post-drain).

### 2.6 `replay_source.rs` — SDK-backed replay source

Only compiled under `local-runner` (the `numaflow` SDK dep). Do **not** reuse
`source/test_utils.rs` / `shared/test_utils` — they're `#[cfg(test)]`-gated
(`source.rs:69-70`, `shared.rs:20-21`) and pull test scaffolding; write a minimal
standalone version here, using them as reference only.

```rust
pub(crate) struct ReplayHandle {
    tx: mpsc::Sender<InputEvent>,     // feed() pushes here
    acked: Arc<AtomicUsize>,
    fed: Arc<AtomicUsize>,
    shutdown: oneshot::Sender<()>,
    pub socket_path: PathBuf,
    pub server_info_path: PathBuf,
    server_task: JoinHandle<()>,
}
```

Implementation: a struct implementing `numaflow::source::Sourcer`
(`read(SourceReadRequest, tx)`, `ack(Vec<Offset>)`, `pending() -> usize`,
`partitions()`):

- Internal `Mutex<VecDeque<(offset: u64, InputEvent)>>` plus an in-flight map;
  `read` pops up to `request.count` entries (non-blocking beyond what's queued — return
  what's available; empty is fine), emitting SDK `Message { value, keys, offset:
  Offset::new(seq.to_be_bytes(), 0), event_time, headers }`.
- `ack` moves in-flight → acked (`acked.fetch_add`); `pending` = queued + in-flight.
- Serve with `numaflow::source::Server::new(replay).with_socket_file(sock)
  .with_server_info_file(info).start_with_shutdown(rx)` in a `tempfile::TempDir` owned
  by the handle (the SDK writes a valid server-info file itself, so pre-flight and
  `create_source` work unmodified). `tempfile` moves from dev-deps to an optional dep
  under `local-runner` (or use `std::env::temp_dir()` + pid-suffixed dir to avoid the
  dep — implementer's choice; tempfile is cleaner).
- Drain condition for transform/replay runs: `acked == fed` after feeding completes.

### 2.7 Facade tests (`numaflow-core`, `--features local-runner`)

In `local/runner.rs` `#[cfg(test)]`: in-process SDK servers (same pattern as
`reduce_forwarder.rs:625-635` — `numaflow::map::Server` etc. in a temp dir).

1. `test_local_map_roundtrip` — cat-mapper server; `LocalRun::start(Map)`; feed 3
   events; drain; `read_outputs()` returns 3 with matching payload/keys/event_time.
2. `test_local_map_flatmap_and_drop` — mapper emitting 2 results for one input and
   dropping another; assert counts (sent=2, results=2).
3. `test_local_sink` — sink server writing to a shared `Vec`; feed; drain; assert sink
   received all; `read_outputs()` empty.
4. `test_local_reduce_fixed` — port the assertions of `test_reduce_over_inmemory_isb`
   through the facade (counter reducer, 2 keys, one window) — this proves the facade
   is a faithful generalization of the template.
5. `test_local_transform` — replay source + event-time-reassigning transformer; assert
   new event times in outputs.
6. `test_drain_timeout` — mapper that always errors → expect
   `LocalError::Forwarder` (fatal path); and a sink that nacks one id forever if the
   SDK exposes that (else skip) → `DrainTimeout` naming the stuck id.
7. `test_startup_timeout` — nonexistent socket, 200ms startup timeout →
   `LocalError::Startup`.

CI: extend the rust test workflow to also run
`cargo test -p numaflow-core --features local-runner local::` (check
`.github/workflows/` for where `cargo test -p numaflow-core` runs today).

---

## Phase 3 — `rust/numaflow-cli` crate (`nfcli`)

### 3.1 Layout & Cargo.toml

```
numaflow-cli/
├── Cargo.toml
└── src/
    ├── main.rs        — clap dispatch, tracing init, env identity vars, exit-code mapping
    ├── cli.rs         —全 clap derive structs (below)
    ├── input/
    │   ├── mod.rs     — InputEvent assembly (file + inline merge), id auto-generation
    │   ├── yaml.rs    — multi-doc parser
    │   └── time.rs    — RFC3339 / "+dur" parsing, base-time logic
    ├── run/
    │   ├── map.rs sink.rs reduce.rs transform.rs source.rs   — per-subcommand drivers
    │   ├── sideinput.rs ready.rs                             — non-facade commands
    │   └── common.rs  — shared feed/drain/report loop
    └── output/
        ├── mod.rs     — Rendered summary model
        ├── text.rs json.rs raw.rs
```

```toml
[package]
name = "numaflow-cli"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "nfcli"
path = "src/main.rs"

[lints]
workspace = true

[dependencies]
numaflow-core = { path = "../numaflow-core", features = ["local-runner"] }
numaflow-pb.workspace = true        # side-input unary client only
tokio.workspace = true
tokio-util.workspace = true
tonic.workspace = true              # side-input channel
clap = { version = "4", features = ["derive"] }
serde.workspace = true
serde_yaml = "0.9"
serde_json.workspace = true
base64.workspace = true
bytes.workspace = true
chrono.workspace = true
humantime = "2"
thiserror.workspace = true
tracing.workspace = true
tracing-subscriber = "0.3"
```

(Check each `workspace = true` against the root `[workspace.dependencies]`; pin any
that aren't there.)

### 3.2 clap surface (`cli.rs`)

Global/common arg groups (defaults per design doc §3/§6):

```
--socket <PATH>                 (required for all UDF subcommands)
--server-info <PATH>            [default: derived — standard /var/run/numaflow/<type>-server-info
                                 when socket is under /var/run/numaflow, else <socket dir>/<type>-server-info]
--timeout <DUR>                 startup timeout [default: 30s]
--drain-timeout <DUR>           [default: 30s]
--max-message-size <BYTES>      [default: 64MiB]
--batch-size <N>                [default: 500]
--delay <DUR>                   [default: 0s]
--buffer-capacity <N>           [default: 30000]
-f, --file <PATH|->             YAML multi-doc stream; '-' = stdin
--payload <S> | --payload-file <P> | --payload-base64 <S>     (inline; ArgGroup, XOR with -f)
--key <K>... --header <K=V>... --event-time <T> --watermark <T> --id <S>
--base-time <RFC3339>
-o, --output <text|json|raw>    [default: text]
-v / -q
```

Subcommands and extras:

- `map` — `--mode <unary|batch|stream>` optional *assertion only*: after pre-flight,
  compare with `ServerInfo::get_map_mode()`; mismatch → usage error (exit 1).
- `transform` — no extras.
- `reduce` — `--window <fixed|sliding>` (required), `--length <DUR>` (required),
  `--slide <DUR>` (required iff sliding), `--allowed-lateness <DUR>` [default 0].
- `session-reduce` — `--gap <DUR>` (required).
- `accumulator` — `--timeout <DUR>` (required).
- `sink` — `--fallback-socket <P>` / `--fallback-server-info <P>`,
  `--on-success-socket <P>` / `--on-success-server-info <P>`.
- `source` — `--count <N>` [default 500], `--duration <DUR>`, `--pending`; no input
  flags.
- `side-input` — `--socket`/`--server-info` only.
- `ready <type>` — `--socket`/`--server-info`.

Validation rules (exit 1): exactly one input source among `-f`/inline payload flags
for data subcommands; inline flags meaningless for `source`; `repeat`/file allowed
everywhere else.

### 3.3 Input parsing (`input/`)

YAML per-document schema (v1 design §4.3 — unchanged), strict:

```rust
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventDoc {
    payload: Option<String>,
    payload_base64: Option<String>,
    payload_file: Option<PathBuf>,      // relative to the message file's directory
    #[serde(default)] keys: Vec<String>,
    #[serde(default)] headers: HashMap<String, String>,
    event_time: Option<String>,         // RFC3339 or "+dur"
    watermark: Option<String>,          // ditto; reduce-family only (warn elsewhere)
    id: Option<String>,
    #[serde(default)] user_metadata: HashMap<String, HashMap<String, String>>,
    previous_vertex: Option<String>,
    #[serde(default = "one")] repeat: usize,
}
```

- Multi-doc: `serde_yaml::Deserializer::from_str(&content)` iterator; error messages
  include the 1-based document index.
- Exactly one of the three payload fields (error otherwise); decode/read to `Bytes`.
- **Base time** (`input/time.rs`): `--base-time` else `Utc::now()`; for `reduce`
  truncate down to a multiple of `--length` since epoch; for `session-reduce`/
  `accumulator`/sliding, truncate to `--slide`/gap-free second. `+dur` = base +
  `humantime::parse_duration`. `event_time` default = base; `watermark` default = None
  (facade treats None as "= event_time" for the reduce driver).
- **Ids**: default `msg-<n>` (1-based post-`repeat` expansion). `repeat` copies get
  fresh auto-ids even when `id:` was explicit (`<id>-<k>` suffix) — ids must be unique
  (G2). Duplicate ids across the file = usage error (exit 1) — cheaper than debugging
  silent ISB dedup.

### 3.4 Subcommand drivers (`run/`)

`common.rs` — the shared engine loop:

```rust
let run = LocalRun::start(udf, opts).await?;              // Startup err → exit 2
for chunk in events.chunks(batch_size) {
    run.feed(chunk.to_vec()).await?;                      // Forwarder err → exit 4
    if delay > 0 { tokio::time::sleep(delay).await; }
}
let report = run.drain(drain_timeout).await;              // DrainTimeout → exit 3
let outputs = run.read_outputs().await?;
run.stop().await?;
render(outputs, report, opts.output_format);
```

- `reduce`/`session-reduce`/`accumulator`: identical, facade handles watermarks.
- `transform`: identical (feed goes to the replay source internally).
- `source`: no feed; loop `read_outputs()` printing incrementally until `--count`
  reached or `--duration` elapsed, then `stop()`. `--pending` calls the facade's
  passthrough to the source pending (expose `LocalRun::source_pending()` if trivial;
  otherwise print the replay of the pending gauge is *not* available — implementer may
  defer `--pending` to polish).
- `side-input` (`run/sideinput.rs`): no facade. Build a UDS tonic channel (copy the
  ~20-line connector from `numaflow-core/src/shared/grpc.rs:227` — it's `pub(crate)`,
  so replicate locally in the CLI) and call
  `numaflow_pb::clients::sideinput::side_input_client::SideInputClient::retrieve_side_input`.
  Print value + `no_broadcast`.
- `ready` (`run/ready.rs`): pre-flight only — socket wait + `sdk_server_info` is not
  needed; call the service's `IsReady` via the `numaflow_pb` client for the given type.
  Exit 0/2.

### 3.5 Output & exit codes (`output/`, `main.rs`)

- `text`: per-event line `[<id>] keys=[..] eventTime=<t> payload=<utf8-or-base64>`,
  then a summary line `sent=N · results=M · elapsed=…` (+ `dropped≈K` when `M <
  N·expected` is knowable, i.e. non-flatmap heuristics are NOT attempted — just print
  both numbers). Reduce: group by result keys.
- `json`: JSONL, one object per output event
  `{"type":"result","id":…,"keys":…,"eventTime":…,"payloadBase64":…,"headers":…}` plus
  a final `{"type":"summary",…}`.
- `raw`: concatenated payload bytes to stdout, diagnostics to stderr.
- `-v`: `tracing_subscriber::EnvFilter` set to
  `numaflow_core=debug,nfcli=debug` (default `warn`); this is the window into
  wire-level behavior (window ops, retries).
- Exit mapping in `main.rs`: `Ok` → 0 (or **4** if the drain report shows UDF-failed
  messages); clap/validation → 1 (clap default is 2 — override with
  `Command::error(ErrorKind::…)` styling or map manually; keep v1's contract: usage=1);
  `LocalError::Startup` → 2; `DrainTimeout` → 3; `Forwarder` → 4; `Internal` → 3.

---

## Phase 4 — Verification

```bash
cargo build -p numaflow-core                                   # feature off: unchanged
cargo build -p numaflow-core --features local-runner
cargo test  -p numaflow-core --features local-runner local::
cargo test  -p numaflow-core                                   # existing suite intact
cargo build -p numaflow-cli
cargo clippy --workspace --all-targets --features local-runner # match repo lint config
cd rust && make build                                          # repo's own build path
```

Manual e2e (documented in a `numaflow-cli/README.md`):

```bash
# Go SDK example mapper over UDS (writes /tmp socket + server-info via env overrides,
# or run in the SDK's default /var/run/numaflow — document both)
nfcli map --socket /var/run/numaflow/map.sock -f events.yaml
nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml --window fixed --length 60s
```

Add `numaflow-cli/e2e.sh` (not wired to CI initially): builds `nfcli`, spins up Rust
SDK example servers (source/map/reduce/sink from the pinned `numaflow-rs` rev) in temp
dirs, runs the subcommand matrix, asserts JSONL golden outputs.

---

## Appendix A — Verified reference index

| Symbol | File:Line |
|---|---|
| Template test `test_reduce_over_inmemory_isb` | `numaflow-core/src/pipeline/forwarder/reduce_forwarder.rs:617-862` |
| — config literal / factory / writer / KV / publisher | `:685-725 / :728 / :729-739 / :740 / :741-746` |
| — initial & terminal watermark, drain poll, output read | `:749-751, :815-823 / :800-812 / :814-837` |
| — `TERMINAL_WATERMARK_MS` | `:619` |
| `start_forwarder` (do NOT use — G1) | `pipeline/forwarder.rs:59` (factory created at `:63`) |
| `start_source/map/sink/reduce_forwarder` | `source_forwarder.rs:72` / `map_forwarder.rs:97` / `sink_forwarder.rs:83` / `reduce_forwarder.rs:490` |
| `InMemoryFactory` (`new`/`buffer_stats`/`get_or_create_buffer`/reader/writer/KV) | `pipeline/isb/inmemory/factory.rs:38 / 52 / 63 / 89 / 101 / 115` |
| `ISBFactory` trait | `pipeline/isb/factory.rs:28-92`; dispatch `create_isb_factory` `:95-109` |
| `ISBReader`/`ISBWriter` traits | `pipeline/isb.rs:42-85 / 150-190` |
| `ISBReaderRef`/`ISBWriterRef` | `pipeline/isb/dyn_adapter.rs:19-21` |
| In-memory adapter (eager BufferFull, id-dedup) | `pipeline/isb/inmemory/adapter.rs:216-268` |
| `Message` / `Default` / `MessageID` / proto encode (prev-vertex rewrite) | `message.rs:98-128 / 337-354 / 570-575 / 627-661` |
| `PipelineConfig` / `Default` | `config/pipeline.rs:61-83 / 91-120` |
| `VertexConfig`/`SourceVtxConfig`/`SinkVtxConfig`/map module/`ReduceVtxConfig` | `config/pipeline.rs:192-197 / 123-126 / 184-189 / 128-181 / 200-204` |
| `FromVertexConfig` / `ToVertexConfig` / `VertexType` | `config/pipeline.rs:247-251 / 289-299 / 254-259` |
| `ISBClientConfig` / `Stream` / writer & reader buffer configs | `config/pipeline/isb.rs:18-24 / 34-58 / 67-83 / 114-128` |
| `MetricsConfig` | `config/components/metrics.rs:7-31` |
| Watermark configs (`Edge`, `BucketConfig`) | `config/pipeline/watermark.rs:8-11, 183-199` |
| Reducer/window configs (reduce `UserDefinedConfig` = `&'static str`) | `config/components/reduce.rs:24-165` |
| Sink / Source / Transformer component configs | `components/sink.rs:30-44,160` / `components/source.rs:26-93,405-419` / `components/transformer.rs:7-27` |
| `ISBWatermarkPublisher` (`new`/`publish_watermark`) | `watermark/isb/wm_publisher.rs:78 / 98 / 133` |
| `create_mapper` (Phase-1 fix site) | `shared/create_components.rs:332` (override in UDS arm ~`:388-401`) |
| Socket consts (`DEFAULT_MAP_SOCKET` needs `pub(crate)`) | `config/pipeline.rs:46-49` |
| `sdk_server_info` / `read_server_info` | `numaflow-shared/src/server_info.rs:211 / 441` |
| UDS connector (replicate in CLI for side-input) | `shared/grpc.rs:227` |
| Env identity accessors (`get_vertex_name` etc.) | `config.rs:34-94` |
| cfg(test)-gated utils (reference only, do not depend on) | `source.rs:69-70`, `shared.rs:20-21`, `transformer.rs:31-32` |
| SDK dev-dep git pin (reuse for optional dep) | `numaflow-core/Cargo.toml:73` |
| lib.rs insertion point for `pub mod local` | `lib.rs:120` (after `mod reduce;`) |

## Appendix B — Gotcha checklist (each must be reflected in code review)

- **G1** Never call `pipeline::forwarder::start_forwarder` — it builds its own factory
  (empty registry). Use per-type functions with the shared `Arc<InMemoryFactory>`.
- **G2** `MessageID.to_string()` is the ISB dedup key (`adapter.rs` uses it as write
  id). Every fed event needs a unique id; `repeat` expansion must re-suffix; reject
  duplicate ids at parse time.
- **G3** `InMemoryFactory::get_or_create_buffer` is first-creation-wins for capacity —
  create the input writer (with the CLI's `--buffer-capacity`) *before* spawning the
  forwarder.
- **G4** Reduce's `UserDefinedConfig` fields are `&'static str` → `Box::leak` the
  socket/server-info strings (once per process; fine). Map/sink/source/transformer
  configs use `String` — no leak.
- **G5** `metrics_server_listen_port: 0` always (metrics server is fire-and-forget and
  non-fatal, `shared/metrics.rs:13-28`, but don't squat 2469).
- **G6** `Stream`, `FromVertexConfig.name`, `BucketConfig` fields are `&'static str` —
  use the fixed module consts; never format!-and-leak per message.
- **G7** The facade's direct `input_writer.write()` gets **eager** `BufferFull` from
  the in-memory adapter; retry with sleep — the orchestrator's retry machinery does not
  cover this path.
- **G8** Always ack messages fetched from the output buffer, else the next fetch
  redelivers them (fetch marks in-flight; nothing times it out — in-memory has no
  ack-wait redelivery).
- **G9** Reduce terminal watermark must be **re-published in the drain loop** with
  `is_idle=true` at `last_offset + 1` — a single publish can be missed while the
  processor registers (test comment at `reduce_forwarder.rs:819-820`).
- **G10** Drain = `pending == 0 && in_flight == 0` on the input buffer (ack implies the
  downstream write completed — that ordering is the production loop's contract).
- **G11** `select!` on the forwarder `JoinHandle` in every wait loop (feed retries,
  drain, output polling) — fatal UDF errors must surface immediately as exit 4, not as
  a drain timeout.
- **G12** Pre-flight `sdk_server_info` with a *dedicated* token + `tokio::time::timeout`
  — the underlying `read_server_info` polls forever and only exits on cancellation.
- **G13** Message-metadata `previous_vertex` is rewritten on ISB encode to the writing
  vertex's name (`message.rs:~644`); honoring a user-supplied `previousVertex` means
  naming the input vertex accordingly (leak once) — or documenting it as fixed
  `nfcli-in` (implementer may start with the simpler fixed name and a warning).
- **G14** Set `NUMAFLOW_VERTEX_NAME`/`NUMAFLOW_PIPELINE_NAME` in `main()` before any
  facade call — the `OnceLock` accessors freeze on first read.
- **G15** In-memory nack `delay` is ignored (immediate redelivery) and there's no WIP
  auto-redelivery — document in README; drain-timeout is the poison-message backstop.
- **G16** Feature must stay default-off; `cargo build -p numaflow-core` (no features)
  must produce a byte-identical dependency graph (no `numaflow` SDK).
- **G17** Tags are not persisted through the ISB and drops happen inside
  `ISBWriteTask` before any writer call — do not attempt to surface per-message
  tags/drops; summary counts only (design decision, resolved 2026-08-04).
