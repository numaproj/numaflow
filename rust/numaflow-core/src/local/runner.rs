//! [`LocalRun`] — the lifecycle of one local UDF run: start → feed → drain → read_outputs → stop.
//!
//! The single most important invariant is gotcha **G1**: we spawn the *per-type* forwarder
//! (`start_map_forwarder`, …) passing the *shared* `Arc<InMemoryFactory>`. We must NOT call the
//! top-level `pipeline::forwarder::start_forwarder`, which builds its own fresh (empty)
//! `InMemoryFactory` — the forwarder would then read from a different, eternally-empty buffer.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::pipeline::VertexConfig;
use crate::config::pipeline::isb::{BufferWriterConfig, Stream};
use crate::local::config_builder::{
    self, BuildOpts, INPUT_STREAM, INPUT_VERTEX, OUTPUT_STREAM, OUTPUT_VERTEX, RunKind,
};
use crate::local::events::{
    InputEvent, OutputEvent, input_event_to_message, message_to_output_event,
};
use crate::local::replay_source::ReplayHandle;
use crate::local::watermark_driver::WatermarkDriver;
use crate::local::{LocalError, LocalUdf, Result};
use crate::message::MessageType;
use crate::message::Offset;
use crate::pipeline::forwarder::map_forwarder::start_map_forwarder;
use crate::pipeline::forwarder::reduce_forwarder::start_reduce_forwarder;
use crate::pipeline::forwarder::sink_forwarder::start_sink_forwarder;
use crate::pipeline::forwarder::source_forwarder::start_source_forwarder;
use crate::pipeline::isb::WriteError;
use crate::pipeline::isb::dyn_adapter::{ISBReaderRef, ISBWriterRef};
use crate::pipeline::isb::factory::ISBFactory;
use crate::pipeline::isb::inmemory::InMemoryFactory;

/// How often the feed/drain/output loops poll while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// Backoff between buffer-full write retries.
const BUFFER_FULL_BACKOFF: Duration = Duration::from_millis(10);
/// Per-fetch batch size when reading the output buffer. Bounded so a huge configured buffer
/// capacity does not translate into an enormous single fetch.
const OUTPUT_FETCH_BATCH: usize = 500;

/// Tuning knobs for a local run. All fields have sensible defaults for a test tool.
#[derive(Debug, Clone, Copy)]
pub struct LocalRunOpts {
    /// Forwarder read batch size and CLI feed-chunk size. Default 500.
    pub batch_size: usize,
    /// Input/output buffer capacity. Default 30_000 (`DEFAULT_MAX_LENGTH`).
    pub buffer_capacity: usize,
    /// gRPC max send/recv message size. Default 64 MiB.
    pub grpc_max_message_size: usize,
    /// Startup wait for each UDF's socket + server-info. Default 30s.
    pub startup_timeout: Duration,
    /// Graceful shutdown window handed to the forwarder. Default 5s.
    pub graceful_shutdown: Duration,
}

impl Default for LocalRunOpts {
    fn default() -> Self {
        Self {
            batch_size: 500,
            buffer_capacity: 30_000,
            grpc_max_message_size: 64 * 1024 * 1024,
            startup_timeout: Duration::from_secs(30),
            graceful_shutdown: Duration::from_secs(5),
        }
    }
}

/// Summary of a drain: how many events were fed and how many the forwarder finished with, plus any
/// still-stuck count (non-zero only on a drain timeout).
#[derive(Debug, Clone)]
pub struct DrainReport {
    pub fed: usize,
    /// Events still pending/in-flight at drain end (0 on a clean drain).
    pub stuck: usize,
}

/// A running local UDF test. Not `Clone` — it owns the forwarder task and buffers.
pub struct LocalRun {
    factory: Arc<InMemoryFactory>,
    /// Input writer — `None` for Transform/Source (they feed via the replay source instead).
    input_writer: Option<ISBWriterRef>,
    /// Cached output reader — `None` until first `read_outputs`.
    output_reader: Option<ISBReaderRef>,
    /// Replay source — `Some` for Transform (and Source, which serves its own but we don't own it).
    replay: Option<ReplayHandle>,
    /// Watermark driver — `Some` for Reduce.
    watermark: Option<WatermarkDriver>,
    cln_token: CancellationToken,
    forwarder: JoinHandle<crate::error::Result<()>>,
    kind: RunKind,
    graceful_shutdown: Duration,
    /// Total events fed so far (for the drain report).
    fed: usize,
}

impl LocalRun {
    /// Start a local run: pre-flight the UDF socket(s), create the shared factory and input writer,
    /// build the watermark driver / replay source as needed, then spawn the production forwarder.
    pub async fn start(udf: LocalUdf, opts: LocalRunOpts) -> Result<LocalRun> {
        let cln_token = CancellationToken::new();

        // 1. Pre-flight every UDF socket in play. This gives clean `Startup` (exit-2) semantics
        //    instead of hanging inside the forwarder waiting on a socket that will never appear.
        preflight(&udf, opts.startup_timeout).await?;

        // 2. Shared factory — this is the CLI's handle to the buffers the forwarder also uses.
        let factory = Arc::new(InMemoryFactory::new());

        let build_opts = BuildOpts {
            batch_size: opts.batch_size,
            buffer_capacity: opts.buffer_capacity,
            grpc_max_message_size: opts.grpc_max_message_size,
            graceful_shutdown: opts.graceful_shutdown,
        };
        let (mut config, kind) = config_builder::build(&udf, &build_opts);

        // 3. For input-consuming kinds create the input writer FIRST (with the CLI's capacity),
        //    before the forwarder can race to create the buffer with default capacity —
        //    `get_or_create_buffer` is first-creation-wins (G3).
        let input_writer = if matches!(kind, RunKind::Map | RunKind::Sink | RunKind::Reduce) {
            let input_stream = Stream::new(INPUT_STREAM, INPUT_VERTEX, 0);
            let writer = factory
                .create_writer(
                    input_stream.clone(),
                    BufferWriterConfig {
                        streams: vec![input_stream],
                        max_length: opts.buffer_capacity,
                        ..Default::default()
                    },
                    None,
                    cln_token.clone(),
                )
                .await
                .map_err(|e| LocalError::Internal(format!("failed to create input writer: {e}")))?;
            Some(writer)
        } else {
            None
        };

        // 4. Reduce: build the watermark driver (creates the OT KV store + publisher, publishes the
        //    floor watermark). The floor is a fixed low bound so real events are never late-dropped.
        let watermark = if kind == RunKind::Reduce {
            let driver = WatermarkDriver::new(&factory).await.map_err(|e| {
                LocalError::Internal(format!("failed to build watermark driver: {e}"))
            })?;
            Some(driver)
        } else {
            None
        };

        // 5. Transform: start the replay source and patch its socket into the source config (the
        //    config builder left a placeholder since the path is only known now).
        let replay = if kind == RunKind::Transform {
            let handle = ReplayHandle::start()?;
            // Pre-flight the replay source too — it must be up before the source forwarder connects.
            preflight_socket(
                &handle.socket_path,
                &handle.server_info_path,
                opts.startup_timeout,
            )
            .await?;
            patch_replay_source(&mut config, &handle, opts.grpc_max_message_size);
            Some(handle)
        } else {
            None
        };

        // 6. Spawn the per-type forwarder with the SHARED factory (G1). All four entry points take
        //    the same `(token, Arc<dyn ISBFactory>, PipelineConfig, per-type config)` shape.
        let f: Arc<dyn ISBFactory> = Arc::clone(&factory) as Arc<dyn ISBFactory>;
        let cln = cln_token.clone();
        let forwarder = spawn_forwarder(kind, cln, f, config)?;

        Ok(LocalRun {
            factory,
            input_writer,
            output_reader: None,
            replay,
            watermark,
            cln_token,
            forwarder,
            kind,
            graceful_shutdown: opts.graceful_shutdown,
            fed: 0,
        })
    }

    /// Feed events into the run. For Map/Sink/Reduce this writes into the input buffer (and, for
    /// reduce, publishes each event's watermark after the write). For Transform it hands events to
    /// the replay source. Not applicable to plain Source.
    pub async fn feed(&mut self, events: Vec<InputEvent>) -> Result<()> {
        self.fed += events.len();

        if let Some(replay) = &self.replay {
            // Transform: the source forwarder pulls from the replay queue.
            replay.push(events);
            return Ok(());
        }

        let Some(writer) = &self.input_writer else {
            return Err(LocalError::Internal(
                "feed() called on a run with no input writer (source kind?)".to_string(),
            ));
        };

        for event in events {
            // Reduce needs the event again after the write to publish its watermark; keep a light
            // clone of just the fields the driver reads.
            let wm_event = if self.watermark.is_some() {
                Some(event.clone())
            } else {
                None
            };

            let write_result = self.write_with_retry(writer, event).await?;

            if let (Some(driver), Some(ev)) = (self.watermark.as_mut(), wm_event) {
                let Offset::Int(int_offset) = &write_result else {
                    return Err(LocalError::Internal(
                        "in-memory writer must return an integer offset".to_string(),
                    ));
                };
                driver.on_written(int_offset.offset, &ev).await;
            }
        }
        Ok(())
    }

    /// Write one event, retrying on the in-memory adapter's *eager* `BufferFull` (the orchestrator
    /// retry machinery does not cover this direct write path — G7). Returns the assigned offset.
    async fn write_with_retry(&self, writer: &ISBWriterRef, event: InputEvent) -> Result<Offset> {
        let message = input_event_to_message(event);
        loop {
            // The forwarder dying mid-feed must surface immediately, not as a hang (G11). We check
            // the token and the (non-consuming) forwarder liveness between attempts.
            if self.cln_token.is_cancelled() || self.forwarder.is_finished() {
                return Err(LocalError::Forwarder(
                    "forwarder stopped while feeding input".to_string(),
                ));
            }
            match writer.write(message.clone()).await {
                Ok(result) => return Ok(result.offset),
                Err(WriteError::BufferFull) => {
                    tokio::time::sleep(BUFFER_FULL_BACKOFF).await;
                }
                Err(WriteError::WriteFailed(msg)) => {
                    return Err(LocalError::Internal(format!("input write failed: {msg}")));
                }
            }
        }
    }

    /// Wait for the run to drain, bounded by `timeout`.
    ///
    /// Completion signal depends on kind:
    /// - Map/Sink/Reduce: the input buffer is fully acked (`pending == 0 && in_flight == 0`). Ack
    ///   implies the downstream write completed — that ordering is the production loop's contract
    ///   (G10). Reduce additionally re-publishes the terminal idle watermark each poll (G9).
    /// - Transform: the replay source reports all fed offsets acked.
    /// - Source: there is no drain; the CLI stops by count/duration (returns immediately here).
    ///
    /// A fatal forwarder error surfaces as [`LocalError::Forwarder`] (exit 4) via the `select!` on
    /// the forwarder handle (G11).
    pub async fn drain(&mut self, timeout: Duration) -> Result<DrainReport> {
        // Plain source has no input to drain.
        if self.kind == RunKind::Source {
            return Ok(DrainReport {
                fed: self.fed,
                stuck: 0,
            });
        }

        let deadline = tokio::time::Instant::now() + timeout;
        // Reduce needs an extra "settle" once the input drains: the input buffer empties almost
        // immediately (the PBQ acks without a WAL), but the window only closes after the terminal
        // watermark advances through it. We track when the input drained and how many consecutive
        // polls the output buffer's pending count has been non-zero and unchanged (window closed,
        // results written and stable). See G9 / plan §2.5.
        let mut input_drained = false;
        let mut last_output_pending = 0usize;
        let mut stable_polls = 0u32;
        const REQUIRED_STABLE_POLLS: u32 = 3;

        loop {
            if tokio::time::Instant::now() >= deadline {
                let stuck = self.stuck_count();
                return Err(LocalError::DrainTimeout(stuck, self.fed));
            }

            // Watch the forwarder for a fatal error the whole time.
            tokio::select! {
                res = &mut self.forwarder => {
                    return Err(forwarder_join_error(res));
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {
                    if self.kind == RunKind::Reduce {
                        // Keep re-publishing the terminal watermark so the windower closes and the
                        // synthetic upstream processor stays alive (G9). This must continue *after*
                        // the input drains, until the window has actually closed.
                        if let Some(driver) = self.watermark.as_mut() {
                            driver.publish_terminal().await;
                        }

                        if !input_drained {
                            input_drained = self.is_drained();
                        }
                        if input_drained {
                            // Wait for the output buffer to have produced results and then hold
                            // steady across a few polls (the window has closed and nothing more is
                            // being written).
                            let output_pending = self
                                .factory
                                .buffer_stats(OUTPUT_STREAM)
                                .map(|(pending, _in_flight)| pending)
                                .unwrap_or(0);
                            if output_pending > 0 && output_pending == last_output_pending {
                                stable_polls += 1;
                                if stable_polls >= REQUIRED_STABLE_POLLS {
                                    return Ok(DrainReport { fed: self.fed, stuck: 0 });
                                }
                            } else {
                                stable_polls = 0;
                            }
                            last_output_pending = output_pending;
                        }
                    } else if self.is_drained() {
                        return Ok(DrainReport { fed: self.fed, stuck: 0 });
                    }
                }
            }
        }
    }

    /// Whether the input side is fully drained for the current kind.
    fn is_drained(&self) -> bool {
        match self.kind {
            RunKind::Map | RunKind::Sink | RunKind::Reduce => self
                .factory
                .buffer_stats(INPUT_STREAM)
                // No buffer yet → nothing written → trivially drained.
                .map(|(pending, in_flight)| pending == 0 && in_flight == 0)
                .unwrap_or(true),
            RunKind::Transform => self.replay.as_ref().is_some_and(|r| r.is_drained()),
            RunKind::Source => true,
        }
    }

    /// Count of events still pending/in-flight (for the drain-timeout report).
    fn stuck_count(&self) -> usize {
        match self.kind {
            RunKind::Map | RunKind::Sink | RunKind::Reduce => self
                .factory
                .buffer_stats(INPUT_STREAM)
                .map(|(pending, in_flight)| pending + in_flight)
                .unwrap_or(0),
            RunKind::Transform => self
                .replay
                .as_ref()
                .map(|r| {
                    let (acked, fed) = r.progress();
                    fed.saturating_sub(acked)
                })
                .unwrap_or(0),
            RunKind::Source => 0,
        }
    }

    /// Read all currently-available output events. Fetches in batches until an empty fetch, acking
    /// every fetched message before returning (un-acked messages would be redelivered by the next
    /// fetch — G8). Callable repeatedly (the source kind polls it while running).
    pub async fn read_outputs(&mut self) -> Result<Vec<OutputEvent>> {
        // Create (and cache) the output reader on first use.
        if self.output_reader.is_none() {
            let output_stream = Stream::new(OUTPUT_STREAM, OUTPUT_VERTEX, 0);
            let reader = self
                .factory
                .create_reader(output_stream, None)
                .await
                .map_err(|e| {
                    LocalError::Internal(format!("failed to create output reader: {e}"))
                })?;
            self.output_reader = Some(reader);
        }
        let reader = self
            .output_reader
            .as_ref()
            .expect("output reader was just set");

        let mut outputs = Vec::new();
        loop {
            let batch = reader
                .fetch(OUTPUT_FETCH_BATCH, Duration::from_millis(100))
                .await
                .map_err(|e| LocalError::Internal(format!("output fetch failed: {e}")))?;
            if batch.is_empty() {
                break;
            }
            for message in batch {
                // Always ack (even control messages), else the next fetch redelivers them (G8).
                reader
                    .ack(&message.offset)
                    .await
                    .map_err(|e| LocalError::Internal(format!("output ack failed: {e}")))?;
                // Skip WMB (watermark control) messages — they are not UDF results. The reduce
                // forwarder writes a WMB to the output edge; surfacing it as an empty-keys result
                // would corrupt the output.
                if message.typ == MessageType::Data {
                    outputs.push(message_to_output_event(message));
                }
            }
        }
        Ok(outputs)
    }

    /// Passthrough to the source/replay pending gauge, if available.
    pub fn source_pending(&self) -> Option<usize> {
        self.replay.as_ref().map(|r| {
            let (acked, fed) = r.progress();
            fed.saturating_sub(acked)
        })
    }

    /// Cancel the run and join the forwarder (production graceful shutdown). Also shuts down the
    /// replay source server if any.
    pub async fn stop(mut self) -> Result<()> {
        self.cln_token.cancel();

        // Give the forwarder graceful_shutdown + a slack window to wind down.
        let join = tokio::time::timeout(
            self.graceful_shutdown + Duration::from_secs(5),
            &mut self.forwarder,
        );
        let result = match join.await {
            Ok(joined) => forwarder_join_result(joined),
            Err(_) => Err(LocalError::Internal(
                "forwarder did not stop within the graceful shutdown window".to_string(),
            )),
        };

        if let Some(replay) = self.replay.as_mut() {
            replay.shutdown();
        }

        result
    }
}

/// Spawn the correct per-type forwarder. Kept separate so `start` reads cleanly.
fn spawn_forwarder(
    kind: RunKind,
    cln: CancellationToken,
    factory: Arc<dyn ISBFactory>,
    config: crate::config::pipeline::PipelineConfig,
) -> Result<JoinHandle<crate::error::Result<()>>> {
    // Each entry point wants its own per-type config, which we extract from the built
    // `PipelineConfig.vertex_config`. The config builder guarantees the matching variant per kind.
    let handle = match kind {
        RunKind::Map => {
            let VertexConfig::Map(map_cfg) = config.vertex_config.clone() else {
                return Err(LocalError::Internal(
                    "expected map vertex config".to_string(),
                ));
            };
            tokio::spawn(start_map_forwarder(cln, factory, config, map_cfg))
        }
        RunKind::Sink => {
            let VertexConfig::Sink(sink_cfg) = config.vertex_config.clone() else {
                return Err(LocalError::Internal(
                    "expected sink vertex config".to_string(),
                ));
            };
            tokio::spawn(start_sink_forwarder(cln, factory, config, *sink_cfg))
        }
        RunKind::Reduce => {
            let VertexConfig::Reduce(reduce_cfg) = config.vertex_config.clone() else {
                return Err(LocalError::Internal(
                    "expected reduce vertex config".to_string(),
                ));
            };
            tokio::spawn(start_reduce_forwarder(cln, factory, config, reduce_cfg))
        }
        RunKind::Transform | RunKind::Source => {
            let VertexConfig::Source(src_cfg) = config.vertex_config.clone() else {
                return Err(LocalError::Internal(
                    "expected source vertex config".to_string(),
                ));
            };
            tokio::spawn(start_source_forwarder(cln, factory, config, src_cfg))
        }
    };
    Ok(handle)
}

/// Patch the replay source's real socket/server-info into the (Transform) source config.
fn patch_replay_source(
    config: &mut crate::config::pipeline::PipelineConfig,
    handle: &ReplayHandle,
    grpc_max_message_size: usize,
) {
    use crate::config::components::source::{SourceType, UserDefinedConfig as SourceUdConfig};
    if let VertexConfig::Source(src) = &mut config.vertex_config {
        src.source_config.source_type = SourceType::UserDefined(SourceUdConfig {
            grpc_max_message_size,
            socket_path: handle.socket_path.to_string_lossy().into_owned(),
            server_info_path: handle.server_info_path.to_string_lossy().into_owned(),
        });
    }
}

/// Map a `JoinHandle` result (from `select!`, where the future yields the inner `Result`) into a
/// `LocalError::Forwarder`. This branch is only taken when the forwarder finished — either an error
/// or an unexpected early clean exit.
fn forwarder_join_error(
    res: std::result::Result<crate::error::Result<()>, tokio::task::JoinError>,
) -> LocalError {
    match forwarder_join_result(res) {
        Ok(()) => LocalError::Forwarder("forwarder exited before drain completed".to_string()),
        Err(e) => e,
    }
}

/// Turn a joined forwarder result into a facade `Result`, mapping the internal error at the
/// boundary (the internal `Error` type is never exposed publicly).
fn forwarder_join_result(
    res: std::result::Result<crate::error::Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match res {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(LocalError::Forwarder(e.to_string())),
        Err(join_err) => Err(LocalError::Internal(format!(
            "forwarder task panicked: {join_err}"
        ))),
    }
}

/// Pre-flight every UDF socket the run connects to, so a missing/late server fails fast with a
/// clear `Startup` error instead of hanging inside the forwarder.
async fn preflight(udf: &LocalUdf, timeout: Duration) -> Result<()> {
    match udf {
        LocalUdf::Map {
            socket_path,
            server_info_path,
        }
        | LocalUdf::Reduce {
            socket_path,
            server_info_path,
            ..
        }
        | LocalUdf::Source {
            socket_path,
            server_info_path,
        } => preflight_socket(socket_path, server_info_path, timeout).await,
        LocalUdf::Transform {
            socket_path,
            server_info_path,
        } => {
            // The transformer must be up; the replay source is pre-flighted separately once started.
            preflight_socket(socket_path, server_info_path, timeout).await
        }
        LocalUdf::Sink {
            socket_path,
            server_info_path,
            fallback,
            on_success,
        } => {
            preflight_socket(socket_path, server_info_path, timeout).await?;
            if let Some((s, i)) = fallback {
                preflight_socket(s, i, timeout).await?;
            }
            if let Some((s, i)) = on_success {
                preflight_socket(s, i, timeout).await?;
            }
            Ok(())
        }
    }
}

/// Wait for one socket to exist and its server-info to be readable, bounded by `timeout`.
///
/// `sdk_server_info` polls forever and only exits on token cancellation (G12), so we drive it with
/// a dedicated token cancelled by a `tokio::time::timeout`.
async fn preflight_socket(
    socket_path: &std::path::Path,
    server_info_path: &std::path::Path,
    timeout: Duration,
) -> Result<()> {
    let token = CancellationToken::new();
    let info_path = server_info_path.to_path_buf();
    let token_for_call = token.clone();

    let result = tokio::time::timeout(timeout, async move {
        numaflow_shared::server_info::sdk_server_info(info_path, token_for_call).await
    })
    .await;

    match result {
        Ok(Ok(_server_info)) => Ok(()),
        Ok(Err(e)) => Err(LocalError::Startup(format!(
            "server-info at {} is invalid: {e}",
            server_info_path.display()
        ))),
        Err(_elapsed) => {
            // Cancel the (still-polling) read so it does not leak past the timeout.
            token.cancel();
            Err(LocalError::Startup(format!(
                "server-info not found at {} (socket {}) — is the UDF server running?",
                server_info_path.display(),
                socket_path.display()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use tokio::sync::mpsc;

    use numaflow::shared::ServerExtras;

    use crate::local::events::InputEvent;
    use crate::local::{LocalError, LocalRun, LocalRunOpts, LocalUdf, LocalWindow};
    use crate::shared::test_utils::server::{TestServerHandle, start_server};

    /// Build an `InputEvent` with a unique id and the given payload/keys/event_time.
    fn event(id: &str, payload: &str, keys: &[&str], secs: i64) -> InputEvent {
        InputEvent {
            payload: Bytes::from(payload.to_string()),
            keys: keys.iter().map(|k| k.to_string()).collect(),
            event_time: Utc.timestamp_opt(secs, 0).unwrap(),
            id: id.to_string(),
            ..Default::default()
        }
    }

    fn opts() -> LocalRunOpts {
        LocalRunOpts {
            // Short startup timeout keeps failing tests fast.
            startup_timeout: Duration::from_secs(10),
            ..Default::default()
        }
    }

    // ---- SDK server handlers used by the tests ----

    /// A mapper that returns its input unchanged (cat).
    struct CatMapper;
    #[tonic::async_trait]
    impl numaflow::map::Mapper for CatMapper {
        async fn map(&self, input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
            vec![numaflow::map::Message::new(input.value).with_keys(input.keys)]
        }
    }

    /// A mapper that flat-maps: for a "dup" payload emits two results; for "drop" emits nothing;
    /// otherwise emits one.
    struct FlatMapper;
    #[tonic::async_trait]
    impl numaflow::map::Mapper for FlatMapper {
        async fn map(&self, input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
            match input.value.as_slice() {
                b"dup" => vec![
                    numaflow::map::Message::new(b"a".to_vec()),
                    numaflow::map::Message::new(b"b".to_vec()),
                ],
                b"drop" => vec![],
                other => vec![numaflow::map::Message::new(other.to_vec())],
            }
        }
    }

    /// A mapper that always errors (fatal path → forwarder exits with error).
    struct FailMapper;
    #[tonic::async_trait]
    impl numaflow::map::Mapper for FailMapper {
        async fn map(&self, _input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
            panic!("mapper intentionally failing");
        }
    }

    /// A sink that records every value it receives into a shared Vec and acks all.
    struct CollectingSink {
        received: Arc<StdMutex<Vec<Vec<u8>>>>,
    }
    #[tonic::async_trait]
    impl numaflow::sink::Sinker for CollectingSink {
        async fn sink(
            &self,
            mut input: mpsc::Receiver<numaflow::sink::SinkRequest>,
        ) -> Vec<numaflow::sink::Response> {
            let mut responses = Vec::new();
            while let Some(req) = input.recv().await {
                self.received.lock().unwrap().push(req.value.clone());
                responses.push(numaflow::sink::Response::ok(req.id));
            }
            responses
        }
    }

    /// A transformer that reassigns event time to a fixed value and passes payload through.
    struct RetimeTransformer {
        new_event_secs: i64,
    }
    #[tonic::async_trait]
    impl numaflow::sourcetransform::SourceTransformer for RetimeTransformer {
        async fn transform(
            &self,
            input: numaflow::sourcetransform::SourceTransformRequest,
        ) -> Vec<numaflow::sourcetransform::Message> {
            let new_time = Utc.timestamp_opt(self.new_event_secs, 0).unwrap();
            vec![
                numaflow::sourcetransform::Message::new(input.value, new_time)
                    .with_keys(input.keys)
                    .with_tags(vec![]),
            ]
        }
    }

    /// A reducer that counts the elements in a window (mirrors the template test's counter).
    struct Counter;
    struct CounterCreator;
    impl numaflow::reduce::ReducerCreator for CounterCreator {
        type R = Counter;
        fn create(&self) -> Self::R {
            Counter
        }
    }
    #[tonic::async_trait]
    impl numaflow::reduce::Reducer for Counter {
        async fn reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<numaflow::reduce::ReduceRequest>,
            _md: &numaflow::reduce::Metadata,
        ) -> Vec<numaflow::reduce::Message> {
            let mut count = 0u64;
            while input.recv().await.is_some() {
                count += 1;
            }
            vec![numaflow::reduce::Message::new(count.to_string().into_bytes()).with_keys(keys)]
        }
    }

    /// Helper to grab a server's socket/server-info paths.
    fn paths(server: &TestServerHandle) -> (PathBuf, PathBuf) {
        (server.socket_path(), server.server_info_path())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_map_roundtrip() {
        let server = start_server("mapper", |sock, info, shutdown| async move {
            numaflow::map::Server::new(CatMapper)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown)
                .await
                .expect("map server failed");
        });
        let (socket_path, server_info_path) = paths(&server);

        let mut run = LocalRun::start(
            LocalUdf::Map {
                socket_path,
                server_info_path,
            },
            opts(),
        )
        .await
        .expect("start map run");

        run.feed(vec![
            event("m-1", "hello", &["k1"], 10),
            event("m-2", "world", &["k2"], 20),
            event("m-3", "again", &["k1"], 30),
        ])
        .await
        .expect("feed");

        run.drain(Duration::from_secs(10)).await.expect("drain");
        let mut outputs = run.read_outputs().await.expect("read outputs");
        run.stop().await.expect("stop");

        assert_eq!(outputs.len(), 3, "cat mapper should echo all 3 events");
        outputs.sort_by_key(|e| e.event_time);
        let first = outputs.first().expect("at least one output");
        assert_eq!(first.payload, Bytes::from("hello"));
        assert_eq!(first.keys, vec!["k1".to_string()]);
        assert_eq!(first.event_time.timestamp(), 10);
        // Detach the SDK server thread instead of joining (join can hang on shutdown — see
        // test_local_sink). The daemon thread dies with the process at test end.
        std::mem::forget(server);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_map_flatmap_and_drop() {
        // NOTE: the server name determines the server-info basename, which `ContainerType::from`
        // parses to run version-compat checks — so it MUST be a recognized container name
        // ("mapper", not "flatmapper"), else pre-flight fails with "container type: unknown".
        let server = start_server("mapper", |sock, info, shutdown| async move {
            numaflow::map::Server::new(FlatMapper)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown)
                .await
                .expect("map server failed");
        });
        let (socket_path, server_info_path) = paths(&server);

        let mut run = LocalRun::start(
            LocalUdf::Map {
                socket_path,
                server_info_path,
            },
            opts(),
        )
        .await
        .expect("start map run");

        // 1 "dup" (→2 results) + 1 "drop" (→0) + 1 "keep" (→1) = 3 results.
        run.feed(vec![
            event("m-1", "dup", &[], 10),
            event("m-2", "drop", &[], 20),
            event("m-3", "keep", &[], 30),
        ])
        .await
        .expect("feed");

        run.drain(Duration::from_secs(10)).await.expect("drain");
        let outputs = run.read_outputs().await.expect("read outputs");
        run.stop().await.expect("stop");

        assert_eq!(outputs.len(), 3, "expected 2 (dup) + 0 (drop) + 1 (keep)");
        // Detach the SDK server thread instead of joining (join can hang on shutdown — see
        // test_local_sink). The daemon thread dies with the process at test end.
        std::mem::forget(server);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_sink() {
        let received = Arc::new(StdMutex::new(Vec::new()));
        let received_for_server = Arc::clone(&received);
        let server = start_server("sinker", move |sock, info, shutdown| {
            let received = Arc::clone(&received_for_server);
            async move {
                numaflow::sink::Server::new(CollectingSink { received })
                    .with_socket_file(sock)
                    .with_server_info_file(info)
                    .start_with_shutdown(shutdown)
                    .await
                    .expect("sink server failed");
            }
        });
        let (socket_path, server_info_path) = paths(&server);

        let mut run = LocalRun::start(
            LocalUdf::Sink {
                socket_path,
                server_info_path,
                fallback: None,
                on_success: None,
            },
            opts(),
        )
        .await
        .expect("start sink run");

        run.feed(vec![
            event("s-1", "one", &[], 10),
            event("s-2", "two", &[], 20),
        ])
        .await
        .expect("feed");

        run.drain(Duration::from_secs(10)).await.expect("drain");
        let outputs = run.read_outputs().await.expect("read outputs");
        run.stop().await.expect("stop");

        // Sink is terminal — no output buffer events.
        assert!(outputs.is_empty(), "sink run produces no output events");
        let got = received.lock().unwrap().len();
        assert_eq!(got, 2, "sink should have received both events");
        // The SDK sink server thread does not exit on the shutdown signal (unlike the map server),
        // so joining it hangs (a documented `TestServerHandle` hazard). Detach instead of joining —
        // the daemon thread dies with the process at test end.
        std::mem::forget(server);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_reduce_fixed() {
        let server = start_server("reducer", |sock, info, shutdown| async move {
            numaflow::reduce::Server::new(CounterCreator)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown)
                .await
                .expect("reduce server failed");
        });
        let (socket_path, server_info_path) = paths(&server);

        // All events fall inside one 60s window from t=0. Two keys: key1 x3, key2 x2.
        let mut run = LocalRun::start(
            LocalUdf::Reduce {
                socket_path,
                server_info_path,
                window: LocalWindow::Fixed {
                    length: Duration::from_secs(60),
                },
                keyed: true,
                allowed_lateness: Duration::ZERO,
            },
            opts(),
        )
        .await
        .expect("start reduce run");

        run.feed(vec![
            event("r-1", "v", &["key1"], 10),
            event("r-2", "v", &["key2"], 20),
            event("r-3", "v", &["key1"], 30),
            event("r-4", "v", &["key2"], 40),
            event("r-5", "v", &["key1"], 50),
        ])
        .await
        .expect("feed");

        run.drain(Duration::from_secs(15)).await.expect("drain");

        // The window closes during drain (the terminal watermark is published there); read the
        // results back. Both keys' results are written together.
        let mut results: HashMap<String, Vec<u8>> = HashMap::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while results.len() < 2 && tokio::time::Instant::now() < deadline {
            for out in run.read_outputs().await.expect("read outputs") {
                if let Some(key) = out.keys.first() {
                    results.insert(key.clone(), out.payload.to_vec());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        run.stop().await.expect("stop");

        assert_eq!(
            results,
            HashMap::from([
                ("key1".to_string(), b"3".to_vec()),
                ("key2".to_string(), b"2".to_vec()),
            ]),
            "each key should reduce to its element count"
        );
        // Detach the SDK server thread instead of joining (join can hang on shutdown — see
        // test_local_sink). The daemon thread dies with the process at test end.
        std::mem::forget(server);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_transform() {
        // Must be "sourcetransformer" so pre-flight recognizes the container type.
        let server = start_server("sourcetransformer", |sock, info, shutdown| async move {
            numaflow::sourcetransform::Server::new(RetimeTransformer {
                new_event_secs: 999,
            })
            .with_socket_file(sock)
            .with_server_info_file(info)
            .start_with_shutdown(shutdown)
            .await
            .expect("transform server failed");
        });
        let (socket_path, server_info_path) = paths(&server);

        let mut run = LocalRun::start(
            LocalUdf::Transform {
                socket_path,
                server_info_path,
            },
            opts(),
        )
        .await
        .expect("start transform run");

        run.feed(vec![
            event("t-1", "aaa", &["k"], 10),
            event("t-2", "bbb", &["k"], 20),
        ])
        .await
        .expect("feed");

        run.drain(Duration::from_secs(10)).await.expect("drain");
        // Poll for outputs (the source forwarder writes asynchronously).
        let mut outputs = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while outputs.len() < 2 && tokio::time::Instant::now() < deadline {
            outputs.extend(run.read_outputs().await.expect("read outputs"));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        run.stop().await.expect("stop");

        assert_eq!(outputs.len(), 2, "transformer passes both events through");
        for out in &outputs {
            assert_eq!(
                out.event_time.timestamp(),
                999,
                "transformer reassigned event time"
            );
        }
        // Detach the SDK server thread instead of joining (join can hang on shutdown — see
        // test_local_sink). The daemon thread dies with the process at test end.
        std::mem::forget(server);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_forwarder_failure() {
        let server = start_server("mapper", |sock, info, shutdown| async move {
            numaflow::map::Server::new(FailMapper)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown)
                .await
                .expect("map server failed");
        });
        let (socket_path, server_info_path) = paths(&server);

        let mut run = LocalRun::start(
            LocalUdf::Map {
                socket_path,
                server_info_path,
            },
            opts(),
        )
        .await
        .expect("start map run");

        // Feed may or may not error depending on timing. A failing mapper poisons its message: the
        // production path either treats the error as fatal (forwarder exits → `Forwarder`, exit 4)
        // or spins retrying until we bound it (`DrainTimeout`, exit 3). Both are correct "the UDF
        // failed" outcomes for a test tool (design §6.1); a *clean* drain would be the bug.
        let _ = run.feed(vec![event("f-1", "boom", &[], 10)]).await;
        let result = run.drain(Duration::from_secs(3)).await;
        run.stop().await.ok();

        assert!(
            matches!(
                result,
                Err(LocalError::Forwarder(_) | LocalError::DrainTimeout(_, _))
            ),
            "a failing mapper must surface as Forwarder or DrainTimeout, got {result:?}"
        );
        // Detach the SDK server thread instead of joining (join can hang on shutdown — see
        // test_local_sink). The daemon thread dies with the process at test end.
        std::mem::forget(server);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_startup_timeout() {
        // Point at a socket/server-info that will never exist, with a tiny startup timeout.
        let result = LocalRun::start(
            LocalUdf::Map {
                socket_path: PathBuf::from("/tmp/nfcli-nonexistent-xyz.sock"),
                server_info_path: PathBuf::from("/tmp/nfcli-nonexistent-xyz-server-info"),
            },
            LocalRunOpts {
                startup_timeout: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .await;

        // `LocalRun` is not `Debug`, so describe the error side only.
        match result {
            Err(LocalError::Startup(_)) => {}
            Err(other) => panic!("expected LocalError::Startup, got {other:?}"),
            Ok(_) => panic!("expected LocalError::Startup, but the run started"),
        }
    }
}
