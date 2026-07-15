//! clap command-line definition for `nfcli`.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::transport::ConnectOpts;

/// Default gRPC max message size (64 MiB, matching numa).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Test any numaflow user-defined function (UDF) gRPC server directly, without deploying a
/// pipeline.
#[derive(Parser, Debug)]
#[command(name = "nfcli", version, about, long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub tuning: TuningOpts,
    #[command(flatten)]
    pub output: OutputOpts,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Map UDF (unary / batch / stream modes).
    #[command(
        after_help = "Examples:\n  nfcli map --tcp 50051 --payload '{\"temp_c\":21.5}' --key sensor-1\n  nfcli map --socket /var/run/numaflow/map.sock -f messages.yaml --batch-size 20\n  nfcli map --mode batch --socket /var/run/numaflow/batchmap.sock -f messages.yaml"
    )]
    Map(MapArgs),
    /// Source transformer UDF.
    #[command(
        after_help = "Example:\n  nfcli transform --socket /var/run/numaflow/transform.sock --payload x"
    )]
    Transform(DataArgs),
    /// Aligned reduce UDF (fixed & sliding windows).
    #[command(
        after_help = "Examples:\n  nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml --window fixed --length 60s\n  nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml --window sliding --length 60s --slide 10s"
    )]
    Reduce(ReduceArgs),
    /// Session reduce UDF.
    #[command(
        after_help = "Example:\n  nfcli session-reduce --socket /var/run/numaflow/sessionreduce.sock -f clicks.yaml --gap 10s"
    )]
    SessionReduce(SessionReduceArgs),
    /// Accumulator UDF.
    #[command(
        after_help = "Example:\n  nfcli accumulator --socket /var/run/numaflow/accumulator.sock -f messages.yaml"
    )]
    Accumulator(DataArgs),
    /// Sink UDF (incl. fallback and on-success sinks).
    #[command(
        after_help = "Example:\n  nfcli sink --socket /var/run/numaflow/fb-sink.sock --payload 'poison-pill' --id order-17"
    )]
    Sink(DataArgs),
    /// User-defined source UDF.
    #[command(
        after_help = "Example:\n  nfcli source --socket /var/run/numaflow/source.sock --count 5 --rounds 2 --delay 1s --pending"
    )]
    Source(SourceArgs),
    /// Side input UDF.
    #[command(
        after_help = "Example:\n  nfcli side-input --socket /var/run/numaflow/sideinput.sock"
    )]
    SideInput(SideInputArgs),
    /// Smoke-test any UDF server's IsReady endpoint.
    #[command(after_help = "Example:\n  nfcli ready map --tcp 50051")]
    Ready(ReadyArgs),
    /// Generate shell completions (bash, zsh, fish, elvish, powershell).
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

/// Map modes; replaces the server-info `MAP_MODE` metadata.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapMode {
    Unary,
    Batch,
    Stream,
}

/// Output format.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
    Raw,
}

/// UDF service kinds for the `ready` subcommand.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceKind {
    Map,
    Transform,
    Reduce,
    SessionReduce,
    Accumulator,
    Sink,
    Source,
    SideInput,
}

/// Tuning flags shared by every subcommand.
#[derive(Args, Debug, Clone)]
pub struct TuningOpts {
    /// Connect/ready/response-wait timeout per phase.
    #[arg(long, value_parser = parse_duration, default_value = "5s", global = true)]
    pub timeout: Duration,

    /// gRPC max send/recv size in bytes.
    #[arg(long, default_value_t = DEFAULT_MAX_MESSAGE_SIZE, global = true)]
    pub max_message_size: usize,
}

/// Output flags shared by every subcommand.
#[derive(Args, Debug, Clone)]
pub struct OutputOpts {
    /// Output format.
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Text, global = true)]
    pub output: OutputFormat,

    /// Show wire-level events (handshake, EOT, window ops, timings).
    #[arg(short = 'v', long, global = true)]
    pub verbose: bool,

    /// Summary line only.
    #[arg(short = 'q', long, global = true)]
    pub quiet: bool,
}

/// Message-input flags shared by data subcommands (inline single message OR a YAML file).
#[derive(Args, Debug, Clone)]
pub struct InputOpts {
    /// YAML message stream ('-' = stdin).
    #[arg(short = 'f', long, value_name = "path|-")]
    pub file: Option<String>,

    /// Inline UTF-8 payload.
    #[arg(long, group = "payload_src")]
    pub payload: Option<String>,

    /// Inline payload = raw bytes of the given file (binary-safe).
    #[arg(long, group = "payload_src")]
    pub payload_file: Option<PathBuf>,

    /// Inline payload = base64-decoded bytes.
    #[arg(long, group = "payload_src")]
    pub payload_base64: Option<String>,

    /// Inline key (repeatable).
    #[arg(long = "key", value_name = "k")]
    pub keys: Vec<String>,

    /// Inline header k=v (repeatable).
    #[arg(long = "header", value_name = "k=v")]
    pub headers: Vec<String>,

    /// Inline event time (RFC3339 or +dur).
    #[arg(long)]
    pub event_time: Option<String>,

    /// Inline watermark (RFC3339 or +dur); default = event time.
    #[arg(long)]
    pub watermark: Option<String>,

    /// Inline message id.
    #[arg(long)]
    pub id: Option<String>,

    /// Anchor for relative (+dur) times (RFC3339).
    #[arg(long)]
    pub base_time: Option<String>,
}

/// Pacing flags for streamed sending.
#[derive(Args, Debug, Clone)]
pub struct PacingOpts {
    /// Messages taken from the file per batch.
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,

    /// Sleep between batches (e.g. 500ms, 2s).
    #[arg(long, value_parser = parse_duration, default_value = "0s")]
    pub delay: Duration,
}

/// Args for data subcommands with no extra options (transform, accumulator, sink).
#[derive(Args, Debug)]
pub struct DataArgs {
    #[command(flatten)]
    pub connect: ConnectOpts,
    #[command(flatten)]
    pub input: InputOpts,
    #[command(flatten)]
    pub pacing: PacingOpts,
}

/// Args for the map subcommand (adds --mode).
#[derive(Args, Debug)]
pub struct MapArgs {
    /// Map mode; must match how the server was built.
    #[arg(long, value_enum, default_value_t = MapMode::Unary)]
    pub mode: MapMode,
    #[command(flatten)]
    pub connect: ConnectOpts,
    #[command(flatten)]
    pub input: InputOpts,
    #[command(flatten)]
    pub pacing: PacingOpts,
}

/// Window type for aligned reduce.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    Fixed,
    Sliding,
}

/// Args for the reduce subcommand.
#[derive(Args, Debug)]
pub struct ReduceArgs {
    /// Window type.
    #[arg(long = "window", value_enum)]
    pub window: WindowKind,
    /// Window length (e.g. 60s).
    #[arg(long, value_parser = parse_duration)]
    pub length: Duration,
    /// Slide interval for sliding windows (e.g. 10s).
    #[arg(long, value_parser = parse_duration)]
    pub slide: Option<Duration>,
    #[command(flatten)]
    pub connect: ConnectOpts,
    #[command(flatten)]
    pub input: InputOpts,
    #[command(flatten)]
    pub pacing: PacingOpts,
}

/// Args for the session-reduce subcommand.
#[derive(Args, Debug)]
pub struct SessionReduceArgs {
    /// Session inactivity gap (e.g. 10s).
    #[arg(long, value_parser = parse_duration)]
    pub gap: Duration,
    #[command(flatten)]
    pub data: DataArgs,
}

/// Args for the source subcommand.
#[derive(Args, Debug)]
pub struct SourceArgs {
    #[command(flatten)]
    pub connect: ConnectOpts,
    /// Records per ReadRequest.
    #[arg(long, default_value_t = 500)]
    pub count: u64,
    /// ReadRequest timeout_in_ms.
    #[arg(long, value_parser = parse_duration, default_value = "1s")]
    pub read_timeout: Duration,
    /// Number of ReadRequests.
    #[arg(long, default_value_t = 1)]
    pub rounds: usize,
    /// Pause between rounds.
    #[arg(long, value_parser = parse_duration, default_value = "0s")]
    pub delay: Duration,
    /// Read without acking.
    #[arg(long)]
    pub no_ack: bool,
    /// Also print PendingFn before/after.
    #[arg(long)]
    pub pending: bool,
    /// Also print PartitionsFn.
    #[arg(long)]
    pub partitions: bool,
}

/// Args for the side-input subcommand.
#[derive(Args, Debug)]
pub struct SideInputArgs {
    #[command(flatten)]
    pub connect: ConnectOpts,
}

/// Args for the ready subcommand.
#[derive(Args, Debug)]
pub struct ReadyArgs {
    /// UDF service kind.
    #[arg(value_enum)]
    pub kind: ServiceKind,
    #[command(flatten)]
    pub connect: ConnectOpts,
}

/// Parse a humantime duration (e.g. `500ms`, `2s`, `1m`).
fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| format!("invalid duration {s:?}: {e}"))
}
