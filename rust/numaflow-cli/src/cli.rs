//! clap definitions for `nfcli`. Kept declarative; all behavior lives in `run/`.

use std::path::PathBuf;

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "nfcli",
    about = "Test Numaflow UDFs locally by running the real vertex forwarder against an in-memory ISB",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Increase log verbosity (numaflow_core + nfcli at debug).
    #[arg(short = 'v', long, global = true)]
    pub verbose: bool,

    /// Silence all logs except errors.
    #[arg(short = 'q', long, global = true, conflicts_with = "verbose")]
    pub quiet: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run a map UDF over the input events.
    Map(MapArgs),
    /// Run a source transformer over the input events (via a replay source).
    Transform(TransformArgs),
    /// Run an aligned (fixed/sliding) reduce UDF.
    Reduce(ReduceArgs),
    /// Run a session (unaligned) reduce UDF.
    SessionReduce(SessionReduceArgs),
    /// Run an accumulator (unaligned) reduce UDF.
    Accumulator(AccumulatorArgs),
    /// Run a sink UDF over the input events.
    Sink(SinkArgs),
    /// Read messages from a user-defined source.
    Source(SourceArgs),
    /// Retrieve a single side-input value.
    SideInput(SideInputArgs),
    /// Check readiness of a UDF server.
    Ready(ReadyArgs),
}

/// Output format shared across subcommands.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
    Raw,
}

/// The map-mode assertion (compared against server-info; never overrides it).
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapModeArg {
    Unary,
    Batch,
    Stream,
}

/// Reduce window shape for the `reduce` subcommand.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind {
    Fixed,
    Sliding,
}

/// Connection flags common to every UDF subcommand.
#[derive(Args, Debug, Clone)]
pub struct ConnArgs {
    /// UDS path to the UDF server (required).
    #[arg(long)]
    pub socket: PathBuf,

    /// server-info file path. Default: derived from the socket path (see README).
    #[arg(long)]
    pub server_info: Option<PathBuf>,

    /// Overall wait for the socket + server-info to be ready.
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    pub timeout: std::time::Duration,

    /// gRPC max send/recv message size in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub max_message_size: usize,
}

/// Feed/drain pacing flags shared by data subcommands.
#[derive(Args, Debug, Clone)]
pub struct FeedArgs {
    /// Upper bound on the drain phase.
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    pub drain_timeout: std::time::Duration,

    /// Forwarder read batch size and CLI feed-chunk size.
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,

    /// Sleep between feed chunks.
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
    pub delay: std::time::Duration,

    /// Input/output buffer capacity.
    #[arg(long, default_value_t = 30_000)]
    pub buffer_capacity: usize,
}

/// Input-source flags: a YAML file (`-f`) XOR one inline payload flag.
#[derive(Args, Debug, Clone)]
#[command(group(
    // Group name must not collide with any arg name (`payload`), hence `payload_source`.
    ArgGroup::new("payload_source")
        .args(["payload", "payload_file", "payload_base64"])
        .multiple(false)
))]
pub struct InputArgs {
    /// YAML multi-doc message stream; `-` = stdin.
    #[arg(short = 'f', long)]
    pub file: Option<String>,

    /// Inline UTF-8 payload for a single event.
    #[arg(long)]
    pub payload: Option<String>,

    /// Inline payload read from a file.
    #[arg(long)]
    pub payload_file: Option<PathBuf>,

    /// Inline base64-encoded payload.
    #[arg(long)]
    pub payload_base64: Option<String>,

    /// Keys for the inline event (repeatable).
    #[arg(long = "key")]
    pub keys: Vec<String>,

    /// Headers for the inline event as `K=V` (repeatable).
    #[arg(long = "header")]
    pub headers: Vec<String>,

    /// Event time for the inline event (RFC3339 or `+dur`).
    #[arg(long)]
    pub event_time: Option<String>,

    /// Watermark for the inline event (reduce family only).
    #[arg(long)]
    pub watermark: Option<String>,

    /// Explicit id for the inline event.
    #[arg(long)]
    pub id: Option<String>,

    /// Base time for relative (`+dur`) event/watermark times. Default: now.
    #[arg(long)]
    pub base_time: Option<String>,
}

/// Output flags shared across subcommands.
#[derive(Args, Debug, Clone)]
pub struct OutputArgs {
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,
}

#[derive(Args, Debug)]
pub struct MapArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub feed: FeedArgs,
    #[command(flatten)]
    pub input: InputArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    /// Optional assertion: fail if server-info's map mode differs.
    #[arg(long, value_enum)]
    pub mode: Option<MapModeArg>,
}

#[derive(Args, Debug)]
pub struct TransformArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub feed: FeedArgs,
    #[command(flatten)]
    pub input: InputArgs,
    #[command(flatten)]
    pub output: OutputArgs,
}

#[derive(Args, Debug)]
pub struct ReduceArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub feed: FeedArgs,
    #[command(flatten)]
    pub input: InputArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    /// Window shape.
    #[arg(long, value_enum)]
    pub window: WindowKind,
    /// Window length.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub length: std::time::Duration,
    /// Slide (required iff `--window sliding`).
    #[arg(long, value_parser = humantime::parse_duration)]
    pub slide: Option<std::time::Duration>,
    /// Allowed lateness.
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
    pub allowed_lateness: std::time::Duration,
}

#[derive(Args, Debug)]
pub struct SessionReduceArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub feed: FeedArgs,
    #[command(flatten)]
    pub input: InputArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    /// Session gap.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub gap: std::time::Duration,
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
    pub allowed_lateness: std::time::Duration,
}

#[derive(Args, Debug)]
pub struct AccumulatorArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub feed: FeedArgs,
    #[command(flatten)]
    pub input: InputArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    /// Accumulator timeout.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub timeout: std::time::Duration,
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
    pub allowed_lateness: std::time::Duration,
}

#[derive(Args, Debug)]
pub struct SinkArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub feed: FeedArgs,
    #[command(flatten)]
    pub input: InputArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    /// Fallback sink socket.
    #[arg(long)]
    pub fallback_socket: Option<PathBuf>,
    /// Fallback sink server-info.
    #[arg(long)]
    pub fallback_server_info: Option<PathBuf>,
    /// On-success sink socket.
    #[arg(long)]
    pub on_success_socket: Option<PathBuf>,
    /// On-success sink server-info.
    #[arg(long)]
    pub on_success_server_info: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct SourceArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    #[command(flatten)]
    pub output: OutputArgs,
    /// Stop after reading this many messages.
    #[arg(long, default_value_t = 500)]
    pub count: usize,
    /// Or stop after this wall-clock duration.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<std::time::Duration>,
    /// Print the source's pending count.
    #[arg(long)]
    pub pending: bool,
}

#[derive(Args, Debug)]
pub struct SideInputArgs {
    /// UDS path to the side-input server.
    #[arg(long)]
    pub socket: PathBuf,
    /// server-info file path (unused for the unary call, accepted for symmetry).
    #[arg(long)]
    pub server_info: Option<PathBuf>,
}

/// The container types `ready` can probe.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyType {
    Map,
    Sink,
    Source,
    Transform,
    Reduce,
    SessionReduce,
    Accumulator,
    SideInput,
}

#[derive(Args, Debug)]
pub struct ReadyArgs {
    /// The server type to probe.
    pub kind: ReadyType,
    /// UDS path to the server.
    #[arg(long)]
    pub socket: PathBuf,
    /// server-info file path (unused for the readiness call).
    #[arg(long)]
    pub server_info: Option<PathBuf>,
    /// Wait for readiness up to this long.
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    pub timeout: std::time::Duration,
}
