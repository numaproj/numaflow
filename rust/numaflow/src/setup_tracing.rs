//! Tracing subscriber setup (RUST_LOG, NUMAFLOW_DEBUG, optional OTLP).
//!
//! OTLP tracing is enabled when the `numa` container sees any of these env vars:
//!
//! - `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` –
//!   OTLP gRPC endpoint (e.g. `http://collector:4317`).
//! - `OTEL_SERVICE_NAME` – service name for spans (default: `numaflow-core`).
//!
//! There are two ways to supply them:
//!
//! 1. **Pipeline/MonoVertex-level** – `spec.tracing.otlpEndpoint` (static string).
//!    The controller injects `OTEL_*` env vars into every vertex and daemon container.
//!
//! 2. **Container-level** – `spec.containerTemplate.env` (or vertex-level equivalent).
//!    Supports `fieldRef`, `$(VAR)` substitution, and any other Kubernetes env features.
//!    This is required when the collector runs as a DaemonSet and the endpoint depends
//!    on `status.hostIP`.
//!
//! When the endpoint is present the global W3C Trace Context propagator is set so that
//! trace context can be extracted/injected from message metadata
//! (see `numaflow_core::shared::otel`).

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, filter::EnvFilter, fmt};

use std::backtrace::{Backtrace, BacktraceStatus};
use std::panic::PanicHookInfo;

/// Standard env vars for OTLP tracing (see OpenTelemetry spec).
const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";
const DEFAULT_OTEL_SERVICE_NAME: &str = "numaflow-core";

/// Panic hook to send panic info to `tracing` instead of stderr.
/// Without this, a panic will be logged to stderr as:
/// ```
/// 2025-06-23T06:10:39.561021Z  INFO t: Waiting for task to finish
///
/// thread 'main' panicked at src/main.rs:51:13:
/// called `Result::unwrap()` on an `Err` value: JoinError::Panic(Id(13), "Panic in task", ...)
/// ```
///
/// With the panic hook, the same will be logged as:
/// ```
/// 2025-06-23T06:10:59.171995Z  INFO t: Waiting for task to finish
/// 2025-06-23T06:10:59.172054Z ERROR t: src/main.rs:51:13: called `Result::unwrap()` on an `Err` value: JoinError::Panic(Id(13), "Panic in task", ...)
/// ```
fn report_panic(panic_info: &PanicHookInfo<'_>) {
    // noop if the RUST_BACKTRACE or RUST_LIB_BACKTRACE backtrace variables are both not set
    let backtrace = Backtrace::capture();
    let backtrace_captured = backtrace.status() == BacktraceStatus::Captured;
    let payload = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
        Some(*s)
    } else {
        panic_info
            .payload()
            .downcast_ref::<String>()
            .map(|s| s.as_str())
    };

    // https://doc.rust-lang.org/std/panic/struct.PanicHookInfo.html#method.location
    // This method will currently always return Some, but this may change in future versions.
    match (panic_info.location(), payload, backtrace_captured) {
        (Some(location), Some(payload), false) => {
            // Same as tracing::error!("{}", panic_info), except that all the info will be printed in one line
            tracing::error!(
                "{}:{}:{}: {}",
                location.file(),
                location.line(),
                location.column(),
                payload,
            );
        }
        _ => {
            // default formatting
            tracing::error!("{}\n{}", panic_info, backtrace);
        }
    };
}

pub fn register() {
    // Set up the tracing subscriber. RUST_LOG can be used to set the log level.
    // The default log level is `info`. The `axum::rejection=trace` enables showing
    // rejections from built-in extractors at `TRACE` level.
    let debug_mode = std::env::var("NUMAFLOW_DEBUG").is_ok_and(|v| v.to_lowercase() == "true");

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if debug_mode {
            EnvFilter::new("debug,h2::codec=info")
        } else {
            EnvFilter::new("info")
        }
    });

    let layer = if debug_mode {
        // Text format
        fmt::layer().boxed()
    } else {
        // JSON format, flattened
        fmt::layer()
            .with_ansi(false)
            .json()
            .flatten_event(true)
            .boxed()
    };

    // Print all OTEL-related env vars for debugging (before subscriber is installed, goes to stderr).
    let traces_endpoint = std::env::var(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).ok();
    let generic_endpoint = std::env::var(OTEL_EXPORTER_OTLP_ENDPOINT).ok();
    let service_name_env = std::env::var(OTEL_SERVICE_NAME).ok();

    eprintln!("[setup_tracing] OTEL env vars:");
    eprintln!("  {}={}", OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, traces_endpoint.as_deref().unwrap_or("<not set>"));
    eprintln!("  {}={}", OTEL_EXPORTER_OTLP_ENDPOINT, generic_endpoint.as_deref().unwrap_or("<not set>"));
    eprintln!("  {}={}", OTEL_SERVICE_NAME, service_name_env.as_deref().unwrap_or("<not set>"));
    eprintln!("  NUMAFLOW_DEBUG={}", std::env::var("NUMAFLOW_DEBUG").unwrap_or_else(|_| "<not set>".into()));

    let otlp_endpoint = traces_endpoint.or(generic_endpoint);
    let service_name = service_name_env.unwrap_or_else(|| DEFAULT_OTEL_SERVICE_NAME.to_string());

    // Add optional OTLP layer first so it wraps Registry (required for Layer type).
    let otel_layer = match otlp_endpoint {
        Some(ref ep) => {
            eprintln!(
                "[setup_tracing] OTLP tracing ENABLED: endpoint={}, service_name={}",
                ep, service_name
            );
            match init_otlp_tracing(ep.clone(), service_name.clone()) {
                Ok(l) => {
                    opentelemetry::global::set_text_map_propagator(
                        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
                    );
                    eprintln!("[setup_tracing] OTLP exporter initialized successfully, W3C propagator set");
                    Some(l)
                }
                Err(e) => {
                    eprintln!("[setup_tracing] OTLP tracing DISABLED: failed to init exporter: {e}");
                    None
                }
            }
        }
        None => {
            eprintln!("[setup_tracing] OTLP tracing DISABLED (no OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT set)");
            None
        }
    };

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(filter)
        .with(layer)
        .init();

    std::panic::set_hook(Box::new(report_panic));
}

/// Builds OTLP span exporter and tracer provider, sets global provider, returns
/// a tracing layer that records spans to OTLP. Uses batch exporter.
///
/// Requires the main Tokio runtime to be entered (via `rt.enter()`) before
/// calling, so the tonic gRPC channel binds to the long-lived runtime.
fn init_otlp_tracing(
    endpoint: String,
    service_name: String,
) -> Result<
    impl Layer<tracing_subscriber::Registry> + Send + Sync,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let endpoint = endpoint.trim_end_matches('/').to_string();

    let handle = tokio::runtime::Handle::current();
    let exporter = handle.block_on(async {
        opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
    })?;

    let resource = opentelemetry_sdk::Resource::builder_empty()
        .with_attributes([opentelemetry::KeyValue::new(
            "service.name",
            service_name.clone(),
        )])
        .build();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer(service_name);
    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}
