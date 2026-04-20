use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, filter::EnvFilter, fmt};

use std::backtrace::{Backtrace, BacktraceStatus};
use std::panic::PanicHookInfo;

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

/// Initialize the OTLP tracing layer if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
/// Returns `None` if tracing is not configured (no env var), in which case
/// the subscriber runs with logging only.
fn init_otlp_layer<S>(
    service_name: String,
) -> Option<(
    tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
    opentelemetry_sdk::trace::SdkTracerProvider,
)>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()?;

    eprintln!(
        "[setup_tracing] Configuring OTLP exporter: endpoint={otlp_endpoint}, service_name={service_name}"
    );

    use opentelemetry_otlp::WithExportConfig;
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_endpoint)
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[setup_tracing] Failed to create OTLP exporter: {e}");
            return None;
        }
    };

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name)
                .build(),
        )
        .build();

    use opentelemetry::trace::TracerProvider as _;
    let tracer = tracer_provider.tracer("numaflow-core");

    // Set the global tracer provider so OTel API users (e.g., per-message sink.write
    // spans created via the OTel API directly) can access it.
    // We clone here because we also return the provider for explicit shutdown.
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Set W3C Trace Context propagator for context propagation via sys_metadata.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    eprintln!("[setup_tracing] OTLP tracing ENABLED");

    Some((otel_layer, tracer_provider))
}

/// Initialize the tracing subscriber with optional OTLP export.
/// Returns the `SdkTracerProvider` handle if OTLP is enabled — the caller
/// must keep it alive and call `.shutdown()` before process exit to flush
/// buffered spans.
pub fn register() -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    let debug_mode = std::env::var("NUMAFLOW_DEBUG").is_ok_and(|v| v.to_lowercase() == "true");
    let default_log_level = if debug_mode {
        "debug,h2::codec=info" // "h2::codec" is too noisy
    } else {
        "info"
    };

    let filter = EnvFilter::builder()
        .with_default_directive(default_log_level.parse().unwrap_or(Level::INFO.into()))
        .from_env_lossy();

    let fmt_layer = if debug_mode {
        fmt::layer().boxed()
    } else {
        fmt::layer()
            .with_ansi(false)
            .json()
            .flatten_event(true)
            .boxed()
    };

    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "platform".into());
    let (otel_layer, tracer_provider) = match init_otlp_layer(service_name) {
        Some((layer, provider)) => (Some(layer), Some(provider)),
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    std::panic::set_hook(Box::new(report_panic));

    tracer_provider
}
