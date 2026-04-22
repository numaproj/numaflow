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

/// Build a sampler from standard OTel environment variables.
///
/// - `OTEL_TRACES_SAMPLER`: sampler type (default: `parentbased_always_on`)
/// - `OTEL_TRACES_SAMPLER_ARG`: sampler argument (e.g., `0.1` for 10%)
///
/// Supported samplers: `always_on`, `always_off`, `traceidratio`,
/// `parentbased_always_on`, `parentbased_always_off`, `parentbased_traceidratio`.
fn build_sampler() -> opentelemetry_sdk::trace::Sampler {
    use opentelemetry_sdk::trace::Sampler;

    let sampler_name =
        std::env::var("OTEL_TRACES_SAMPLER").unwrap_or_else(|_| "parentbased_always_on".into());
    let sampler_arg_raw = std::env::var("OTEL_TRACES_SAMPLER_ARG").ok();
    let sampler_arg = sampler_arg_raw
        .as_deref()
        .and_then(|v| v.parse::<f64>().ok());

    let ratio_or_default = |sampler_kind: &str| {
        sampler_arg.unwrap_or_else(|| {
            if let Some(raw) = sampler_arg_raw.as_deref() {
                eprintln!(
                    "[setup_tracing] Invalid OTEL_TRACES_SAMPLER_ARG='{raw}' for sampler '{sampler_kind}', defaulting ratio to 1.0"
                );
            }
            1.0
        })
    };

    let sampler = match sampler_name.as_str() {
        "always_on" => Sampler::AlwaysOn,
        "always_off" => Sampler::AlwaysOff,
        "traceidratio" => Sampler::TraceIdRatioBased(ratio_or_default("traceidratio")),
        "parentbased_always_on" => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        "parentbased_traceidratio" => Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            ratio_or_default("parentbased_traceidratio"),
        ))),
        _ => {
            eprintln!(
                "[setup_tracing] Unknown sampler '{sampler_name}', defaulting to parentbased_always_on"
            );
            Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
        }
    };

    eprintln!(
        "[setup_tracing] Sampler: {sampler_name}{}",
        sampler_arg
            .map(|r| format!(", ratio={r}"))
            .unwrap_or_default()
    );

    sampler
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

    let sampler = build_sampler();
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name)
                .build(),
        )
        .build();

    use opentelemetry::trace::TracerProvider as _;
    let tracer = tracer_provider.tracer("numaflow");

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

/// RAII guard that flushes buffered spans by shutting down the provider
/// on drop. Must be dropped while the Tokio runtime is still alive.
pub struct TracerProviderGuard(Option<opentelemetry_sdk::trace::SdkTracerProvider>);

impl Drop for TracerProviderGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.0.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("[setup_tracing] Failed to shut down tracer provider: {e}");
        }
    }
}

/// Initialize the tracing subscriber with optional OTLP export.
/// Returns a `TracerProviderGuard` that will flush buffered spans on drop.
/// Callers must bind it (e.g., `let _guard = register();`) rather than
/// discard it, or the provider will shut down immediately.
pub fn register() -> TracerProviderGuard {
    let debug_mode = std::env::var("NUMAFLOW_DEBUG").is_ok_and(|v| v.to_lowercase() == "true");
    let default_log_level = if debug_mode {
        "debug,h2::codec=info" // "h2::codec" is too noisy
    } else {
        "info"
    };

    // Build filtering from default directives and allow `RUST_LOG` environment variable to override.
    let filter = EnvFilter::builder()
        .with_default_directive(default_log_level.parse().unwrap_or(Level::INFO.into()))
        .from_env_lossy();

    let fmt_layer = if debug_mode {
        // Log in a human-readable format for local debugging/development.
        fmt::layer().boxed()
    } else {
        // Log in a JSON format with flattened event fields.
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

    // Only export spans (info_span!, tracing::Span) to the OTel layer, not log
    // events (info!, error!, warn!). Without this filter, every log statement in the
    // codebase becomes an OTel event, overwhelming the batch exporter at high throughput.
    let otel_filter = tracing_subscriber::filter::filter_fn(|metadata| metadata.is_span());

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer.with_filter(otel_filter))
        .init();

    std::panic::set_hook(Box::new(report_panic));

    TracerProviderGuard(tracer_provider)
}
