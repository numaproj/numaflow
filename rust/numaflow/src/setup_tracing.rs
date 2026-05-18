use opentelemetry_sdk::trace::Sampler;
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

/// Build a sampler from the standard OTel environment variables.
///
/// - `OTEL_TRACES_SAMPLER`: sampler type
/// - `OTEL_TRACES_SAMPLER_ARG`: sampler argument (ratio for `*_traceidratio` samplers)
///
/// When neither env var is set, defaults to `parentbased_traceidratio` at 0.1 (10%).
/// Numaflow emits multiple spans per message, so the SDK's `parentbased_always_on` default
/// would flood collectors at typical message rates; operators who want full sampling can opt
/// in via `OTEL_TRACES_SAMPLER=parentbased_always_on`.
///
/// Supported samplers: `always_on`, `always_off`, `traceidratio`,
/// `parentbased_always_on`, `parentbased_always_off`, `parentbased_traceidratio`.
fn build_sampler() -> opentelemetry_sdk::trace::Sampler {
    let sampler_name =
        std::env::var("OTEL_TRACES_SAMPLER").unwrap_or_else(|_| "parentbased_traceidratio".into());
    let sampler_arg_raw = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .or_else(|| Some("0.1".into()));
    build_sampler_from(&sampler_name, sampler_arg_raw.as_deref())
}

fn build_sampler_from(
    sampler_name: &str,
    sampler_arg_raw: Option<&str>,
) -> opentelemetry_sdk::trace::Sampler {
    let sampler_arg = sampler_arg_raw.and_then(|v| v.parse::<f64>().ok());

    let ratio_or_default = |sampler_kind: &str| {
        sampler_arg.unwrap_or_else(|| {
            if let Some(raw) = sampler_arg_raw {
                eprintln!(
                    "[setup_tracing] Invalid OTEL_TRACES_SAMPLER_ARG='{raw}' for sampler '{sampler_kind}', defaulting ratio to 1.0"
                );
            }
            1.0
        })
    };

    let sampler = match sampler_name {
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
            .unwrap_or_default(),
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
    use opentelemetry_otlp::WithExportConfig;

    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()?;

    eprintln!(
        "[setup_tracing] Configuring OTLP exporter: endpoint={otlp_endpoint}, service_name={service_name}"
    );

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
        .with_sampler(build_sampler())
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
    // `.clone()` is an Arc refcount bump: the global handle and the returned handle
    // share the same underlying span queue and exporter, so `shutdown()` on either
    // (done via `TracerProviderGuard`'s drop) flushes and tears down both.
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Eagerly populate numaflow-core's cached `BoxedTracer` now that the global provider
    // is registered, so the first per-message span doesn't accidentally cache a noop tracer.
    numaflow_core::init_tracer();

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
#[must_use = "binding drops the guard immediately and flushes; hold it with `let _guard = ...` for the duration of the program"]
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::{
        Context,
        trace::{
            SamplingDecision, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId,
            TraceState,
        },
    };
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, ShouldSample};

    fn sampling_decision(
        sampler: &Sampler,
        parent_context: Option<&Context>,
        trace_id: TraceId,
    ) -> SamplingDecision {
        sampler
            .should_sample(
                parent_context,
                trace_id,
                "test-span",
                &SpanKind::Internal,
                &[],
                &[],
            )
            .decision
    }

    fn trace_id_at_probability_boundary(prob: f64, just_below_boundary: bool) -> TraceId {
        let upper_bound = (prob.max(0.0) * (1u64 << 63) as f64) as u64;
        let rnd = if just_below_boundary {
            upper_bound.saturating_sub(1)
        } else {
            upper_bound
        };

        TraceId::from((rnd as u128) << 1)
    }

    fn parent_context(sampled: bool) -> Context {
        let trace_flags = if sampled {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        };

        Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from(1u128),
            SpanId::from(1u64),
            trace_flags,
            true,
            TraceState::default(),
        ))
    }

    #[test]
    fn sampler_always_on() {
        assert!(matches!(
            build_sampler_from("always_on", None),
            Sampler::AlwaysOn
        ));
    }

    #[test]
    fn sampler_always_off() {
        assert!(matches!(
            build_sampler_from("always_off", None),
            Sampler::AlwaysOff
        ));
    }

    #[test]
    fn sampler_traceidratio_parses_arg() {
        let sampler = build_sampler_from("traceidratio", Some("0.25"));
        assert!(matches!(&sampler, Sampler::TraceIdRatioBased(_)));
        assert_eq!(
            sampling_decision(&sampler, None, trace_id_at_probability_boundary(0.25, true)),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sampling_decision(
                &sampler,
                None,
                trace_id_at_probability_boundary(0.25, false)
            ),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn sampler_traceidratio_defaults_to_1_when_arg_missing() {
        let sampler = build_sampler_from("traceidratio", None);
        assert!(matches!(&sampler, Sampler::TraceIdRatioBased(_)));
        assert_eq!(
            sampling_decision(&sampler, None, TraceId::from(u128::MAX)),
            SamplingDecision::RecordAndSample
        );
    }

    #[test]
    fn sampler_parentbased_always_on() {
        let sampler = build_sampler_from("parentbased_always_on", None);
        let sampled_parent = parent_context(true);
        let unsampled_parent = parent_context(false);

        assert!(matches!(&sampler, Sampler::ParentBased(_)));
        assert_eq!(
            sampling_decision(&sampler, None, TraceId::from(u128::MAX)),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sampling_decision(&sampler, Some(&sampled_parent), TraceId::from(u128::MAX)),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sampling_decision(&sampler, Some(&unsampled_parent), TraceId::from(u128::MAX)),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn sampler_parentbased_always_off() {
        let sampler = build_sampler_from("parentbased_always_off", None);
        let sampled_parent = parent_context(true);

        assert!(matches!(&sampler, Sampler::ParentBased(_)));
        assert_eq!(
            sampling_decision(&sampler, None, TraceId::from(1u128)),
            SamplingDecision::Drop
        );
        assert_eq!(
            sampling_decision(&sampler, Some(&sampled_parent), TraceId::from(1u128)),
            SamplingDecision::RecordAndSample
        );
    }

    #[test]
    fn sampler_parentbased_traceidratio_with_arg() {
        let sampler = build_sampler_from("parentbased_traceidratio", Some("0.1"));
        let sampled_parent = parent_context(true);
        let unsampled_parent = parent_context(false);

        assert!(matches!(&sampler, Sampler::ParentBased(_)));
        assert_eq!(
            sampling_decision(&sampler, None, trace_id_at_probability_boundary(0.1, true)),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sampling_decision(&sampler, None, trace_id_at_probability_boundary(0.1, false)),
            SamplingDecision::Drop
        );
        assert_eq!(
            sampling_decision(
                &sampler,
                Some(&sampled_parent),
                trace_id_at_probability_boundary(0.1, false),
            ),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sampling_decision(
                &sampler,
                Some(&unsampled_parent),
                trace_id_at_probability_boundary(0.1, true),
            ),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn sampler_unknown_name_falls_back_to_parentbased_always_on() {
        let sampler = build_sampler_from("nonsense", None);
        let unsampled_parent = parent_context(false);

        assert!(matches!(&sampler, Sampler::ParentBased(_)));
        assert_eq!(
            sampling_decision(&sampler, None, TraceId::from(u128::MAX)),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sampling_decision(&sampler, Some(&unsampled_parent), TraceId::from(u128::MAX)),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn tracer_provider_guard_drop_without_provider_is_noop() {
        // Guard holding None must not panic on drop (the path taken when
        // OTLP is not configured).
        let guard = TracerProviderGuard(None);
        drop(guard);
    }

    #[test]
    fn tracer_provider_guard_drop_with_provider_shuts_down_cleanly() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should build");
        let _runtime_guard = runtime.enter();

        let guard = TracerProviderGuard(Some(SdkTracerProvider::builder().build()));
        drop(guard);
    }
}
