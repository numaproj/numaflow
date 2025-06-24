use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
    } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
        Some(s.as_str())
    } else {
        None
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
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // TODO: add a better default based on entry point invocation
                //  e.g., serving/monovertex might need a different default
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .init();

    std::panic::set_hook(Box::new(report_panic));
}
