use crate::config::pipeline::VERTEX_TYPE_SOURCE;
use crate::error::Error;
use crate::metrics::critical_error_reasons;
use crate::runtime_server::config::RuntimeInfoConfig;
use crate::runtime_server::runtime::{RuntimeErrorReport, persist_runtime_error_to_file};
use tracing::{info, warn};

const NUMA_CONTAINER: &str = "numa";

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailureSignature {
    source_name: String,
    operation: &'static str,
    code: String,
    message: String,
}

/// Tracks, deduplicates, and persists built-in source runtime errors.
#[derive(Debug)]
pub(crate) struct SourceRuntimeErrorTracker {
    last_reported_failure: Option<FailureSignature>,
    runtime_info_config: RuntimeInfoConfig,
}

impl Default for SourceRuntimeErrorTracker {
    fn default() -> Self {
        Self::new(RuntimeInfoConfig::default())
    }
}

impl SourceRuntimeErrorTracker {
    pub(crate) fn new(runtime_info_config: RuntimeInfoConfig) -> Self {
        Self {
            last_reported_failure: None,
            runtime_info_config,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_error_path(app_error_path: String) -> Self {
        Self::new(RuntimeInfoConfig {
            app_error_path,
            max_error_files_per_container: 5,
        })
    }

    pub(crate) fn record_failure(
        &mut self,
        source_name: &str,
        operation: &'static str,
        error: &Error,
    ) {
        let signature = failure_signature(source_name, operation, error);
        if !is_new_failure(&self.last_reported_failure, &signature) {
            return;
        }

        self.last_reported_failure = Some(signature.clone());
        critical_error!(
            VERTEX_TYPE_SOURCE,
            critical_error_reasons::SOURCE_RUNTIME_ERROR
        );
        persist_runtime_error_to_file(
            self.runtime_info_config.app_error_path.clone(),
            self.runtime_info_config.max_error_files_per_container,
            RuntimeErrorReport {
                container: NUMA_CONTAINER.to_string(),
                code: signature.code,
                message: format!(
                    "Built-in {source_name} source {operation} failed: {}",
                    signature.message
                ),
                details: format!(
                    "source={source_name}, operation={operation}, error={}",
                    signature.message
                ),
            },
        );
        warn!(
            source = source_name,
            operation,
            error = %signature.message,
            "Built-in source entered degraded state"
        );
    }

    pub(crate) fn record_recovery(&mut self, source_name: &str) {
        if self.last_reported_failure.take().is_some() {
            info!(
                source = source_name,
                "Built-in source recovered from degraded state"
            );
        }
    }
}

fn failure_signature(
    source_name: &str,
    operation: &'static str,
    error: &Error,
) -> FailureSignature {
    FailureSignature {
        source_name: source_name.to_string(),
        operation,
        code: failure_code(error),
        message: error.to_string(),
    }
}

fn is_new_failure(
    last_reported_failure: &Option<FailureSignature>,
    signature: &FailureSignature,
) -> bool {
    last_reported_failure.as_ref() != Some(signature)
}

fn failure_code(error: &Error) -> String {
    match error {
        Error::Config(_) => "Config".to_string(),
        Error::Source(_) => "Source".to_string(),
        Error::NonRetryable(_) => "NonRetryable".to_string(),
        Error::Connection(_) => "Connection".to_string(),
        Error::Grpc(status) | Error::UdfRedrive(status) => status.code().to_string(),
        Error::Shared(err) => format!("Shared({err})"),
        Error::Lag(_) => "Lag".to_string(),
        Error::AckPendingExceeded(_) => "AckPendingExceeded".to_string(),
        Error::AckOffsetNotFound(_) => "AckOffsetNotFound".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduplicates_repeated_failures_until_error_changes_or_recovery() {
        let mut tracker = SourceRuntimeErrorTracker::default();
        let first_error = Error::Source("broker disconnected".into());
        let second_error = Error::Config("missing secret".into());
        let first = failure_signature("kafka", "read", &first_error);
        let second = failure_signature("kafka", "read", &second_error);

        assert!(is_new_failure(&tracker.last_reported_failure, &first));
        tracker.last_reported_failure = Some(first.clone());
        assert!(!is_new_failure(&tracker.last_reported_failure, &first));
        assert!(is_new_failure(&tracker.last_reported_failure, &second));

        tracker.record_recovery("kafka");
        assert!(tracker.last_reported_failure.is_none());
        assert!(is_new_failure(&tracker.last_reported_failure, &first));
    }

    #[test]
    fn failure_code_maps_known_variants() {
        assert_eq!(
            failure_code(&Error::Config("bad".into())),
            "Config".to_string()
        );
        assert_eq!(
            failure_code(&Error::NonRetryable("denied".into())),
            "NonRetryable".to_string()
        );
    }
}
