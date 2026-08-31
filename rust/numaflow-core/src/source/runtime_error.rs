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
            if let Some(last) = &mut self.last_reported_failure {
                last.message = signature.message;
                warn!(
                    source = source_name,
                    operation,
                    code = %last.code,
                    error = %last.message,
                    "Built-in source remains degraded"
                );
            }
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
    let (code, message) = match error {
        Error::SourceRedrive { code, message, .. } => ((*code).to_string(), message.clone()),
        _ => (failure_code(error), error.to_string()),
    };
    FailureSignature {
        source_name: source_name.to_string(),
        operation,
        code,
        message,
    }
}

fn is_new_failure(
    last_reported_failure: &Option<FailureSignature>,
    signature: &FailureSignature,
) -> bool {
    last_reported_failure.as_ref().is_none_or(|last| {
        last.source_name != signature.source_name
            || last.operation != signature.operation
            || last.code != signature.code
    })
}

fn failure_code(error: &Error) -> String {
    match error {
        Error::Config(_) => "Config".to_string(),
        Error::Source(_) => "Source".to_string(),
        Error::NonRetryable(_) => "NonRetryable".to_string(),
        Error::Connection(_) => "Connection".to_string(),
        Error::Grpc(status) | Error::UdfRedrive(status) => status.code().to_string(),
        Error::Shared(_) => "Shared".to_string(),
        Error::Lag(_) => "Lag".to_string(),
        Error::AckPendingExceeded(_) => "AckPendingExceeded".to_string(),
        Error::AckOffsetNotFound(_) => "AckOffsetNotFound".to_string(),
        Error::SourceRedrive { code, .. } => (*code).to_string(),
        Error::Metrics(_) => "Metrics".to_string(),
        Error::Sink(_) => "Sink".to_string(),
        Error::FbSink(_) => "FallbackSink".to_string(),
        Error::OsSink(_) => "OnSuccessSink".to_string(),
        Error::Transformer(_) => "Transformer".to_string(),
        Error::Mapper(_) => "Mapper".to_string(),
        Error::Forwarder(_) => "Forwarder".to_string(),
        Error::BypassRouter(_) => "BypassRouter".to_string(),
        Error::Proto(_) => "Proto".to_string(),
        Error::ISB(_) => "ISB".to_string(),
        Error::ActorPatternRecv(_) => "ActorPatternRecv".to_string(),
        Error::Tracker(_) => "Tracker".to_string(),
        Error::DuplicateInflight(_) => "DuplicateInflight".to_string(),
        Error::Watermark(_) => "Watermark".to_string(),
        Error::SideInput(_) => "SideInput".to_string(),
        Error::Reduce(_) => "Reduce".to_string(),
        Error::Cancelled() => "Cancelled".to_string(),
        Error::WAL(_) => "WAL".to_string(),
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
        let same_code_new_message =
            failure_signature("kafka", "read", &Error::Source("new request id".into()));
        assert!(!is_new_failure(
            &tracker.last_reported_failure,
            &same_code_new_message
        ));
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

    #[test]
    fn record_failure_persists_and_deduplicates_files() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().to_str().expect("temp path").to_string();
        let mut tracker = SourceRuntimeErrorTracker::with_runtime_error_path(path);
        let error = Error::Source("broker disconnected".into());

        tracker.record_failure("kafka", "read", &error);
        tracker.record_failure("kafka", "read", &error);

        assert_eq!(runtime_error_files(&temp_dir).len(), 1);
    }

    #[test]
    fn record_recovery_allows_persisting_the_same_failure_again() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let path = temp_dir.path().to_str().expect("temp path").to_string();
        let mut tracker = SourceRuntimeErrorTracker::with_runtime_error_path(path);
        let error = Error::Source("broker disconnected".into());

        tracker.record_failure("kafka", "read", &error);
        tracker.record_recovery("kafka");
        tracker.record_failure("kafka", "read", &error);

        assert_eq!(runtime_error_files(&temp_dir).len(), 2);
    }

    fn runtime_error_files(temp_dir: &tempfile::TempDir) -> Vec<std::path::PathBuf> {
        let dir = temp_dir.path().join("numa");
        if !dir.exists() {
            return vec![];
        }
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect()
    }
}
