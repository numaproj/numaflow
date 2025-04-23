//! Library for robust SQS message handling using an actor-based architecture.
//!
//! This module provides a fault-tolerant interface for interacting with Amazon SQS,
//! with a focus on:
//! - Error propagation and handling for AWS SDK errors
//! - Actor-based concurrency model for thread safety
//! - Clean abstraction of SQS operations
use tokio::sync::oneshot;
pub mod source;

/// Custom error types for the SQS client library.
///
/// Design goals:
/// - Ergonomic error handling with thiserror
/// - Clear error propagation from AWS SDK
/// - Explicit handling of actor communication failures
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed with SQS error - {0}")]
    Sqs(#[from] aws_sdk_sqs::Error),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = core::result::Result<T, Error>;

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}

#[cfg(test)]
mod tests {
    use aws_config::BehaviorVersion;
    use aws_smithy_mocks_experimental::{mock, MockResponseInterceptor, RuleMode};
    use aws_smithy_types::error::ErrorMetadata;

    use super::*;

    #[tokio::test]
    async fn test_sqs_error_conversion() {
        let modeled_error = mock!(aws_sdk_sqs::Client::get_queue_url).then_error(|| {
            aws_sdk_sqs::operation::get_queue_url::GetQueueUrlError::generic(
                ErrorMetadata::builder().code("InvalidAddress").build(),
            )
        });

        let get_object_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&modeled_error);

        let sqs = aws_sdk_sqs::Client::from_conf(
            aws_sdk_sqs::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(aws_sdk_sqs::config::Region::new("us-east-1"))
                .credentials_provider(make_sqs_test_credentials())
                .interceptor(get_object_mocks)
                .build(),
        );
        let err = sqs.get_queue_url().send().await.unwrap_err();

        let converted_error = Error::Sqs(err.into());
        assert!(matches!(converted_error, Error::Sqs(_)));
        assert!(converted_error
            .to_string()
            .contains("Failed with SQS error"));
    }

    #[test]
    fn test_string_error_conversion() {
        let str_err = "custom error message".to_string();
        let err: Error = str_err.into();
        assert!(matches!(err, Error::Other(_)));
        assert_eq!(err.to_string(), "custom error message");
    }

    #[tokio::test]
    async fn test_actor_task_terminated() {
        let (tx, rx) = oneshot::channel::<()>();
        drop(tx); // Force the error
        let err = Error::ActorTaskTerminated(rx.await.unwrap_err());
        assert!(matches!(err, Error::ActorTaskTerminated(_)));
        assert!(err.to_string().contains("Actor task is terminated"));
    }

    fn make_sqs_test_credentials() -> aws_sdk_sqs::config::Credentials {
        aws_sdk_sqs::config::Credentials::new(
            "ATESTCLIENT",
            "astestsecretkey",
            Some("atestsessiontoken".to_string()),
            None,
            "",
        )
    }
}
