//! Library for robust SQS message handling using an actor-based architecture.
//!
//! This module provides a fault-tolerant interface for interacting with Amazon SQS,
//! with a focus on:
//! - Error propagation and handling for AWS SDK errors
//! - Actor-based concurrency model for thread safety
//! - Clean abstraction of SQS operations

use std::sync::Arc;

use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::meta::region::RegionProviderChain;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_sqs::Client;
use tokio::sync::oneshot;
use tracing;

pub mod sink;
pub mod source;

/// Simple assume role configuration struct for the SQS client
#[derive(Debug, Clone, PartialEq)]
pub struct AssumeRoleConfig {
    pub role_arn: String,
    pub session_name: Option<String>,
    pub duration_seconds: Option<i32>,
    pub external_id: Option<String>,
    pub policy: Option<String>,
    pub policy_arns: Option<Vec<String>>,
}

/// Configuration for SQS client creation
#[derive(Debug)]
pub enum SqsConfig {
    Source(source::SqsSourceConfig),
    Sink(sink::SqsSinkConfig),
}

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

    #[error("Failed with STS error - {0}")]
    Sts(#[from] aws_sdk_sts::Error),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("{0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
pub enum SqsSourceError {
    #[error("SQS Source Error: {0}")]
    Error(#[from] Error),
}
#[derive(thiserror::Error, Debug)]
pub enum SqsSinkError {
    #[error("SQS Sink Error: {0}")]
    Error(#[from] Error),
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}

impl From<SqsSourceError> for Error {
    fn from(value: SqsSourceError) -> Self {
        match value {
            SqsSourceError::Error(err) => err,
        }
    }
}

impl From<SqsSinkError> for Error {
    fn from(value: SqsSinkError) -> Self {
        match value {
            SqsSinkError::Error(err) => err,
        }
    }
}

/// AWS SDK behavior version to use across the library
pub fn aws_behavior_version() -> BehaviorVersion {
    BehaviorVersion::v2025_08_07()
}

/// Creates and configures an SQS client based on the provided configuration.
pub async fn create_sqs_client(config: SqsConfig) -> Result<Client, Error> {
    let (region, assume_role_config, endpoint_url) = match &config {
        SqsConfig::Source(cfg) => (
            cfg.region,
            cfg.assume_role_config.as_ref(),
            cfg.endpoint_url.as_deref(),
        ),
        SqsConfig::Sink(cfg) => (
            cfg.region,
            cfg.assume_role_config.as_ref(),
            None, // Sink doesn't support endpoint_url
        ),
    };

    let region = Region::new(region);
    let region_provider = RegionProviderChain::first_try(region.clone())
        .or_default_provider()
        .or_else(Region::new("us-west-2"));

    // Create appropriate credential provider (default or assume role)
    let credentials_provider =
        create_credentials_provider(region, assume_role_config, endpoint_url).await?;

    // Build AWS config with credential provider
    let mut config_builder = aws_config::defaults(aws_behavior_version())
        .region(region_provider)
        .credentials_provider(credentials_provider);

    // Apply endpoint URL if configured
    if let Some(endpoint_url) = endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    let shared_config = config_builder.load().await;
    Ok(Client::new(&shared_config))
}

/// Creates a credential provider based on the assume role configuration
pub async fn create_credentials_provider(
    region: Region,
    assume_role_config: Option<&AssumeRoleConfig>,
    endpoint_url: Option<&str>,
) -> Result<Arc<dyn ProvideCredentials>, Error> {
    let Some(assume_role_config) = assume_role_config else {
        // Use default credential chain when assume role is not configured
        tracing::info!("Using default AWS credential chain");
        return Ok(Arc::new(CredentialsProviderChain::default_provider().await));
    };

    let role_arn = &assume_role_config.role_arn;
    tracing::info!(role_arn = %role_arn, "Creating STS assume role credential provider");

    // Create assume role credential provider with automatic refresh
    // The AssumeRoleProvider handles STS client creation internally
    let mut provider_builder = aws_config::sts::AssumeRoleProvider::builder(role_arn).session_name(
        assume_role_config
            .session_name
            .as_deref()
            .unwrap_or("numaflow-sqs-client"),
    );

    if let Some(duration) = assume_role_config.duration_seconds {
        provider_builder =
            provider_builder.session_length(std::time::Duration::from_secs(duration as u64));
    }

    if let Some(external_id) = &assume_role_config.external_id {
        provider_builder = provider_builder.external_id(external_id);
    }

    // Add inline session policy if provided
    if let Some(policy) = &assume_role_config.policy {
        provider_builder = provider_builder.policy(policy);
    }

    // Add managed policy ARNs if provided
    if let Some(policy_arns) = &assume_role_config.policy_arns {
        if !policy_arns.is_empty() {
            // AssumeRoleProvider expects Vec<String> for policy ARNs
            provider_builder = provider_builder.policy_arns(policy_arns.clone());
        }
    }

    // Set region and endpoint if needed
    if let Some(endpoint_url) = endpoint_url {
        // For LocalStack or custom endpoints, we need to build the config differently
        let sts_config_builder = aws_config::defaults(aws_behavior_version())
            .region(region.clone())
            .endpoint_url(endpoint_url);
        let sts_config = sts_config_builder.load().await;
        provider_builder = provider_builder.configure(&sts_config);
    } else {
        let sts_config = aws_config::defaults(aws_behavior_version())
            .region(region.clone())
            .load()
            .await;
        provider_builder = provider_builder.configure(&sts_config);
    }

    let credential_provider = provider_builder.build().await;

    Ok(Arc::new(credential_provider))
}

#[cfg(test)]
mod tests {
    use aws_smithy_mocks::{MockResponseInterceptor, RuleMode, mock};
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
                .behavior_version(aws_behavior_version())
                .region(aws_sdk_sqs::config::Region::new("us-east-1"))
                .credentials_provider(make_sqs_test_credentials())
                .interceptor(get_object_mocks)
                .build(),
        );
        let err = sqs.get_queue_url().send().await.unwrap_err();

        let converted_error = Error::Sqs(err.into());
        assert!(matches!(converted_error, Error::Sqs(_)));
        assert!(
            converted_error
                .to_string()
                .contains("Failed with SQS error")
        );
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

    #[tokio::test]
    async fn test_default_credential_provider() {
        let region = Region::new("us-west-2");
        let result = create_credentials_provider(region, None, None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_assume_role_credential_provider() {
        let region = Region::new("us-west-2");
        let assume_role = AssumeRoleConfig {
            role_arn: "arn:aws:iam::123456789012:role/test-role".to_string(),
            session_name: Some("test-session".to_string()),
            duration_seconds: Some(3600),
            external_id: None,
            policy: None,
            policy_arns: None,
        };

        let result = create_credentials_provider(region, Some(&assume_role), None).await;
        assert!(result.is_ok());
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
