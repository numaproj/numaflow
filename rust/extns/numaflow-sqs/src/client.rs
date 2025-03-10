use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_sqs::Client;

use super::{Error, Result};
use crate::source::SQSSourceConfig;

/// Creates and configures an SQS client based on the provided configuration.
pub async fn create_sqs_client(config: Option<SQSSourceConfig>) -> Result<Client> {
    let config = match config {
        Some(cfg) => cfg,
        None => {
            return Err(Error::InvalidConfig(
                "SQS configuration is required".to_string(),
            ))
        }
    };

    // Validate configuration before proceeding
    config.validate()?;

    tracing::info!(
        region = config.region.clone(),
        "Creating SQS client in region"
    );

    let region_provider = RegionProviderChain::first_try(Region::new(config.region.clone()))
        .or_default_provider()
        .or_else(Region::new("us-west-2")); // Default region if none provided

    let mut config_builder =
        aws_config::defaults(BehaviorVersion::v2024_03_28()).region(region_provider);

    // Apply endpoint URL if configured
    if let Some(endpoint_url) = config.endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    // Load the shared config
    let shared_config = config_builder.load().await;

    // Create and return the client
    Ok(Client::new(&shared_config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{AWSCredentials, SQSAuth};

    #[tokio::test]
    async fn test_client_creation_with_defaults() {
        let config = SQSSourceConfig {
            region: "us-west-2".to_string(),
            queue_name: "test-queue".to_string(),
            auth: SQSAuth::Credentials(AWSCredentials {
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            }),
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
        };

        let result = create_sqs_client(Some(config)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_client_creation_with_custom_endpoint() {
        let mut config = SQSSourceConfig {
            region: "us-west-2".to_string(),
            queue_name: "test-queue".to_string(),
            auth: SQSAuth::Credentials(AWSCredentials {
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            }),
            visibility_timeout: Some(30),
            max_number_of_messages: Some(5),
            wait_time_seconds: Some(10),
            endpoint_url: Some("http://localhost:4566".to_string()),
            attribute_names: vec!["All".to_string()],
            message_attribute_names: vec!["All".to_string()],
        };

        let result = create_sqs_client(Some(config.clone())).await;
        assert!(result.is_ok());

        // Test with invalid endpoint
        config.endpoint_url = Some("invalid-url".to_string());
        let result = create_sqs_client(Some(config)).await;
        assert!(result.is_ok()); // The URL is validated when making requests, not during client creation
    }

    #[tokio::test]
    async fn test_client_creation_validation_failures() {
        // Test missing config
        let result = create_sqs_client(None).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));

        // Test empty region
        let config = SQSSourceConfig {
            region: "".to_string(),
            queue_name: "test-queue".to_string(),
            auth: SQSAuth::Credentials(AWSCredentials {
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            }),
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
        };
        let result = create_sqs_client(Some(config)).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn test_client_creation_with_invalid_parameters() {
        let config = SQSSourceConfig {
            region: "us-west-2".to_string(),
            queue_name: "test-queue".to_string(),
            auth: SQSAuth::Credentials(AWSCredentials {
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            }),
            visibility_timeout: Some(50000),  // Invalid: > 43200
            max_number_of_messages: Some(20), // Invalid: > 10
            wait_time_seconds: Some(30),      // Invalid: > 20
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
        };

        let result = create_sqs_client(Some(config)).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }
}
