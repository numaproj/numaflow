use base64::Engine;
use base64::prelude::BASE64_STANDARD;

/// Configurations for the side-input trigger from the pipeline spec.
pub(crate) struct SideInputTriggerConfig {
    pub(crate) name: &'static str,
    pub(crate) schedule: &'static str,
    pub(crate) timezone: Option<&'static str>,
}

impl SideInputTriggerConfig {
    const ENV_SIDE_INPUT_OBJECT: &'static str = "NUMAFLOW_SIDE_INPUT_OBJECT";

    /// Load the SideInputTriggerConfig from the environment variables.
    pub(crate) fn load(env_vars: std::collections::HashMap<String, String>) -> Self {
        let side_input_obj = env_vars
            .get(Self::ENV_SIDE_INPUT_OBJECT)
            .expect("{ENV_SIDE_INPUT_OBJECT} not found");

        let decoded_spec = BASE64_STANDARD
            .decode(side_input_obj)
            .expect("Failed to decode side input spec");

        let side_input_obj: numaflow_models::models::SideInput =
            serde_json::from_slice(&decoded_spec).expect("Failed to parse side input spec as JSON");

        let name = side_input_obj.name.clone();
        let schedule = side_input_obj.trigger.schedule.clone();
        let timezone: Option<&'static str> = side_input_obj
            .trigger
            .timezone
            .map(|tz| Box::leak(tz.into_boxed_str()) as &str);

        SideInputTriggerConfig {
            name: Box::leak(name.into_boxed_str()),
            schedule: Box::leak(schedule.into_boxed_str()),
            timezone,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_side_input_trigger_config_load_success() {
        // Create a valid SideInput JSON object
        let side_input = numaflow_models::models::SideInput {
            container: Box::new(numaflow_models::models::Container::new()),
            name: "test-sideinput".to_string(),
            trigger: Box::new(numaflow_models::models::SideInputTrigger {
                schedule: "0 0 * * *".to_string(),
                timezone: Some("UTC".to_string()),
            }),
            volumes: None,
        };

        let json_str = serde_json::to_string(&side_input).unwrap();
        let encoded = BASE64_STANDARD.encode(json_str.as_bytes());

        let mut env_vars = HashMap::new();
        env_vars.insert(
            SideInputTriggerConfig::ENV_SIDE_INPUT_OBJECT.to_string(),
            encoded,
        );

        let config = SideInputTriggerConfig::load(env_vars);

        assert_eq!(config.name, "test-sideinput");
        assert_eq!(config.schedule, "0 0 * * *");
        assert_eq!(config.timezone, Some("UTC"));
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn test_side_input_trigger_config_load_missing_env_var() {
        let env_vars = HashMap::new();
        SideInputTriggerConfig::load(env_vars);
    }

    #[test]
    #[should_panic(expected = "Failed to decode")]
    fn test_side_input_trigger_config_load_invalid_base64() {
        let mut env_vars = HashMap::new();
        env_vars.insert(
            SideInputTriggerConfig::ENV_SIDE_INPUT_OBJECT.to_string(),
            "invalid-base64!@#".to_string(),
        );

        SideInputTriggerConfig::load(env_vars);
    }

    #[test]
    #[should_panic(expected = "Failed to parse")]
    fn test_side_input_trigger_config_load_invalid_json() {
        let invalid_json = "not a json";
        let encoded = BASE64_STANDARD.encode(invalid_json.as_bytes());

        let mut env_vars = HashMap::new();
        env_vars.insert(
            SideInputTriggerConfig::ENV_SIDE_INPUT_OBJECT.to_string(),
            encoded,
        );

        SideInputTriggerConfig::load(env_vars);
    }

    mod isb_tests {
        use super::*;
        use numaflow_shared::isb::jetstream::config::ClientConfig;

        #[test]
        fn test_client_config_default() {
            let config = ClientConfig::default();
            assert_eq!(config.url, "localhost:4222");
            assert_eq!(config.user, None);
            assert_eq!(config.password, None);
        }

        #[test]
        fn test_client_config_load_success_all_vars() {
            let mut env_vars = HashMap::new();
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
                "nats://localhost:4222".to_string(),
            );
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_USER".to_string(),
                "testuser".to_string(),
            );
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD".to_string(),
                "testpass".to_string(),
            );
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED".to_string(),
                "true".to_string(),
            );

            let config = ClientConfig::load(env_vars).unwrap();
            assert_eq!(config.url, "nats://localhost:4222");
            assert_eq!(config.user, Some("testuser".to_string()));
            assert_eq!(config.password, Some("testpass".to_string()));
        }

        #[test]
        fn test_client_config_load_success_only_url() {
            let mut env_vars = HashMap::new();
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
                "nats://localhost:4222".to_string(),
            );

            let config = ClientConfig::load(env_vars).unwrap();
            assert_eq!(config.url, "nats://localhost:4222");
            assert_eq!(config.user, None);
            assert_eq!(config.password, None);
        }

        #[test]
        fn test_client_config_load_missing_url() {
            let mut env_vars = HashMap::new();
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_USER".to_string(),
                "testuser".to_string(),
            );

            let result = ClientConfig::load(env_vars);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("NUMAFLOW_ISBSVC_JETSTREAM_URL is not set")
            );
        }

        #[test]
        fn test_client_config_load_filters_irrelevant_vars() {
            let mut env_vars = HashMap::new();
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
                "nats://localhost:4222".to_string(),
            );
            env_vars.insert(
                "IRRELEVANT_VAR".to_string(),
                "should_be_ignored".to_string(),
            );
            env_vars.insert("ANOTHER_VAR".to_string(), "also_ignored".to_string());

            let config = ClientConfig::load(env_vars).unwrap();
            assert_eq!(config.url, "nats://localhost:4222");
            assert_eq!(config.user, None);
            assert_eq!(config.password, None);
        }
    }
}
