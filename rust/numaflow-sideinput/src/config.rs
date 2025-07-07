use base64::Engine;
use base64::prelude::BASE64_STANDARD;

const ENV_SIDE_INPUT_OBJECT: &str = "NUMAFLOW_SIDE_INPUT_OBJECT";

/// Configurations for the side-input trigger from the pipeline spec.
pub(crate) struct SideInputTriggerConfig {
    pub(crate) name: String,
    pub(crate) schedule: String,
    pub(crate) timezone: Option<String>,
}

impl SideInputTriggerConfig {
    pub(crate) fn load(env_vars: std::collections::HashMap<String, String>) -> Self {
        let side_input_obj = env_vars
            .get(ENV_SIDE_INPUT_OBJECT)
            .expect("{ENV_SIDE_INPUT_OBJECT} not found");

        let decoded_spec = BASE64_STANDARD
            .decode(side_input_obj)
            .expect("Failed to decode side input spec");

        let side_input_obj: numaflow_models::models::SideInput =
            serde_json::from_slice(&decoded_spec).expect("Failed to parse side input spec as JSON");

        let name = side_input_obj.name.clone();
        let schedule = side_input_obj.trigger.schedule.clone();
        let timezone = side_input_obj.trigger.timezone.clone();

        SideInputTriggerConfig {
            name,
            schedule,
            timezone,
        }
    }
}

pub(crate) mod isb {
    const DEFAULT_URL: &str = "localhost:4222";
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct ClientConfig {
        pub url: String,
        pub user: Option<String>,
        pub password: Option<String>,
    }

    impl Default for ClientConfig {
        fn default() -> Self {
            ClientConfig {
                url: DEFAULT_URL.to_string(),
                user: None,
                password: None,
            }
        }
    }
}
