use base64::Engine;
use base64::prelude::BASE64_STANDARD;

const ENV_SIDE_INPUT_OBJECT: &str = "NUMAFLOW_SIDE_INPUT_OBJECT";

/// Configurations for the side-input trigger from the pipeline spec.
pub(crate) struct SideInputTriggerConfig {
    pub(crate) name: &'static str,
    pub(crate) schedule: &'static str,
    pub(crate) timezone: Option<&'static str>,
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

pub(crate) mod isb {
    use crate::error::{Error, Result};
    use std::collections::HashMap;

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

    const ENV_NUMAFLOW_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
    const ENV_NUMAFLOW_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";
    const ENV_NUMAFLOW_JETSTREAM_URL: &str = "NUMAFLOW_ISBSVC_JETSTREAM_URL";
    const ENV_NUMAFLOW_JETSTREAM_TLS_ENABLED: &str = "NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED";

    impl ClientConfig {
        pub(crate) fn load(
            env_vars: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        ) -> Result<Self> {
            let env_vars: HashMap<String, String> = env_vars
                .into_iter()
                .map(|(key, val)| (key.into(), val.into()))
                .filter(|(key, _val)| {
                    [
                        ENV_NUMAFLOW_JETSTREAM_URL,
                        ENV_NUMAFLOW_JETSTREAM_PASSWORD,
                        ENV_NUMAFLOW_JETSTREAM_USER,
                        ENV_NUMAFLOW_JETSTREAM_TLS_ENABLED,
                    ]
                    .contains(&key.as_str())
                })
                .collect();

            let get_var = |var: &str| -> Result<String> {
                Ok(env_vars
                    .get(var)
                    .ok_or_else(|| Error::Config(format!("Environment variable {var} is not set")))?
                    .to_string())
            };

            Ok(Self {
                url: get_var(ENV_NUMAFLOW_JETSTREAM_URL)?,
                user: get_var(ENV_NUMAFLOW_JETSTREAM_USER).ok(),
                password: get_var(ENV_NUMAFLOW_JETSTREAM_PASSWORD).ok(),
            })
        }
    }
}
