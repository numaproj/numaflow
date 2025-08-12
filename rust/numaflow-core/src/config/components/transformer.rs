const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_TRANSFORMER_SOCKET: &str = "/var/run/numaflow/sourcetransform.sock";
const DEFAULT_TRANSFORMER_SERVER_INFO_FILE: &str =
    "/var/run/numaflow/sourcetransformer-server-info";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TransformerConfig {
    pub(crate) concurrency: usize,
    pub(crate) transformer_type: TransformerType,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TransformerType {
    #[allow(dead_code)]
    Noop(NoopConfig),
    UserDefined(UserDefinedConfig),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NoopConfig {}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UserDefinedConfig {
    pub grpc_max_message_size: usize,
    pub socket_path: String,
    pub server_info_path: String,
}

impl Default for UserDefinedConfig {
    fn default() -> Self {
        Self {
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            socket_path: DEFAULT_TRANSFORMER_SOCKET.to_string(),
            server_info_path: DEFAULT_TRANSFORMER_SERVER_INFO_FILE.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_user_defined_config() {
        let default_config = UserDefinedConfig::default();
        assert_eq!(default_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(
            default_config.socket_path,
            "/var/run/numaflow/sourcetransform.sock"
        );
        assert_eq!(
            default_config.server_info_path,
            "/var/run/numaflow/sourcetransformer-server-info"
        );
    }

    #[test]
    fn test_transformer_config_user_defined() {
        let user_defined_config = UserDefinedConfig::default();
        let transformer_config = TransformerConfig {
            concurrency: 1,
            transformer_type: TransformerType::UserDefined(user_defined_config.clone()),
        };
        if let TransformerType::UserDefined(config) = transformer_config.transformer_type {
            assert_eq!(config, user_defined_config);
        } else {
            panic!("Expected TransformerType::UserDefined");
        }
    }
}
