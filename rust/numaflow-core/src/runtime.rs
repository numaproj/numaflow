use chrono::Utc;
use regex::Regex;

use crate::config::get_vertex_replica;
use numaflow_pb::clients::mvtxdaemon::mono_vertex_daemon_service_client::MonoVertexDaemonServiceClient;
use numaflow_pb::clients::mvtxdaemon::PersistRuntimeErrorRequest;
use std::str;
use tonic::transport::Channel;
use tonic::Status;
use tracing::error;

#[derive(Clone)]
pub(crate) struct Runtime {
    pub(crate) client: MonoVertexDaemonServiceClient<Channel>,
}

impl Runtime {
    /// Creates a new Runtime instance with the specified emptyDir path.
    pub(crate) async fn new(addr: String) -> Self {
        let client = MonoVertexDaemonServiceClient::connect(addr)
            .await
            .expect("failed to connect to daemon server");
        Runtime { client }
    }

    // Call daemon server
    pub async fn persist_application_error(&mut self, grpc_status: Status) {
        // we can extract the type of udf based on the error message
        let container_name =
            extract_container_name(grpc_status.message()).expect("container name not found");

        let timestamp = Utc::now().to_rfc3339();
        let replica = get_vertex_replica().to_string();
        let code = grpc_status.code().to_string();
        let message = grpc_status.message().to_string();
        let details = String::from_utf8_lossy(grpc_status.details()).to_string();

        let request = PersistRuntimeErrorRequest {
            container_name,
            timestamp,
            code,
            message,
            details,
            replica,
        };

        if let Err(e) = self.client.persist_runtime_error(request).await {
            error!(
                ?e,
                "failed to post runtime error information to daemon server"
            );
        };
    }
}

/// extracts the container information from the error message by doing regex matching
fn extract_container_name(error_message: &str) -> Option<String> {
    let re = Regex::new(r"\((.*?)\)").expect("error message should have container information");
    re.captures(error_message)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
}
