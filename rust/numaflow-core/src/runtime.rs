use chrono::Utc;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;

use std::str;
use tonic::Status;

use crate::config::{get_vertex_name, get_vertex_replica};
pub struct Runtime {}

#[derive(Serialize, Deserialize, Debug)]
pub struct RuntimeErrorEntry {
    pub container_name: String,
    pub timestamp: String,
    pub code: String,
    pub message: String,
    pub details: String,
    pub mvtx_name: String,
    pub replica: String,
}

impl Runtime {
    /// Creates a new Runtime instance with the specified emptyDir path.
    pub fn new() -> Self {
        Runtime {}
    }

    // Call daemon server
    pub async fn persist_application_error(
        &self,
        grpc_status: Status,
    ) -> Result<(), Box<dyn Error>> {
        // we can extract the type of udf based on the error message
        let container_name = match get_container_name(grpc_status.message()) {
            Ok(name) => name,
            Err(_err) => String::from(""),
        };

        let timestamp = Utc::now().to_rfc3339();
        let vertex_name = get_vertex_name().to_string();
        let replica = get_vertex_replica().to_string();
        let code = grpc_status.code().to_string();
        let message = grpc_status.message().to_string();
        let details_bytes = grpc_status.details();
        let details_str = String::from_utf8_lossy(details_bytes).to_string();
        let daemon_url =
            "http://simple-mono-vertex-mv-daemon-svc.default.svc.cluster.local:4327/api/v1/runtime/errors";

        let error_entry = RuntimeErrorEntry {
            container_name,
            timestamp,
            code,
            message,
            details: details_str,
            mvtx_name: vertex_name,
            replica,
        };

        let client = Client::new();
        let response = client.post(daemon_url).json(&error_entry).send().await?;

        if response.status().is_success() {
            println!("Runtime error reported successfully");
        } else {
            println!("Failed to report runtime error: {}", response.status());
        }

        Ok(())
    }
}

fn get_container_name(error_message: &str) -> Result<String, String> {
    extract_container_name(error_message)
        .ok_or_else(|| "Failed to extract container name from error message".to_string())
}

fn extract_container_name(error_message: &str) -> Option<String> {
    let re = Regex::new(r"\((.*?)\)").unwrap();
    re.captures(error_message)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
}
