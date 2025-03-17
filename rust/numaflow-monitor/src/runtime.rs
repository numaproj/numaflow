//! Runtime module is used for persisting runtime information such as application errors.
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str;
use tonic::Status;

#[derive(Serialize, Deserialize, Debug)]
pub struct RuntimeErrorEntry {
    pub container: String,
    pub timestamp: String,
    pub code: String,
    pub message: String,
    pub details: String,
}

/// Runtime is used for persisting runtime information such as application errors.
pub struct Runtime {
    application_error_path: String,
}

impl Runtime {
    // Creates a new Runtime instance with the specified directory path, where the runtime information
    // will be persisted.
    pub fn new(empty_dir_path: &str) -> Self {
        // FIXME: create  /application-errors
        Runtime {
            application_error_path: empty_dir_path.to_string(),
        }
    }

    // Persists the application errors.
    pub fn persist_application_error(&self, grpc_status: Status) {
        // extract the type of udf based on the error message
        let container_name = extract_container_name(grpc_status.message());

        let dir_path = Path::new(&self.application_error_path).join(&container_name);

        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).expect("Failed to create application errors directory");
        }

        let timestamp = Utc::now().timestamp();
        let file_name = format!("{}.json", timestamp);

        let json_str = grpc_status_to_json(&grpc_status, container_name.as_str(), timestamp);

        let file_path = dir_path.join(file_name);
        let mut file = File::create(&file_path).expect("Failed to create application errors file");
        //TODO: check if we already have 'x' files for a container, if yes, remove the oldest and write new one
        file.write_all(json_str.as_bytes())
            .expect("Failed to write to application error file");
    }
}

/// extracts the container information from the error message
fn extract_container_name(error_message: &str) -> String {
    let re = Regex::new(r"\((.*?)\)").unwrap();
    re.captures(error_message)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .expect(
            format!(
                "Failed to extract container name from the error message: {}",
                error_message
            )
            .as_str(),
        )
}

/// Converts gRPC status to a JSON object.
fn grpc_status_to_json(grpc_status: &Status, container_name: &str, timestamp: i64) -> String {
    // Extract code, message, and details from gRPC Status
    let code = grpc_status.code().to_string();
    let message = grpc_status.message().to_string();
    let details_bytes = grpc_status.details();

    // Convert status details from bytes to a string
    let details_str = String::from_utf8_lossy(details_bytes);

    // Convert timestamp to RFC 3339 string
    let datetime = DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap();
    let rfc3339_timestamp = datetime.to_rfc3339();
    // Create a RuntimeErrorEntry instance
    let runtime_error_entry = RuntimeErrorEntry {
        container: container_name.to_string(),
        timestamp: rfc3339_timestamp,
        code,
        message,
        details: details_str.to_string(),
    };

    // Serialize the RuntimeErrorEntry instance to a JSON string
    serde_json::to_string(&runtime_error_entry).expect("Failed to serialize runtime error message")
}

// TODO: add unit tests
