use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str;
use std::{fs, io};
use tonic::Status;
pub struct Runtime {
    empty_dir_path: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RuntimeErrorEntry {
    pub container_name: String,
    pub timestamp: String,
    pub code: String,
    pub message: String,
    pub details: String,
}

impl Runtime {
    // Creates a new Runtime instance with the specified directory path.
    pub fn new(empty_dir_path: &str) -> Self {
        Runtime {
            empty_dir_path: empty_dir_path.to_string(),
        }
    }

    // Persists the data in file storage
    pub fn persist_application_error(&self, grpc_status: Status) -> io::Result<()> {
        // extract the type of udf based on the error message
        let container_name = match get_container_name(grpc_status.message()) {
            Ok(name) => name,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        };

        let dir_path = Path::new(&self.empty_dir_path)
            .join("application-errors")
            .join(&container_name);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        let timestamp = Utc::now().timestamp();
        let file_name = format!("{}.json", timestamp);

        let json_str = grpc_status_to_json(&grpc_status, container_name.as_str(), timestamp)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let file_path = dir_path.join(file_name);
        let mut file = File::create(&file_path)?;
        //TODO: check if we already have 10 files for a container, if yes, remove the oldest and write new one
        file.write_all(json_str.as_bytes())?;

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

// Converts gRPC status to a JSON object.
pub fn grpc_status_to_json(
    grpc_status: &Status,
    container_name: &str,
    timestamp: i64,
) -> Result<String, Box<dyn Error>> {
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
        container_name: container_name.to_string(),
        timestamp: rfc3339_timestamp,
        code,
        message,
        details: details_str.to_string(),
    };

    // Serialize the RuntimeErrorEntry instance to a JSON string
    let json_str = serde_json::to_string(&runtime_error_entry)?;
    Ok(json_str)
}
