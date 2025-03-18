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
use tracing::error;

use crate::error::{Error, Result};

#[derive(Serialize, Deserialize, Debug)]
pub struct RuntimeErrorEntry {
    pub container: String,
    pub timestamp: String,
    pub code: String,
    pub message: String,
    pub details: String,
}

#[derive(serde::Serialize)]
pub struct ApiResponse {
    pub error_message: Option<String>,
    pub data: Vec<RuntimeErrorEntry>,
}

/// Runtime is used for persisting runtime information such as application errors.
pub struct Runtime {
    application_error_path: String,
}

impl Runtime {
    // Creates a new Runtime instance with the specified directory path, where the runtime information
    // will be persisted.
    pub fn new(empty_dir_path: &str) -> Self {
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

    // get the application errors persisted in app err directory
    pub fn get_application_errors(&self) -> Result<Vec<RuntimeErrorEntry>> {
        let app_err_path = Path::new(&self.application_error_path);
        let mut errors = Vec::new();
        // if no app errors are persisted, directory wouldn't be created yet
        if !app_err_path.exists() || !app_err_path.is_dir() {
            let err = Error::File("App Err path does not exist".to_string());
            error!("{}", err);
            return Err(err);
        }
        let paths = match fs::read_dir(app_err_path) {
            Ok(path) => path,
            Err(e) => {
                let err = Error::File(format!("Failed to read directory: {:?}", e));
                return Err(err);
            }
        };

        // iterate over all subdirectories and its files
        for entry in paths.flatten() {
            // UD container will have its own directory
            let sub_dir_path = entry.path();
            if !sub_dir_path.is_dir() {
                continue;
            }
            match fs::read_dir(&sub_dir_path) {
                Err(e) => {
                    error!(
                        "{}",
                        Error::File(format!("Failed to read subdirectory: {:?}", e))
                    );
                    continue;
                }
                Ok(file_paths) => {
                    for file_entry in file_paths.flatten() {
                        // process content of each file into error entry
                        if let Err(e) = process_file_entry(&file_entry, &mut errors) {
                            error!(
                                "{}",
                                Error::File(format!(
                                    "error: {} in processing file entry: {:?}",
                                    e,
                                    file_entry.file_name()
                                ))
                            );
                            continue;
                        }
                    }
                }
            }
        }
        Ok(errors)
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

fn process_file_entry(
    file_entry: &fs::DirEntry,
    errors: &mut Vec<RuntimeErrorEntry>,
) -> Result<()> {
    let file_path = file_entry.path();
    // if file path isn't a file, continue
    if !file_path.is_file() || !file_path.exists() {
        return Ok(());
    }
    match fs::read(&file_path) {
        // log the error and continue processing other files
        Err(e) => {
            let err = Error::File(format!("Failed to read file content: {:?}", e));
            error!("{}", err);
            Err(err)
        }
        // save the file content in errors
        Ok(content) => match serde_json::from_slice::<RuntimeErrorEntry>(&content) {
            Ok(payload) => {
                errors.push(RuntimeErrorEntry {
                    container: payload.container.to_string(),
                    timestamp: payload.timestamp,
                    code: payload.code,
                    message: payload.message,
                    details: payload.details,
                });
                Ok(())
            }
            Err(e) => {
                let err = Error::Deserialize(format!("Failed to deserialize content: {:?}", e));
                error!("{}", err);
                Err(err)
            }
        },
    }
}

// TODO: add unit tests
