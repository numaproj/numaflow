//! Runtime module is used for persisting runtime information such as application errors.
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str;
use std::{fs, io::Result as io_result};
use tonic::Status;
use tracing::error;

use crate::config::{
    RuntimeInfoConfig, DEFAULT_MAX_ERROR_FILES_PER_CONTAINER,
    DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH,
};
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
    max_error_files_per_container: usize,
}

impl Runtime {
    // Creates a new Runtime instance with the specified directory path, where the runtime information
    // will be persisted.
    pub fn new(runtime_info_config: Option<RuntimeInfoConfig>) -> Self {
        let config = runtime_info_config.unwrap_or_else(|| RuntimeInfoConfig {
            app_error_path: (DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH.to_string()),
            max_error_files_per_container: (DEFAULT_MAX_ERROR_FILES_PER_CONTAINER),
        });

        Runtime {
            application_error_path: config.app_error_path,
            max_error_files_per_container: config.max_error_files_per_container,
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

        // Check the number of files in the directory
        let mut files: Vec<_> = fs::read_dir(&dir_path)
            .expect("Failed to read application errors directory")
            .filter_map(io_result::ok)
            .filter(|e| e.path().is_file())
            .collect();

        // Sort files by their names (timestamps)
        files.sort_by_key(|e| e.file_name());

        // Remove the oldest file if the number of files exceeds the limit
        if files.len() >= self.max_error_files_per_container {
            if let Some(oldest_file) = files.first() {
                fs::remove_file(oldest_file.path())
                    .expect("Failed to remove the oldest application error file");
            }
        }

        let timestamp = Utc::now().timestamp();
        let file_name = format!("{}.json", timestamp);

        let json_str = grpc_status_to_json(&grpc_status, container_name.as_str(), timestamp);

        let temp_file_path = dir_path.join("current.json");
        let final_file_path = dir_path.join(&file_name);

        // Write to the temporary current.json file to avoid concurrent read/write on the same file
        let mut temp_file = File::create(&temp_file_path)
            .expect("Failed to create temporary application errors file");
        temp_file
            .write_all(json_str.as_bytes())
            .expect("Failed to write to temporary application error file");

        // Rename the temporary file to the final file name once write operation completes
        fs::rename(&temp_file_path, &final_file_path)
            .expect("Failed to rename temporary file to final file name");
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
    let file_name_to_ignore = Some(std::ffi::OsStr::new("current.json"));
    // if file path isn't a file or file name is current.json, continue
    if !file_path.exists() || !file_path.is_file() || file_path.file_name() == file_name_to_ignore {
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
