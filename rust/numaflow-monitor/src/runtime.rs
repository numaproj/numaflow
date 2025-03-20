//! The `runtime` module is responsible for persisting runtime information, such as application errors.
use crate::config::RuntimeInfoConfig;
use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str;
use tonic::Status;
use tracing::error;

const CURRENT_FILE: &str = "current.json";

/// Represents a single runtime error entry persisted by the application.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct RuntimeErrorEntry {
    /// The name of the container where the error occurred.
    pub(crate) container: String,
    /// The timestamp of the error in RFC 3339 format.
    pub(crate) timestamp: String,
    /// The error code.
    pub(crate) code: String,
    /// The error message.
    pub(crate) message: String,
    /// Additional details, such as the error stack trace.
    pub(crate) details: String,
}

impl TryFrom<&[u8]> for RuntimeErrorEntry {
    type Error = Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        serde_json::from_slice::<RuntimeErrorEntry>(value)
            .map_err(|e| Error::Deserialize(format!("{:?}", e)))
    }
}

impl From<(&Status, &str, i64)> for RuntimeErrorEntry {
    fn from(value: (&Status, &str, i64)) -> RuntimeErrorEntry {
        let (grpc_status, container_name, timestamp) = value;

        // Extract code, message, and details from gRPC Status
        let code = grpc_status.code().to_string();
        let message = grpc_status.message().to_string();
        let details_bytes = grpc_status.details();

        // Convert status details from bytes to a string
        let details_str = String::from_utf8_lossy(details_bytes);

        // Convert timestamp to RFC 3339 string
        let rfc3339_timestamp = DateTime::<Utc>::from_timestamp(timestamp, 0)
            .unwrap()
            .to_rfc3339();

        RuntimeErrorEntry {
            container: container_name.to_string(),
            timestamp: rfc3339_timestamp,
            code,
            message,
            details: details_str.to_string(),
        }
    }
}

impl From<RuntimeErrorEntry> for String {
    fn from(val: RuntimeErrorEntry) -> Self {
        serde_json::to_string(&val).expect("Failed to serialize runtime error message")
    }
}

/// A structure used to represent API responses containing runtime error entries.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ApiResponse {
    /// Optional error message for the API response.
    pub(crate) error_message: Option<String>,
    /// A list of `RuntimeErrorEntry` objects
    pub(crate) data: Vec<RuntimeErrorEntry>,
}

/// Response for managing runtime error persistence and retrieval.
pub struct Runtime {
    /// The root directory where error files are stored.
    application_error_path: String,
    /// The maximum number of error files allowed per container.
    max_error_files_per_container: usize,
}

impl Runtime {
    // Creates a new Runtime instance
    pub fn new(runtime_info_config: Option<RuntimeInfoConfig>) -> Self {
        let config = runtime_info_config.unwrap_or_default();
        Runtime {
            application_error_path: config.app_error_path,
            max_error_files_per_container: config.max_error_files_per_container,
        }
    }

    /// Persists a gRPC error as a JSON file in the appropriate container directory. Automatically manages
    /// the number of files by removing the oldest file if the max file limit is exceeded.
    pub fn persist_application_error(&self, grpc_status: Status) {
        // extract the type of udf container based on the error message
        let container_name = extract_container_name(grpc_status.message());
        // create a directory for the container if it doesn't exist
        let dir_path = Path::new(&self.application_error_path).join(&container_name);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path).expect("Failed to create application errors directory");
        }

        // to check the number of files in the directory
        let mut files: Vec<_> = fs::read_dir(&dir_path)
            .expect("Failed to read application errors directory")
            .filter_map(|entry| entry.ok())
            .collect();

        // sort the files based on timestamp
        files.sort_by_key(|e| e.file_name());

        // remove the oldest file if the number of files exceeds the limit
        if files.len() >= self.max_error_files_per_container {
            if let Some(oldest_file) = files.first() {
                fs::remove_file(oldest_file.path())
                    .expect("Failed to remove the oldest application error file");
            }
        }

        let timestamp = Utc::now().timestamp();
        let runtime_error_entry =
            RuntimeErrorEntry::from((&grpc_status, container_name.as_str(), timestamp));
        let json_str: String = runtime_error_entry.into();

        // we create a file current.json and write into this file first
        // rename it back to <timestamp>.json once write operation is completed
        // this is to ensure that while reading we skip this file to avoid race condition
        let current_file_path = dir_path.join(CURRENT_FILE);
        let file_name = format!("{}.json", timestamp);
        let final_file_path = dir_path.join(&file_name);

        let mut current_file = File::create(&current_file_path)
            .expect("Failed to create current application errors file");
        current_file
            .write_all(json_str.as_bytes())
            .expect("Failed to write to current application error file");

        // rename the current file to the final file name once write operation completes
        fs::rename(&current_file_path, &final_file_path)
            .expect("Failed to rename current file to final file name");
    }

    // ## File Structure
    // The runtime module organizes application error files in a directory structure as follows:
    // Root: /var/numaflow/runtime/
    // └── application-errors
    //     └── <container-name>/
    //         ├── <timestamp1>.json
    //         ├── <timestamp2>.json

    /// Retrieves all persisted application errors from the error directory.
    pub(crate) fn get_application_errors(&self) -> Result<Vec<RuntimeErrorEntry>> {
        let app_err_path = Path::new(&self.application_error_path);
        let mut errors = Vec::new();
        // if no app errors are persisted, directory wouldn't be created yet
        if !app_err_path.exists() || !app_err_path.is_dir() {
            let err = Error::File("No application errors persisted yet".to_string());
            return Err(err);
        }

        let paths = fs::read_dir(app_err_path)
            .map_err(|e| Error::File(format!("Failed to read directory: {:?}", e)))?;

        // iterate over all subdirectories and its files
        for entry in paths.flatten() {
            // ud container will have its own directory
            let sub_dir_path = entry.path();
            if !sub_dir_path.is_dir() {
                continue;
            }

            let file_paths = match fs::read_dir(&sub_dir_path) {
                Ok(paths) => paths,
                Err(e) => {
                    error!(
                        "{}",
                        Error::File(format!("Failed to read subdirectory: {:?}", e))
                    );
                    continue;
                }
            };

            for file_entry in file_paths.flatten() {
                // skip processing if the file name is "current.json"
                if file_entry
                    .file_name()
                    .to_str()
                    .expect("file name should be valid")
                    == CURRENT_FILE
                {
                    continue;
                }

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
                }
            }
        }

        Ok(errors)
    }
}

///  Extracts the container name from an error message using a regular expression.
fn extract_container_name(error_message: &str) -> String {
    let re = Regex::new(r"\((.*?)\)").unwrap();
    re.captures(error_message)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_else(|| {
            panic!(
                "Failed to extract container name from the error message: {}",
                error_message
            )
        })
}

///  Processes a single file entry, deserializing its content into a `RuntimeErrorEntry` and adding it
///  to the provided vector of errors.
fn process_file_entry(
    file_entry: &fs::DirEntry,
    errors: &mut Vec<RuntimeErrorEntry>,
) -> Result<()> {
    if !file_entry.path().exists() || !file_entry.path().is_file() {
        return Ok(());
    }

    fs::read(file_entry.path())
        .map_err(|e| {
            let err = Error::File(format!("Failed to read file content: {:?}", e));
            error!("{}", err);
            err
        })
        .and_then(|content| {
            RuntimeErrorEntry::try_from(content.as_slice()).map_err(|e| {
                error!("{}", e);
                e
            })
        })
        .map(|payload| errors.push(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::runtime::process_file_entry;
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_extract_container_name_with_valid_pattern() {
        let error_message = "Error occurred in container (my-container)";
        let container_name = extract_container_name(error_message);
        assert_eq!(container_name, "my-container");
    }

    #[test]
    fn test_process_file_entry_valid_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("valid.json");
        let mut file = fs::File::create(&file_path).unwrap();
        let content = r#"{
                "container": "test_container",
                "timestamp": "1234567890",
                "code": "Internal error",
                "message": "An error occurred",
                "details": "Error details"
            }"#;
        file.write_all(content.as_bytes()).unwrap();

        let file_entry = fs::read_dir(dir.path()).unwrap().next().unwrap();
        let mut errors = Vec::new();

        if let Ok(entry) = file_entry {
            let result: Result<()> = process_file_entry(&entry, &mut errors);
            assert!(result.is_ok());
        } else {
            panic!("Failed to read directory entry");
        }

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].container, "test_container");
        assert_eq!(errors[0].timestamp, "1234567890");
        assert_eq!(errors[0].code, "Internal error");
        assert_eq!(errors[0].message, "An error occurred");
        assert_eq!(errors[0].details, "Error details");
    }

    #[test]
    fn test_process_file_entry_deserialization_failure() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("invalid.json");
        let mut file = fs::File::create(&file_path).unwrap();
        let invalid_content = r#"{
                "container": "test_container",
                "timestamp": 1234,
                "code": "Internal error",
                "message": "An error occurred",
                "details": "Error details"
            }"#;
        file.write_all(invalid_content.as_bytes()).unwrap();

        let file_entry = fs::read_dir(dir.path()).unwrap().next().unwrap().unwrap();
        let mut errors = Vec::new();

        let result: Result<()> = process_file_entry(&file_entry, &mut errors);
        assert!(result.is_err());
        assert_eq!(errors.len(), 0);
    }
}
