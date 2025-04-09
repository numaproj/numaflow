//! The `runtime` module is responsible for persisting runtime information, such as application errors.
use crate::config::RuntimeInfoConfig;
use crate::error::{Error, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::Path;
use std::str;
use tonic::Status;
use tracing::error;
use uuid::Uuid;

/// Represents a single runtime error entry persisted by the application.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RuntimeErrorEntry {
    /// The name of the container where the error occurred.
    pub(crate) container: String,
    /// The timestamp of the error.
    pub(crate) timestamp: i64,
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

        RuntimeErrorEntry {
            container: container_name.to_string(),
            timestamp: timestamp,
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
    #[serde(rename = "errorMessage")]
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
    /// Creates a new Runtime instance
    pub fn new(runtime_info_config: Option<RuntimeInfoConfig>) -> Self {
        let config = runtime_info_config.unwrap_or_default();
        Runtime {
            application_error_path: config.app_error_path,
            max_error_files_per_container: config.max_error_files_per_container,
        }
    }

    /// Persists a gRPC error as a JSON file in the appropriate container directory.
    /// It organizes error files in a directory structure based on container names and ensures that the
    /// number of error files per container does not exceed a specified limit. If the limit is exceeded,
    /// the oldest file is removed to make room for new entries.
    ///
    /// # Parameters:
    /// - `grpc_status`: The gRPC error (`tonic::Status`) to be persisted.
    ///
    /// # Example:
    /// ```no_run
    /// use numaflow_monitor::runtime::Runtime;
    ///
    /// let grpc_status = tonic::Status::internal("UDF_EXECUTION_ERROR(container-name): Test error");
    /// let runtime = Runtime::new(None);
    /// runtime.persist_application_error(grpc_status);
    /// ```
    pub fn persist_application_error(&self, grpc_status: Status) {
        // extract the type of udf container based on the error message
        let container_name = extract_container_name(grpc_status.message());
        // skip processing if the container name is empty. This happens only if
        // the gRPC status is not created by us (e.g., unknown bugs like https://github.com/grpc/grpc-go/issues/7641)
        // TODO: we should try to expose this in the UI if we encounter a few of this in prod.
        if container_name.is_empty() {
            error!(?grpc_status, "unknown-container");
            return;
        }
        // create a directory for the container if it doesn't exist
        let dir_path = Path::new(&self.application_error_path).join(&container_name);
        if !dir_path.exists() {
            // create directory with permissions to read, write, and execute for all
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            builder.mode(0o777);
            builder
                .create(&dir_path)
                .expect("Failed to create application errors directory");
        }

        // this is to check the number of files in the directory
        // additional check in place to process only files and ignore directories
        // ignore files starting with current prefix
        let mut files: Vec<_> = fs::read_dir(&dir_path)
            .expect("Failed to read application errors directory")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().is_file())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| !name.starts_with("current"))
                    .unwrap_or(false)
            })
            .collect();

        // sort the files based on timestamp
        files.sort_by_key(|e| {
            e.file_name()
                .to_str()
                .and_then(|name| name.split('-').next())
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
        });

        // remove the oldest files until the number of files is within the max limit
        // this is to ensure that we don't exceed the max limit of files in the container directory
        while files.len() >= self.max_error_files_per_container {
            if let Some(oldest_file) = files.first() {
                if let Err(e) = fs::remove_file(oldest_file.path()) {
                    error!(
                        "Failed to remove the oldest application error file: {:?}, error: {:?}",
                        oldest_file.path(),
                        e
                    );
                    break;
                }
                // Remove the file from the list after successful deletion
                files.remove(0);
            }
        }

        let timestamp = Utc::now().timestamp();
        let runtime_error_entry =
            RuntimeErrorEntry::from((&grpc_status, container_name.as_str(), timestamp));
        let json_str: String = runtime_error_entry.into();

        // Write the error details to a temporary file and rename it to a
        // timestamped file once the write operation is complete.
        // this is to ensure that while reading we skip this file to avoid race condition
        let uuid = Uuid::new_v4();
        let current_file_name = format!("current-numa-{}.json", uuid);
        let current_file_path = dir_path.join(&current_file_name);
        // append numa to the file name to denote files created by numa container
        let file_name = format!("{}-numa-{}.json", timestamp, uuid);
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
            return Ok(errors);
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
                    .starts_with("current")
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

///  Extracts the container name from error message.
fn extract_container_name(error_message: &str) -> String {
    if let Some(start) = error_message.find('(') {
        if let Some(end) = error_message[start + 1..].find(')') {
            return error_message[start + 1..start + 1 + end].to_string();
        }
    }
    String::new()
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
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Barrier;

    #[test]
    fn test_runtime_new() {
        // Test with configuration
        let config = RuntimeInfoConfig {
            app_error_path: String::from("/path/to/errors"),
            max_error_files_per_container: 5,
        };
        let runtime_with_config = Runtime::new(Some(config));
        assert_eq!(
            runtime_with_config.application_error_path,
            "/path/to/errors"
        );
        assert_eq!(runtime_with_config.max_error_files_per_container, 5);

        // Test without configuration
        let runtime_without_config = Runtime::new(None);
        assert_eq!(
            runtime_without_config.application_error_path,
            "/var/numaflow/runtime/application-errors"
        );
        assert_eq!(runtime_without_config.max_error_files_per_container, 10);
    }

    #[test]
    fn test_persist_application_error() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let application_error_path = temp_dir.path().to_str().unwrap().to_string();

        // Create a Runtime instance with the temporary directory path
        let runtime = Runtime {
            application_error_path,
            max_error_files_per_container: 5,
        };

        // Create a mock gRPC status
        let grpc_status = Status::internal("UDF_EXECUTION_ERROR(udsource): Test error message");

        // Call the function to test
        runtime.persist_application_error(grpc_status.clone());

        // Verify that the directory for the container was created
        let container_name = extract_container_name(grpc_status.message());
        let dir_path = Path::new(&runtime.application_error_path).join(&container_name);
        assert!(dir_path.exists());

        // Verify that a new error file was created
        let files: Vec<_> = fs::read_dir(&dir_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(files.len(), 1);

        // Verify the file name format
        let file_name = files[0].file_name().into_string().unwrap();
        assert!(file_name.ends_with(".json"));
    }

    #[test]
    fn test_persist_application_error_with_empty_container_name() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let application_error_path = temp_dir.path().to_str().unwrap().to_string();

        // Create a Runtime instance with the temporary directory path
        let runtime = Runtime {
            application_error_path,
            max_error_files_per_container: 5,
        };

        // Create a mock gRPC status with an empty container name
        let grpc_status = Status::internal("UDF_EXECUTION_ERROR: Test error message");

        // Verify that container name is empty for below grpc_status
        let container_name = extract_container_name(grpc_status.message());
        assert!(container_name.is_empty());

        // Call the function to test
        runtime.persist_application_error(grpc_status.clone());

        // Verify that no files were created in the directory
        let dir_path = Path::new(&runtime.application_error_path).join(&container_name);
        if dir_path.exists() {
            let files: Vec<_> = fs::read_dir(&dir_path)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .collect();
            assert!(
                files.is_empty(),
                "No files should be created in the directory"
            );
        }
    }

    #[test]
    fn test_get_application_errors() {
        // Create a temporary directory
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let app_err_path = temp_dir.path().join("application-errors");
        fs::create_dir(&app_err_path).expect("Failed to create application-errors dir");

        // Create a subdirectory for a container
        let container_name = "test-container";
        let container_dir = app_err_path.join(container_name);
        fs::create_dir(&container_dir).expect("Failed to create container dir");

        // Create a mock gRPC status and json string
        let grpc_status =
            Status::internal("UDF_EXECUTION_ERROR(test-container): Test error message");
        let timestamp = Utc::now().timestamp();
        let runtime_error_entry =
            RuntimeErrorEntry::from((&grpc_status, container_name, timestamp));
        let json_str: String = runtime_error_entry.into();

        // Create a file with error content
        let file_name = format!("{}.json", timestamp);
        let error_file_path = container_dir.join(&file_name);
        let mut error_file = File::create(&error_file_path).expect("Failed to create error file");

        error_file
            .write_all(json_str.as_bytes())
            .expect("Failed to write to application error file");

        // Create an instance of the struct containing get_application_errors
        let runtime_info = Runtime {
            application_error_path: app_err_path.to_str().unwrap().to_string(),
            max_error_files_per_container: 10, // other fields as necessary
        };

        // Call the function and assert the results
        let errors = runtime_info
            .get_application_errors()
            .expect("Failed to get application errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].message,
            "UDF_EXECUTION_ERROR(test-container): Test error message"
        );
    }

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
                "timestamp": 1234567890,
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
        assert_eq!(errors[0].timestamp, 1234567890);
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
                "timestamp": "1234",
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
    #[tokio::test]
    async fn test_persist_application_error_concurrent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let application_error_path = temp_dir.path().to_str().unwrap().to_string();

        // Create a Runtime instance with the temporary directory path
        let runtime = Arc::new(Runtime {
            application_error_path,
            max_error_files_per_container: 10,
        });

        let barrier = Arc::new(Barrier::new(5));
        // Create a mock gRPC status
        let grpc_status =
            Status::internal("UDF_EXECUTION_ERROR(test-container): Test error message");

        // Spawn 5 threads to call persist_application_error concurrently
        let mut handles = vec![];
        for _i in 0..5 {
            let runtime_clone = runtime.clone();
            let barrier_clone = barrier.clone();
            let grpc_status_clone = grpc_status.clone();

            let handle = tokio::spawn(async move {
                // Wait for all threads to be ready
                barrier_clone.wait().await;

                // Call persist_application_error
                runtime_clone.persist_application_error(grpc_status_clone.clone());
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify that the directory for the container was created
        let container_name = extract_container_name(grpc_status.message());
        let dir_path = Path::new(&runtime.application_error_path).join(&container_name);
        assert!(dir_path.exists());

        // Verify that 5 error files were created
        let files: Vec<_> = fs::read_dir(&dir_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(files.len(), 5);

        // Verify the file name format
        for file in files {
            let file_name = file.file_name().into_string().unwrap();
            assert!(file_name.ends_with(".json"));
            assert!(file_name.contains("numa"));
        }
    }
}
