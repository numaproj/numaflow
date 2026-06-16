//! The `runtime` module is responsible for persisting runtime information, such as application errors.
use crate::config::RuntimeInfoConfig;
use crate::error::{Error, Result};
use chrono::Utc;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fs;
use std::fs::File;
use std::io::{ErrorKind, Write};
use std::os::unix::fs::DirBuilderExt as _;
use std::path::Path;
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use tonic::Status;
use tonic_types::pb::{DebugInfo, Status as RpcStatus};
use tracing::error;

/// Monotonic per-process sequence appended to error filenames so that two errors persisted within
/// the same nanosecond (or from concurrent writers) produce distinct files. Combined with the
/// nanosecond `Utc::now().timestamp_nanos_opt()` value, collisions are not observable in practice.
static PERSIST_ERROR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Serializes atomic renames and pruning while allowing unique temp-file writes to proceed
/// concurrently.
static PERSIST_ERROR_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
            .map_err(|e| Error::Deserialize(format!("{e:?}")))
    }
}

impl From<(&Status, &str, i64)> for RuntimeErrorEntry {
    fn from(value: (&Status, &str, i64)) -> RuntimeErrorEntry {
        let (grpc_status, container_name, timestamp) = value;

        // Extract code, message, and  binary opaque details
        let code = grpc_status.code().to_string();
        let message = grpc_status.message().to_string();
        // grpc_status.details() is binary opaque details, found in the `grpc-status-details-bin` header from gRPC Status
        // contains the entire Status message
        let details_bytes = grpc_status.details();

        // Try to extract structured error details first, fall back to raw bytes if needed
        // implemented in SDKs for eg: https://github.com/numaproj/numaflow-go/blob/21b573a34817370bfdd435d7be4dd78ed82e9082/pkg/sourcetransformer/service.go#L168
        let details_str = extract_error_details(details_bytes)
            .unwrap_or_else(|| String::from_utf8_lossy(details_bytes).to_string());

        // Extract metadata from gRPC Status
        let metadata = grpc_status.metadata();
        let mut metadata_map = std::collections::HashMap::new();
        for key_value in metadata.iter() {
            match key_value {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                    metadata_map.insert(
                        key.as_str().to_string(),
                        value.to_str().unwrap_or("invalid_utf8").to_string(),
                    );
                }
                tonic::metadata::KeyAndValueRef::Binary(_, _) => {
                    // Skip binary metadata as it's not readable and doesn't add debugging value
                }
            }
        }

        // Convert HashMap to string for storage
        let metadata_str = if metadata_map.is_empty() {
            String::new()
        } else {
            metadata_map
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Combine details and metadata for comprehensive error information
        let combined_details = if metadata_str.is_empty() {
            details_str.to_string()
        } else if details_str.is_empty() {
            format!("metadata: {}", metadata_str)
        } else {
            format!("metadata: {} | details: {}", metadata_str, details_str)
        };

        RuntimeErrorEntry {
            container: container_name.to_string(),
            timestamp,
            code,
            message,
            details: combined_details,
        }
    }
}

impl From<RuntimeErrorEntry> for String {
    fn from(val: RuntimeErrorEntry) -> Self {
        serde_json::to_string(&val).expect("Failed to serialize runtime error message")
    }
}

/// Persists a gRPC error as a JSON file in the appropriate container directory.
/// It organizes error files in a directory structure based on container names and ensures that the
/// number of error files per container (files may be written from udf) does not exceed a specified
/// limit. If the limit is exceeded, the oldest file is removed to make room for new entry.
///
/// Every call records a fresh file. The filename embeds a nanosecond timestamp plus a per-process
/// sequence number so concurrent writers cannot overwrite each other's files.
///
/// The persisted error includes comprehensive information from the gRPC Status:
/// - Error code (e.g., "Internal error", "Unavailable", etc.)
/// - Error message
/// - Details (raw bytes converted to string)
/// - Metadata (key-value pairs with additional context)
///
/// # Parameters:
/// - `grpc_status`: The gRPC error (`tonic::Status`) to be persisted.
///
/// # Example:
/// ```no_run
///  use numaflow_monitor::runtime;
///  let grpc_status = tonic::Status::internal("UDF_EXECUTION_ERROR(container-name): Test error");
///  runtime::persist_application_error(grpc_status);
/// ```
pub fn persist_application_error(grpc_status: Status) {
    persist_application_error_to_file(
        RuntimeInfoConfig::default().app_error_path,
        RuntimeInfoConfig::default().max_error_files_per_container,
        grpc_status,
    );
}

pub(crate) fn persist_application_error_to_file(
    application_error_path: String,
    max_error_files_per_container: usize,
    grpc_status: Status,
) {
    let container_name = extract_container_name(grpc_status.message());
    let dir_path = Path::new(&application_error_path.clone()).join(&container_name);
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    builder.mode(0o777);
    if let Err(e) = builder.create(&dir_path)
        && e.kind() != ErrorKind::AlreadyExists
    {
        panic!("Failed to create application errors directory: {e:?}");
    }

    let now = Utc::now();
    let timestamp_seconds = now.timestamp();
    let timestamp_nanos = now
        .timestamp_nanos_opt()
        .unwrap_or_else(|| timestamp_seconds.saturating_mul(1_000_000_000));
    let sequence = PERSIST_ERROR_SEQUENCE.fetch_add(1, Ordering::Relaxed);

    let runtime_error_entry =
        RuntimeErrorEntry::from((&grpc_status, container_name.as_str(), timestamp_seconds));
    let json_str: String = runtime_error_entry.into();

    // The temp and final filenames share the same unique tuple, so concurrent writers never share
    // a temp path and readers only see complete JSON files.
    let temp_file_path = dir_path.join(format!("{timestamp_nanos}-{sequence}-numa.tmp"));
    let final_file_path = dir_path.join(format!("{timestamp_nanos}-{sequence}-numa.json"));

    let mut temp_file =
        File::create(&temp_file_path).expect("Failed to create temp application errors file");
    temp_file
        .write_all(json_str.as_bytes())
        .expect("Failed to write to temp application error file");

    let _guard = PERSIST_ERROR_LOCK
        .lock()
        .expect("application error persist lock poisoned");
    fs::rename(&temp_file_path, &final_file_path)
        .expect("Failed to rename temp file to final file name");
    prune_error_files(&dir_path, max_error_files_per_container);
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

#[allow(dead_code)]
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
            .map_err(|e| Error::File(format!("Failed to read directory: {e:?}")))?;

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
                        Error::File(format!("Failed to read subdirectory: {e:?}"))
                    );
                    continue;
                }
            };

            for file_entry in file_paths.flatten() {
                // Skip in-flight temp files and legacy temp files left by older writers.
                let file_name = file_entry
                    .file_name()
                    .to_str()
                    .expect("file name should be valid")
                    .to_string();
                if file_name.ends_with(".tmp") || file_name.starts_with("current") {
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

fn prune_error_files(dir_path: &Path, max_error_files_per_container: usize) {
    // Treat zero as unlimited. The default config is positive, and preserving errors is safer than
    // deleting every just-written file if a zero value is supplied.
    if max_error_files_per_container == 0 {
        return;
    }

    let mut files: Vec<_> = fs::read_dir(dir_path)
        .expect("Failed to read application errors directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.ends_with(".json"))
                .unwrap_or(false)
        })
        .collect();

    files.sort_by_key(|e| {
        e.file_name()
            .to_str()
            .and_then(|name| name.split('-').next())
            .and_then(|timestamp| timestamp.parse::<i64>().ok())
    });

    while files.len() > max_error_files_per_container {
        if let Some(oldest_file) = files.first() {
            if let Err(e) = fs::remove_file(oldest_file.path()) {
                error!(
                    "Failed to remove the oldest application error file: {:?}, error: {:?}",
                    oldest_file.path(),
                    e
                );
                break;
            }
            files.remove(0);
        }
    }
}

///  Extracts the container name from error message.
fn extract_container_name(error_message: &str) -> String {
    if let Some(start) = error_message.find('(')
        && let Some(end) = error_message[start + 1..].find(')')
    {
        return error_message[start + 1..start + 1 + end].to_string();
    }
    // Setting container to "numa" as the default container name to ensure that the error is
    // persisted in a consistent manner. This can happen in following cases:
    // 1. The error is a gRPC error, but the gRPC status was not created by this application.
    //    For example, this can happen due to unknown bugs in the gRPC library, such as:
    //    https://github.com/grpc/grpc-go/issues/7641
    // 2. The error is not a gRPC error, and no container name could be extracted from the error message.
    String::from("numa")
}

/// Extracts structured error details from protobuf-encoded gRPC status.
/// This function deserializes known error detail types from the gRPC status.
/// Currently supports DebugInfo, but can be extended for other types like
/// QuotaFailure, BadRequest, etc. Note: grpc_status.details() returns the entire
/// Status message, not just the details field.
fn extract_error_details(details_bytes: &[u8]) -> Option<String> {
    if details_bytes.is_empty() {
        return None;
    }

    // The bytes represent a complete gRPC Status message
    if let Ok(status) = RpcStatus::decode(details_bytes) {
        // Look for known error detail types in the status details
        for detail in &status.details {
            // Future error types can be added here:
            // "type.googleapis.com/google.rpc.QuotaFailure" => { ... }
            // "type.googleapis.com/google.rpc.BadRequest" => { ... }
            // "type.googleapis.com/google.rpc.PreconditionFailure" => { ... }
            if detail.type_url.as_str() == "type.googleapis.com/google.rpc.DebugInfo"
                && let Ok(debug_info) = DebugInfo::decode(&detail.value[..])
            {
                return Some(debug_info.detail);
            }
        }
    }

    None
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
            let err = Error::File(format!("Failed to read file content: {e:?}"));
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
    fn test_persist_application_error_to_file() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let application_error_path = temp_dir.path().to_str().unwrap().to_string();

        // Create a mock gRPC status
        let grpc_status = Status::internal("UDF_EXECUTION_ERROR(udsource): Test error message");

        // Call the function to test
        let before = Utc::now().timestamp();
        persist_application_error_to_file(application_error_path.clone(), 5, grpc_status.clone());
        let after = Utc::now().timestamp();

        // Verify that the directory for the container was created
        let container_name = extract_container_name(grpc_status.message());
        let dir_path = Path::new(&application_error_path).join(&container_name);
        assert!(dir_path.exists());

        // Verify that a new error file was created
        let files: Vec<_> = fs::read_dir(&dir_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(files.len(), 1);

        // Verify the file name format
        let file_name = files
            .first()
            .expect("Expected file entry")
            .file_name()
            .into_string()
            .unwrap();
        assert!(file_name.ends_with(".json"));
        let (timestamp_nanos, _sequence, suffix) = parse_error_filename(&file_name);
        assert!(timestamp_nanos >= before.saturating_mul(1_000_000_000));
        assert_eq!(suffix, "numa.json");

        let file_path = dir_path.join(file_name);
        let file_content = fs::read(file_path).unwrap();
        let entry = RuntimeErrorEntry::try_from(file_content.as_slice()).unwrap();
        assert!(
            (before..=after).contains(&entry.timestamp),
            "persisted timestamp should be Unix seconds, got {}",
            entry.timestamp
        );
    }

    #[test]
    fn test_persist_application_error_with_empty_container_name() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let application_error_path = temp_dir.path().to_str().unwrap().to_string();
        // Create a mock gRPC status with an empty container name
        let grpc_status = Status::internal("UDF_EXECUTION_ERROR: Test error message");

        // Verify that container name is empty for below grpc_status
        let container_name = extract_container_name(grpc_status.message());
        assert_eq!(container_name, "numa".to_string());
        let dir_path = Path::new(&application_error_path).join(&container_name);

        // Call the function to test
        persist_application_error_to_file(application_error_path.clone(), 5, grpc_status.clone());

        // Verify that a new error file was created
        let files: Vec<_> = fs::read_dir(&dir_path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(files.len(), 1);

        // Verify the file name format
        let file_name = files
            .first()
            .expect("Expected file entry")
            .file_name()
            .into_string()
            .unwrap();
        assert!(file_name.ends_with(".json"));
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
        let error0 = errors.first().expect("Expected error");
        assert_eq!(
            error0.message,
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
        let error0 = errors.first().expect("Expected error");
        assert_eq!(error0.container, "test_container");
        assert_eq!(error0.timestamp, 1234567890);
        assert_eq!(error0.code, "Internal error");
        assert_eq!(error0.message, "An error occurred");
        assert_eq!(error0.details, "Error details");
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

    #[test]
    fn test_persist_application_error_repeated_calls_are_all_recorded() {
        let temp_dir = tempdir().unwrap();
        let app_err_path = temp_dir.path().to_str().unwrap().to_string();
        for i in 0..3 {
            let status =
                Status::internal(format!("UDF_EXECUTION_ERROR(repeated-container): call {i}"));
            persist_application_error_to_file(app_err_path.clone(), 10, status);
        }
        let container_dir = Path::new(&app_err_path).join("repeated-container");
        let files: Vec<_> = fs::read_dir(&container_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.ends_with(".json"))
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            files.len(),
            3,
            "expected three distinct error files after three persist calls"
        );
    }

    #[test]
    fn test_persist_application_error_concurrent_filenames_are_unique() {
        let temp_dir = tempdir().unwrap();
        let app_err_path = temp_dir.path().to_str().unwrap().to_string();

        // Allow up to 100 files so rotation does not mask the collision question we are testing.
        // Spawn 100 concurrent persists from a thread pool; each call enters
        // `persist_application_error_to_file` independently.
        const N: usize = 100;
        let mut handles = Vec::with_capacity(N);
        for i in 0..N {
            let path = app_err_path.clone();
            handles.push(std::thread::spawn(move || {
                let status = Status::internal(format!(
                    "UDF_EXECUTION_ERROR(concurrent-container): writer {i}"
                ));
                persist_application_error_to_file(path, N, status);
            }));
        }
        for h in handles {
            h.join().expect("writer thread panicked");
        }

        let container_dir = Path::new(&app_err_path).join("concurrent-container");
        let files: Vec<_> = fs::read_dir(&container_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();

        // No temp file should be left behind — all writes completed and renamed.
        let temp_files: Vec<_> = files
            .iter()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.ends_with(".tmp"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            temp_files.is_empty(),
            "no `.tmp` files should remain after all writers finish, found {temp_files:?}"
        );

        // Exactly N final files. If any two concurrent writers had collided on the same final
        // name, the second rename would have overwritten the first and the count would drop.
        let final_files: Vec<_> = files
            .iter()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.ends_with(".json"))
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            final_files.len(),
            N,
            "expected {N} distinct error files from {N} concurrent writers"
        );
    }

    #[test]
    fn test_persist_application_error_concurrent_writes_respect_file_cap() {
        const WRITERS: usize = 25;
        const MAX_FILES: usize = 10;

        let temp_dir = tempdir().unwrap();
        let app_err_path = temp_dir.path().to_str().unwrap().to_string();
        let mut handles = Vec::with_capacity(WRITERS);
        for i in 0..WRITERS {
            let path = app_err_path.clone();
            handles.push(std::thread::spawn(move || {
                let status =
                    Status::internal(format!("UDF_EXECUTION_ERROR(capped-container): writer {i}"));
                persist_application_error_to_file(path, MAX_FILES, status);
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread panicked");
        }

        let container_dir = Path::new(&app_err_path).join("capped-container");
        let files: Vec<_> = fs::read_dir(&container_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        let final_files = files
            .iter()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.ends_with(".json"))
                    .unwrap_or(false)
            })
            .count();
        let temp_files = files
            .iter()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.ends_with(".tmp"))
                    .unwrap_or(false)
            })
            .count();

        assert!(final_files <= MAX_FILES);
        assert_eq!(temp_files, 0);
    }

    fn parse_error_filename(file_name: &str) -> (i64, u64, &str) {
        let mut parts = file_name.splitn(3, '-');
        let timestamp_nanos = parts.next().unwrap().parse().unwrap();
        let sequence = parts.next().unwrap().parse().unwrap();
        let suffix = parts.next().unwrap();
        (timestamp_nanos, sequence, suffix)
    }

    #[test]
    fn test_runtime_error_entry_with_metadata() {
        use tonic::metadata::MetadataMap;

        // Add some metadata to the status
        let mut metadata = MetadataMap::new();
        metadata.insert("error-type", "udf-execution".parse().unwrap());
        metadata.insert("retry-count", "3".parse().unwrap());
        metadata.insert_bin(
            "binary-data-bin",
            tonic::metadata::MetadataValue::from_bytes(b"some binary data"),
        );

        let status_with_metadata = Status::with_details_and_metadata(
            tonic::Code::Internal,
            "Test error message with metadata",
            "test details".into(),
            metadata,
        );

        let container_name = "test-container";
        let timestamp = 1234567890i64;

        // Convert to RuntimeErrorEntry
        let error_entry =
            RuntimeErrorEntry::from((&status_with_metadata, container_name, timestamp));

        // Verify that metadata is included in the details
        assert_eq!(error_entry.container, "test-container");
        assert_eq!(error_entry.timestamp, 1234567890);
        assert_eq!(error_entry.code, "Internal error"); // This is how tonic::Code::Internal formats as string
        assert_eq!(error_entry.message, "Test error message with metadata");

        // Check that details contains both original details and metadata
        assert!(error_entry.details.contains("test details"));
        assert!(error_entry.details.contains("metadata:"));
        assert!(error_entry.details.contains("error-type=udf-execution"));
        assert!(error_entry.details.contains("retry-count=3"));
        // Binary metadata is intentionally skipped because it is not useful in the JSON payload.
        assert!(!error_entry.details.contains("binary-data-bin="));
    }
}
