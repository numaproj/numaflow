use chrono::Utc;
use std::str;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::{fs, io};
use tonic::Status;
use regex::Regex;
use serde_json::json;
use std::error::Error;
pub struct Runtime {
    empty_dir_path: String,
}

impl Runtime {
    /// Creates a new Runtime instance with the specified emptyDir path.
    pub fn new(empty_dir_path: &str) -> Self {
        Runtime {
            empty_dir_path: empty_dir_path.to_string(),
        }
    }

    /// Writes data to a file in the emptyDir.
    pub fn persist_application_error(
        &self,
        grpc_status: Status,
    ) -> io::Result<()> {
        // we can extract the type of udf based on the error message
        let container_name = match get_container_name(grpc_status.message()) {
            Ok(name) => name,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        };

        let dir_path = Path::new(&self.empty_dir_path).join(container_name);
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        let timestamp = Utc::now().timestamp();
        let file_name = format!("{}.json", timestamp);

        let json_str = grpc_status_to_json(&grpc_status)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let file_path = dir_path.join(file_name);
        let mut file = File::create(&file_path)?;
        file.write_all(json_str.as_bytes())?;

        Ok(())
    }

    /// Reads data from a file in the emptyDir.
    /*
     * We need to fetch errors per container (container -> error)
     */
    pub fn read_from_empty_dir(&self, file_name: &str) -> io::Result<String> {
        let file_path = Path::new(&self.empty_dir_path).join(file_name);
        fs::read_to_string(file_path)
    }
}

fn get_container_name(error_message: &str) -> Result<String, String> {
    extract_container_name(error_message).ok_or_else(|| {
        "Failed to extract container name from error message".to_string()
    })
}

fn extract_container_name(error_message: &str) -> Option<String> {
    let re = Regex::new(r"\((.*?)\)").unwrap();
    re.captures(error_message)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
}

/// Converts gRPC status to a JSON object with code, message, and details.
pub fn grpc_status_to_json(grpc_status: &Status) -> Result<String, Box<dyn Error>> {
    // Extract code, message, and details
    let code = grpc_status.code().to_string();
    let message = grpc_status.message().to_string();
    let details_bytes = grpc_status.details();

    // Convert details from bytes to a string
    let details_str = String::from_utf8_lossy(details_bytes);

    // Create JSON object
    let json_object = json!({
        "code": code,
        "message": message,
        "details": details_str,
    });

    // Serialize JSON object to a string
    Ok(json_object.to_string())
}