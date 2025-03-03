use bytes::Bytes;
use chrono::Utc;
use prost::Message;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::{fs, io};
use tonic::Status;

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
    // handle directory creation
    pub fn persist_application_error(
        &self,
        container_name: &str,
        grpc_status: Status,
    ) -> io::Result<()> {
        let timestamp = Utc::now().timestamp();
        let file_name = format!("{}.pb", timestamp);

        // extract container name from the error message
        // marshal grpc status to proto bytes

        // we can extract the type of udf based on the error
        // after extracting lets check if the directory exists
        // if it doesn't create a new directory

        let file_path = Path::new(&self.empty_dir_path)
            .join(container_name)
            .join(file_name);

        let mut file = File::create(&file_path)?;
        file.write_all(&grpc_status.to_string().encode_to_vec())
            .expect("Failed to write error to emptyDir");
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
