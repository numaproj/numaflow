use pep440_rs::{Version as PepVersion, VersionSpecifier};
use semver::{Version, VersionReq};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::error::Error;
use crate::{error, version};

// Constant to represent the end of the server info. Equivalent to U+005C__END__.
const END: &str = "U+005C__END__";

// fn unwrap_or_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
// where
//     D: Deserializer<'de>,
//     T: Deserialize<'de> + Default,
// {
//     Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
// }

// Struct for ServerInfo
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ServerInfo {
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    minimum_numaflow_version: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    metadata: Option<HashMap<String, String>>,
}

// wait_for_server_info waits until the server info is ready
pub async fn wait_for_server_info(file_path: &str) -> error::Result<()> {
    loop {
        // TODO: add a check for cancellation
        if let Ok(metadata) = fs::metadata(file_path) {
            if metadata.len() > 0 {
                // break out of the loop if the file is ready
                break;
            }
        }
        info!("Server info file {} is not ready, waiting...", file_path);
        sleep(Duration::from_secs(1)).await;
    }

    // Read the server info file
    let read_result = read_server_info(file_path).await;
    // Extract the server info from the Result
    let server_info = match read_result {
        Ok(info) => info,
        Err(e) => {
            return Err(Error::ServerInfoError(format!(
                "Failed to read server info: {}",
                e
            )));
        }
    };

    info!("Server info file: {:?}", server_info);

    let sdk_version = &server_info.version;
    let min_numaflow_version = &server_info.minimum_numaflow_version;
    let sdk_language = &server_info.language;
    let version_info = version::VersionInfo::get_version_info();
    let numaflow_version = &version_info.version;

    println!("version_info: {:?}", version_info);

    if min_numaflow_version.is_empty() {
        warn!("Failed to get the minimum numaflow version, skipping numaflow version compatibility check");
    } else if !numaflow_version.contains("latest")
        && !numaflow_version.contains(&version_info.git_commit)
    {
        // Check the compatibility between the SDK and Numaflow versions
        // If any error occurs, return the error
        if let Err(e) = check_numaflow_compatibility(numaflow_version, min_numaflow_version) {
            return Err(e);
        }
        return Ok(());
    }

    if sdk_version.is_empty() || sdk_language.is_empty() {
        warn!("Failed to get the SDK version/language, skipping SDK version compatibility check");
    } else {
        let min_supported_sdk_versions = version::get_minimum_supported_sdk_versions();
        check_sdk_compatibility(sdk_version, sdk_language, min_supported_sdk_versions)?;
    }

    Ok(())
}
/// Checks if the given version meets the specified constraint.
fn check_constraint(version: &Version, constraint: &str) -> error::Result<()> {
    let version_req = VersionReq::parse(constraint).map_err(|e| {
        Error::ServerInfoError(format!(
            "Error parsing constraint: {},\
         constraint string: {}",
            e, constraint
        ))
    })?;

    if !version_req.matches(version) {
        return Err(Error::ServerInfoError("invalid version".to_string()));
    }

    Ok(())
}

/// Checks if the current numaflow version is compatible with the given language's SDK version.
fn check_numaflow_compatibility(
    numaflow_version: &str,
    min_numaflow_version: &str,
) -> error::Result<()> {
    if min_numaflow_version.is_empty() {
        return Err(Error::ServerInfoError("invalid version".to_string()));
    }

    let numaflow_version_semver = Version::parse(numaflow_version)
        .map_err(|e| Error::ServerInfoError(format!("Error parsing Numaflow version: {}", e)))?;

    let numaflow_constraint = format!(">={}", min_numaflow_version);
    if let Err(e) = check_constraint(&numaflow_version_semver, &numaflow_constraint) {
        let err_string = format!(
            "numaflow version {} must be upgraded to at least {}, in order to work with current SDK version {}",
            numaflow_version_semver, min_numaflow_version, e
        );
        return Err(Error::ServerInfoError(err_string));
    }
    Ok(())
}

fn check_sdk_compatibility(
    sdk_version: &str,
    sdk_language: &str,
    min_supported_sdk_versions: &version::SdkConstraints,
) -> error::Result<()> {
    if let Some(sdk_required_version) = min_supported_sdk_versions.get(sdk_language) {
        let sdk_constraint = format!(">={}", sdk_required_version);

        if sdk_language.to_lowercase() == "python" {
            let sdk_version_pep440 = PepVersion::from_str(sdk_version)
                .map_err(|e| Error::ServerInfoError(format!("Error parsing SDK version: {}", e)))?;

            let specifiers = VersionSpecifier::from_str(&sdk_constraint).map_err(|e| {
                Error::ServerInfoError(format!("Error parsing SDK constraint: {}", e))
            })?;

            println!("sdk_version_semver: {:?}", sdk_version_pep440);
            println!("sdk_constraint: {:?}", sdk_constraint);

            if !specifiers.contains(&sdk_version_pep440) {
                let err_string = format!(
                    "SDK version {} must be upgraded to at least {}, in order to work with the current numaflow version",
                    sdk_version_pep440, sdk_required_version
                );
                return Err(Error::ServerInfoError(err_string));
            }
        } else {
            let sdk_version_stripped = if sdk_version.starts_with('v') {
                &sdk_version[1..]
            } else {
                sdk_version
            };

            let sdk_version_semver = Version::parse(sdk_version_stripped)
                .map_err(|e| Error::ServerInfoError(format!("Error parsing SDK version: {}", e)))?;

            println!("sdk_version_semver: {:?}", sdk_version_semver);
            println!("sdk_constraint: {:?}", sdk_constraint);

            if let Err(e) = check_constraint(&sdk_version_semver, &sdk_constraint) {
                let err_string = format!(
                    "SDK version {} must be upgraded to at least {}, in order to work with the current numaflow version: {}",
                    sdk_version_semver, sdk_required_version, e
                );
                return Err(Error::ServerInfoError(err_string));
            }
        }
    } else {
        // language not found in the supported SDK versions
        warn!(
            "SDK version constraint not found for language: {}",
            sdk_language
        );
        // return error indicating the language
        return Err(Error::ServerInfoError(format!(
            "SDK version constraint not found for language: {}",
            sdk_language
        )));
    }
    Ok(())
}

async fn read_server_info(file_path: &str) -> error::Result<ServerInfo> {
    // Retry logic
    let mut retry = 0;
    let contents;
    loop {
        // Read the file
        match fs::read_to_string(file_path) {
            Ok(data) => {
                if data.ends_with(END) {
                    contents = data.trim_end_matches(END).to_string();
                    // break out of the loop if the file is ready
                    break;
                } else {
                    warn!("Server info file is not ready yet...");
                }
            }
            Err(e) => {
                warn!("Failed to read file: {}", e);
            }
        }

        // Retry logic with limit
        retry += 1;
        if retry >= 10 {
            // return an error if the retry limit is reached
            return Err(Error::ServerInfoError(
                "server-info reading retry exceeded".to_string(),
            ));
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Parse the JSON, if there is an error, return the error
    let server_info: ServerInfo = serde_json::from_str(&contents).map_err(|e| {
        Error::ServerInfoError(format!(
            "Failed to parse server-info file: {}, contents: {}",
            e, contents
        ))
    })?;
    Ok(server_info)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::{collections::HashMap, fs::File};
    use tempfile::tempdir;

    use super::*;

    // Alias Protocol as &str
    const UDS: &str = "uds";
    const TCP: &str = "tcp";
    const PYTHON: &str = "python";
    const GOLANG: &str = "go";
    const JAVA: &str = "java";

    // Constants
    const MAP_MODE_KEY: &str = "MAP_MODE";
    const MINIMUM_NUMAFLOW_VERSION: &str = "1.2.0-rc4";

    async fn write_server_info(
        svr_info: &ServerInfo,
        svr_info_file_path: &str,
    ) -> error::Result<()> {
        let serialized = serde_json::to_string(svr_info).unwrap();

        // Remove the existing file if it exists
        if let Err(e) = fs::remove_file(svr_info_file_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(Error::ServerInfoError(format!(
                    "Failed to remove server-info file: {}",
                    e
                )));
            }
        }

        // Create a new file
        let mut file = File::create(svr_info_file_path);

        // Extract the file from the Result
        let mut file = match file {
            Ok(f) => f,
            Err(e) => {
                return Err(Error::ServerInfoError(format!(
                    "Failed to create server-info file: {}",
                    e
                )));
            }
        };

        // Write the serialized data and the END marker to the file
        // Remove the existing file if it exists
        if let Err(e) = file.write_all(serialized.as_bytes()) {
            return Err(Error::ServerInfoError(format!(
                "Failed to write server-info file: {}",
                e
            )));
        }
        if let Err(e) = file.write_all(END.as_bytes()) {
            return Err(Error::ServerInfoError(format!(
                "Failed to write server-info file: {}",
                e
            )));
        }
        Ok(())
    }

    // Helper function to create a SdkConstraints struct
    fn create_sdk_constraints() -> version::SdkConstraints {
        let mut constraints = HashMap::new();
        constraints.insert("python".to_string(), "1.2.0".to_string());
        constraints.insert("java".to_string(), "2.0.0".to_string());
        constraints.insert("go".to_string(), "0.10.0".to_string());
        constraints
    }

    #[tokio::test]
    async fn test_sdk_compatibility_python_valid() {
        let sdk_version = "v1.3.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_python_invalid() {
        let sdk_version = "1.1.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_java_valid() {
        let sdk_version = "v2.1.0";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_java_invalid() {
        let sdk_version = "1.5.0";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_go_valid() {
        let sdk_version = "0.11.0";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_go_invalid() {
        let sdk_version = "0.9.0";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_valid() {
        let numaflow_version = "1.4.0";
        let min_numaflow_version = "1.3.0";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_invalid() {
        let numaflow_version = "1.2.0";
        let min_numaflow_version = "1.3.0";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_server_info_success() {
        // Create a temporary directory
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("server_info.txt");

        // Server info to write
        let server_info = ServerInfo {
            protocol: TCP.to_string(),
            language: GOLANG.to_string(),
            minimum_numaflow_version: MINIMUM_NUMAFLOW_VERSION.to_string(),
            version: "1.0.0".to_string(),
            metadata: {
                let mut m = HashMap::new();
                m.insert("key1".to_string(), "value1".to_string());
                Some(m)
            },
        };

        // Write server info
        let result = write_server_info(&server_info, file_path.to_str().unwrap()).await;
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);

        // Read the file and check its contents
        let mut content = String::new();
        File::open(&file_path)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        let expected_json = serde_json::to_string(&server_info).unwrap();
        let expected_content = format!("{}{}", expected_json, END);
        assert_eq!(content, expected_content, "File content mismatch");
    }

    #[tokio::test]
    async fn test_write_server_info_failure() {
        // Invalid file path that cannot be created
        let file_path = PathBuf::from("/invalid/path/server_info.txt");

        // Server info to write
        let server_info = ServerInfo {
            protocol: TCP.parse().unwrap(),
            language: GOLANG.parse().unwrap(),
            minimum_numaflow_version: MINIMUM_NUMAFLOW_VERSION.to_string(),
            version: "1.0.0".to_string(),
            metadata: {
                let mut m = HashMap::new();
                m.insert("key1".to_string(), "value1".to_string());
                Some(m)
            },
        };

        // Write server info
        let result = write_server_info(&server_info, file_path.to_str().unwrap()).await;
        assert!(result.is_err(), "Expected Err, got {:?}", result);

        // Check that we received the correct error variant
        let error = result.unwrap_err();
        assert!(
            matches!(error, Error::ServerInfoError(_)),
            "Expected ServerInfoError, got {:?}",
            error
        );
    }

    #[tokio::test]
    async fn test_read_server_info_success() {
        // Create a temporary directory
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("server_info.txt");

        // Server info to write
        let server_info = ServerInfo {
            protocol: TCP.parse().unwrap(),
            language: PYTHON.parse().unwrap(),
            minimum_numaflow_version: MINIMUM_NUMAFLOW_VERSION.to_string(),
            version: "1.0.0".to_string(),
            metadata: {
                let mut m = HashMap::new();
                m.insert("key1".to_string(), "value1".to_string());
                Some(m)
            },
        };

        // Write server info
        let _ = write_server_info(&server_info, file_path.to_str().unwrap()).await;

        // Call the read_server_info function
        let result = read_server_info(file_path.to_str().unwrap()).await;
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);

        let server_info = result.unwrap();
        assert_eq!(server_info.protocol, "tcp");
        assert_eq!(server_info.language, "python");
        assert_eq!(server_info.minimum_numaflow_version, "1.2.0-rc4");
        assert_eq!(server_info.version, "1.0.0");
        // assert_eq!(server_info.metadata.get("key1").unwrap(), "value1");
    }

    #[tokio::test]
    async fn test_read_server_info_retry_limit() {
        // Create a temporary directory
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("server_info.txt");

        // Write a partial test file not ending with END marker
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"protocol":"tcp","language":"go","minimum_numaflow_version":"1.2.0-rc4","version":"1.0.0","metadata":{{"key1":"value1"}}}}"#).unwrap();

        // Call the read_server_info function
        let result = read_server_info(file_path.to_str().unwrap()).await;
        assert!(result.is_err(), "Expected Err, got {:?}", result);

        let error = result.unwrap_err();
        assert!(
            matches!(error, Error::ServerInfoError(_)),
            "Expected ServerInfoError, got {:?}",
            error
        );
    }

    #[test]
    fn test_deserialize_with_null_metadata() {
        let json_data = json!({
            "protocol": "uds",
            "language": "go",
            "minimum_numaflow_version": "1.2.0-rc4",
            "version": "v0.7.0-rc2",
            "metadata": null
        })
        .to_string();

        let expected_server_info = ServerInfo {
            protocol: "uds".to_string(),
            language: "go".to_string(),
            minimum_numaflow_version: "1.2.0-rc4".to_string(),
            version: "v0.7.0-rc2".to_string(),
            metadata: Some(HashMap::new()), // Expecting an empty HashMap here
        };

        let parsed_server_info: ServerInfo =
            serde_json::from_str(&json_data).expect("Failed to parse JSON");

        println!("Parsed server info: {:?}", parsed_server_info);
    }

    #[test]
    fn test_sdk_compatibility_go_version_with_v_prefix() {
        let sdk_version = "v0.11.0";
        let sdk_language = "go";

        let mut min_supported_sdk_versions = HashMap::new();
        min_supported_sdk_versions.insert("go".to_string(), "0.10.0".to_string());

        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[test]
    fn test_sdk_compatibility_go_version_without_v_prefix() {
        let sdk_version = "0.11.0";
        let sdk_language = "go";

        let mut min_supported_sdk_versions = HashMap::new();
        min_supported_sdk_versions.insert("go".to_string(), "0.10.0".to_string());

        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[test]
    fn test_sdk_compatibility_go_version_with_v_prefix_invalid() {
        let sdk_version = "v0.9.0";
        let sdk_language = "go";

        let mut min_supported_sdk_versions = HashMap::new();
        min_supported_sdk_versions.insert("go".to_string(), "0.10.0".to_string());

        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
    }

    #[test]
    fn test_sdk_compatibility_go_version_without_v_prefix_invalid() {
        let sdk_version = "0.9.0";
        let sdk_language = "go";

        let mut min_supported_sdk_versions = HashMap::new();
        min_supported_sdk_versions.insert("go".to_string(), "0.10.0".to_string());

        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
    }
}
