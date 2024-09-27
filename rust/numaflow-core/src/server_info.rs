use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use pep440_rs::{Version as PepVersion, VersionSpecifier};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{self, Error};
use crate::server_info::version::SdkConstraints;

// Constant to represent the end of the server info.
// Equivalent to U+005C__END__.
const END: &str = "U+005C__END__";

/// ServerInfo structure to store server-related information
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ServerInfo {
    #[serde(default)]
    pub(crate) protocol: String,
    #[serde(default)]
    pub(crate) language: String,
    #[serde(default)]
    pub(crate) minimum_numaflow_version: String,
    #[serde(default)]
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) metadata: Option<HashMap<String, String>>, // Metadata is optional
}

/// check_for_server_compatibility waits until the server info file is ready and check whether the
/// server is compatible with Numaflow.
pub(crate) async fn check_for_server_compatibility(
    file_path: PathBuf,
    cln_token: CancellationToken,
) -> error::Result<()> {
    // Read the server info file
    let server_info = read_server_info(file_path, cln_token).await?;

    // Log the server info
    info!("Server info file: {:?}", server_info);

    // Extract relevant fields from server info
    let sdk_version = &server_info.version;
    let min_numaflow_version = &server_info.minimum_numaflow_version;
    let sdk_language = &server_info.language;
    // Get version information
    let version_info = version::get_version_info();
    let numaflow_version = &version_info.version;

    info!("Version_info: {:?}", version_info);

    // Check minimum numaflow version compatibility if specified
    if min_numaflow_version.is_empty() {
        warn!("Failed to get the minimum numaflow version, skipping numaflow version compatibility check");
    } else if !numaflow_version.contains("latest")
        && !numaflow_version.contains(&version_info.git_commit)
    {
        // Check the compatibility between the SDK and Numaflow versions
        // If any error occurs, return the error
        check_numaflow_compatibility(numaflow_version, min_numaflow_version)?;
    }

    // Check SDK compatibility if version and language are specified
    if sdk_version.is_empty() || sdk_language.is_empty() {
        warn!("Failed to get the SDK version/language, skipping SDK version compatibility check");
    } else {
        // Get minimum supported SDK versions and check compatibility
        let min_supported_sdk_versions = version::get_minimum_supported_sdk_versions();
        check_sdk_compatibility(sdk_version, sdk_language, min_supported_sdk_versions)?;
    }

    Ok(())
}

/// Checks if the current numaflow version is compatible with the given minimum numaflow version.
fn check_numaflow_compatibility(
    numaflow_version: &str,
    min_numaflow_version: &str,
) -> error::Result<()> {
    // Ensure that the minimum numaflow version is specified
    if min_numaflow_version.is_empty() {
        return Err(Error::ServerInfoError("invalid version".to_string()));
    }

    // Strip the 'v' prefix if present.
    let numaflow_version_stripped = numaflow_version.trim_start_matches('v');

    // Parse the provided numaflow version as a semantic version
    let numaflow_version_semver = Version::parse(numaflow_version_stripped)
        .map_err(|e| Error::ServerInfoError(format!("Error parsing Numaflow version: {}", e)))?;

    // Create a version constraint based on the minimum numaflow version
    let numaflow_constraint = format!(">={}", min_numaflow_version);
    check_constraint(&numaflow_version_semver, &numaflow_constraint).map_err(|e| {
        Error::ServerInfoError(format!(
            "numaflow version {} must be upgraded to at least {}, in order to work with current SDK version {}",
            numaflow_version_semver, human_readable(min_numaflow_version), e
        ))
    })
}

/// Checks if the current SDK version is compatible with the given language's minimum supported SDK version.
fn check_sdk_compatibility(
    sdk_version: &str,
    sdk_language: &str,
    min_supported_sdk_versions: &SdkConstraints,
) -> error::Result<()> {
    // Check if the SDK language is present in the minimum supported SDK versions
    if let Some(sdk_required_version) = min_supported_sdk_versions.get(sdk_language) {
        let sdk_constraint = format!(">={}", sdk_required_version);

        // For Python, use Pep440 versioning
        if sdk_language.to_lowercase() == "python" {
            let sdk_version_pep440 = PepVersion::from_str(sdk_version)
                .map_err(|e| Error::ServerInfoError(format!("Error parsing SDK version: {}", e)))?;

            let specifiers = VersionSpecifier::from_str(&sdk_constraint).map_err(|e| {
                Error::ServerInfoError(format!("Error parsing SDK constraint: {}", e))
            })?;

            if !specifiers.contains(&sdk_version_pep440) {
                return Err(Error::ServerInfoError(format!(
                    "SDK version {} must be upgraded to at least {}, in order to work with the current numaflow version",
                    sdk_version_pep440, human_readable(sdk_required_version)
                )));
            }
        } else {
            // Strip the 'v' prefix if present for non-Python languages
            let sdk_version_stripped = sdk_version.trim_start_matches('v');

            // Parse the SDK version using semver
            let sdk_version_semver = Version::parse(sdk_version_stripped)
                .map_err(|e| Error::ServerInfoError(format!("Error parsing SDK version: {}", e)))?;

            // Check if the SDK version satisfies the constraint
            check_constraint(&sdk_version_semver, &sdk_constraint).map_err(|_| {
                Error::ServerInfoError(format!(
                    "SDK version {} must be upgraded to at least {}, in order to work with the current numaflow version",
                    sdk_version_semver, human_readable(sdk_required_version)
                ))
            })?;
        }
    } else {
        // Language not found in the supported SDK versions
        warn!(
            "SDK version constraint not found for language: {}",
            sdk_language
        );

        // Return error indicating the language
        return Err(Error::ServerInfoError(format!(
            "SDK version constraint not found for language: {}",
            sdk_language
        )));
    }
    Ok(())
}

// human_readable returns the human-readable minimum supported version.
// it's used for logging purposes.
// it translates the version we used in the constraints to the real minimum supported version.
// e.g., if the given version is "0.8.0rc100", human-readable version is "0.8.0".
// if the given version is "0.8.0-z", "0.8.0".
// if "0.8.0-rc1", "0.8.0-rc1".
fn human_readable(ver: &str) -> String {
    if ver.is_empty() {
        return String::new();
    }
    // semver
    if ver.ends_with("-z") {
        return ver[..ver.len() - 2].to_string();
    }
    // PEP 440
    if ver.ends_with("rc100") {
        return ver[..ver.len() - 5].to_string();
    }
    ver.to_string()
}

/// Checks if the given version meets the specified constraint.
fn check_constraint(version: &Version, constraint: &str) -> error::Result<()> {
    let binding = version.to_string();
    // extract the major.minor.patch version
    let mmp_version =
        Version::parse(binding.split('-').next().unwrap_or_default()).map_err(|e| {
            Error::ServerInfoError(format!(
                "Error parsing version: {}, version string: {}",
                e, binding
            ))
        })?;
    let mmp_ver_str_constraint = trim_after_dash(constraint.trim_start_matches(">="));
    let mmp_ver_constraint = format!(">={}", mmp_ver_str_constraint);

    // "-z" is used to indicate the minimum supported version is a stable version
    // the reason why we choose the letter z is that it can represent the largest pre-release version.
    // e.g., 0.8.0-z means the minimum supported version is 0.8.0.
    if constraint.contains("-z") {
        if !version.to_string().starts_with(mmp_ver_str_constraint) {
            // if the version is prefixed with a different mmp version,
            // rust semver lib can't figure out the correct order.
            // to work around, we compare the mmp version only.
            // e.g., rust semver doesn't treat 0.9.0-rc* as larger than 0.8.0.
            // to work around, instead of comparing 0.9.0-rc* with 0.8.0,
            // we compare 0.9.0 with 0.8.0.
            return check_constraint(&mmp_version, &mmp_ver_constraint);
        }
        return check_constraint(version, &mmp_ver_constraint);
    } else if constraint.contains("-") {
        // if the constraint doesn't contain "-z", but contains "-", it's a pre-release version.
        if !version.to_string().starts_with(mmp_ver_str_constraint) {
            // similar reason as above, we compare the mmp version only.
            return check_constraint(&mmp_version, &mmp_ver_constraint);
        }
    }

    // TODO - remove all the extra check above once rust semver handles pre-release comparison the same way as golang.
    // https://github.com/dtolnay/semver/issues/323

    // Parse the given constraint as a semantic version requirement
    let version_req = VersionReq::parse(constraint).map_err(|e| {
        Error::ServerInfoError(format!(
            "Error parsing constraint: {}, constraint string: {}",
            e, constraint
        ))
    })?;

    // Check if the provided version satisfies the parsed constraint
    if !version_req.matches(version) {
        return Err(Error::ServerInfoError("invalid version".to_string()));
    }

    Ok(())
}

fn trim_after_dash(input: &str) -> &str {
    if let Some(pos) = input.find('-') {
        &input[..pos]
    } else {
        input
    }
}

/// Reads the server info file and returns the parsed ServerInfo struct.
/// The cancellation token is used to stop ready-check of server_info file in case it is missing.
/// This cancellation token is closed via the global shutdown handler.
async fn read_server_info(
    file_path: PathBuf,
    cln_token: CancellationToken,
) -> error::Result<ServerInfo> {
    // Infinite loop to keep checking until the file is ready
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::ServerInfoError("Operation cancelled".to_string()));
        }

        // Check if the file exists and has content
        if let Ok(metadata) = fs::metadata(file_path.as_path()) {
            if metadata.len() > 0 {
                // Break out of the loop if the file is ready (has content)
                break;
            }
        }
        // Log message indicating the file is not ready and sleep for 1 second before checking again
        info!("Server info file {:?} is not ready, waiting...", file_path);
        sleep(Duration::from_secs(1)).await;
    }

    // Retry logic for reading the file
    let mut retry = 0;
    let contents;
    loop {
        // Attempt to read the file
        match fs::read_to_string(file_path.as_path()) {
            Ok(data) => {
                if data.ends_with(END) {
                    // If the file ends with the END marker, trim it and break out of the loop
                    contents = data.trim_end_matches(END).to_string();
                    break;
                } else {
                    warn!("Server info file is incomplete, EOF is missing...");
                }
            }
            Err(e) => {
                warn!("Failed to read file: {}", e);
            }
        }

        // Retry limit logic
        retry += 1;
        if retry >= 10 {
            // Return an error if the retry limit is reached
            return Err(Error::ServerInfoError(
                "server-info reading retry exceeded".to_string(),
            ));
        }

        sleep(Duration::from_millis(100)).await; // Sleep before retrying
    }

    // Parse the JSON; if there is an error, return the error
    let server_info: ServerInfo = serde_json::from_str(&contents).map_err(|e| {
        Error::ServerInfoError(format!(
            "Failed to parse server-info file: {}, contents: {}",
            e, contents
        ))
    })?;

    Ok(server_info) // Return the parsed server info
}

/// create a mod for version.rs
mod version {
    use std::collections::HashMap;
    use std::env;

    use once_cell::sync::Lazy;

    pub(crate) type SdkConstraints = HashMap<String, String>;

    // MINIMUM_SUPPORTED_SDK_VERSIONS is a HashMap with SDK language as key and minimum supported version as value
    static MINIMUM_SUPPORTED_SDK_VERSIONS: Lazy<SdkConstraints> = Lazy::new(|| {
        // TODO: populate this from a static file and make it part of the release process
        // the value of the map matches `minimumSupportedSDKVersions` in pkg/sdkclient/serverinfo/types.go
        // please follow the instruction there to update the value
        let mut m = HashMap::new();
        m.insert("go".to_string(), "0.8.0-z".to_string());
        m.insert("python".to_string(), "0.8.0rc100".to_string());
        m.insert("java".to_string(), "0.8.0-z".to_string());
        m.insert("rust".to_string(), "0.1.0-z".to_string());
        m
    });

    // Function to get the minimum supported SDK version hash map
    pub(crate) fn get_minimum_supported_sdk_versions() -> &'static SdkConstraints {
        &MINIMUM_SUPPORTED_SDK_VERSIONS
    }

    /// Struct to hold version information.
    #[derive(Debug, PartialEq)]
    pub struct VersionInfo {
        pub version: String,
        pub build_date: String,
        pub git_commit: String,
        pub git_tag: String,
        pub git_tree_state: String,
        pub go_version: String,
        pub compiler: String,
        pub platform: String,
    }

    impl VersionInfo {
        /// Initialize with environment variables or default values.
        fn init() -> Self {
            let version = env::var("VERSION").unwrap_or_else(|_| "latest".to_string());
            let build_date =
                env::var("BUILD_DATE").unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());
            let git_commit = env::var("GIT_COMMIT").unwrap_or_default();
            let git_tag = env::var("GIT_TAG").unwrap_or_default();
            let git_tree_state = env::var("GIT_TREE_STATE").unwrap_or_default();
            let go_version = env::var("GO_VERSION").unwrap_or_else(|_| "unknown".to_string());
            let compiler = env::var("COMPILER").unwrap_or_default();
            let platform = env::var("PLATFORM")
                .unwrap_or_else(|_| format!("{}/{}", env::consts::OS, env::consts::ARCH));

            let version_str =
                if !git_commit.is_empty() && !git_tag.is_empty() && git_tree_state == "clean" {
                    git_tag.clone()
                } else {
                    let mut version_str = version.clone();
                    if !git_commit.is_empty() && git_commit.len() >= 7 {
                        version_str.push_str(&format!("+{}", &git_commit[..7]));
                        if git_tree_state != "clean" {
                            version_str.push_str(".dirty");
                        }
                    } else {
                        version_str.push_str("+unknown");
                    }
                    version_str
                };

            VersionInfo {
                version: version_str,
                build_date,
                git_commit,
                git_tag,
                git_tree_state,
                go_version,
                compiler,
                platform,
            }
        }
    }

    /// Use once_cell::sync::Lazy for thread-safe, one-time initialization
    static VERSION_INFO: Lazy<VersionInfo> = Lazy::new(VersionInfo::init);

    /// Getter function for VersionInfo
    pub fn get_version_info() -> &'static VersionInfo {
        &VERSION_INFO
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::io::{Read, Write};
    use std::{collections::HashMap, fs::File};
    use tempfile::tempdir;

    use super::*;

    // Constants for the tests
    const MINIMUM_NUMAFLOW_VERSION: &str = "1.2.0-rc4";
    const TCP: &str = "tcp";
    const PYTHON: &str = "python";
    const GOLANG: &str = "go";

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
        let file = File::create(svr_info_file_path);

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

    // Helper function to create a SdkConstraints struct with minimum supported SDK versions all being stable releases
    fn create_sdk_constraints_stable_versions() -> SdkConstraints {
        let mut constraints = HashMap::new();
        constraints.insert("python".to_string(), "1.2.0rc100".to_string());
        constraints.insert("java".to_string(), "2.0.0-z".to_string());
        constraints.insert("go".to_string(), "0.10.0-z".to_string());
        constraints.insert("rust".to_string(), "0.1.0-z".to_string());
        constraints
    }

    // Helper function to create a SdkConstraints struct with minimum supported SDK versions all being pre-releases
    fn create_sdk_constraints_pre_release_versions() -> SdkConstraints {
        let mut constraints = HashMap::new();
        constraints.insert("python".to_string(), "1.2.0b2".to_string());
        constraints.insert("java".to_string(), "2.0.0-rc2".to_string());
        constraints.insert("go".to_string(), "0.10.0-rc2".to_string());
        constraints.insert("rust".to_string(), "0.1.0-rc3".to_string());
        constraints
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_stable_release_valid() {
        let sdk_version = "1.3.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_stable_release_invalid() {
        let sdk_version = "1.1.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
            "SDK version 1.1.0 must be upgraded to at least 1.2.0, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_pre_release_valid() {
        let sdk_version = "v1.3.0a1";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_pre_release_invalid() {
        let sdk_version = "1.1.0a1";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 1.1.0a1 must be upgraded to at least 1.2.0, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_java_stable_release_valid() {
        let sdk_version = "v2.1.0";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_java_rc_release_invalid() {
        let sdk_version = "2.0.0-rc1";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 2.0.0-rc1 must be upgraded to at least 2.0.0, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_go_rc_release_valid() {
        let sdk_version = "0.11.0-rc2";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_go_pre_release_invalid() {
        let sdk_version = "0.10.0-0.20240913163521-4910018031a7";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 0.10.0-0.20240913163521-4910018031a7 must be upgraded to at least 0.10.0, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_rust_pre_release_valid() {
        let sdk_version = "v0.1.1-0.20240913163521-4910018031a7";
        let sdk_language = "rust";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_rust_stable_release_invalid() {
        let sdk_version = "0.0.9";
        let sdk_language = "rust";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
            "ServerInfoError Error - SDK version 0.0.9 must be upgraded to at least 0.1.0, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_stable_release_valid() {
        let sdk_version = "1.3.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_stable_release_invalid() {
        let sdk_version = "1.1.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 1.1.0 must be upgraded to at least 1.2.0b2, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_pre_release_valid() {
        let sdk_version = "v1.3.0a1";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_pre_release_invalid() {
        let sdk_version = "1.2.0a1";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 1.2.0a1 must be upgraded to at least 1.2.0b2, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_java_stable_release_valid() {
        let sdk_version = "v2.1.0";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_java_rc_release_invalid() {
        let sdk_version = "2.0.0-rc1";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 2.0.0-rc1 must be upgraded to at least 2.0.0-rc2, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_go_rc_release_valid() {
        let sdk_version = "0.11.0-rc2";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_go_pre_release_invalid() {
        let sdk_version = "0.10.0-0.20240913163521-4910018031a7";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "SDK version 0.10.0-0.20240913163521-4910018031a7 must be upgraded to at least 0.10.0-rc2, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_rust_pre_release_valid() {
        let sdk_version = "v0.1.1-0.20240913163521-4910018031a7";
        let sdk_language = "rust";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_rust_stable_release_invalid() {
        let sdk_version = "0.0.9";
        let sdk_language = "rust";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result =
            check_sdk_compatibility(sdk_version, sdk_language, &min_supported_sdk_versions);

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "ServerInfoError Error - SDK version 0.0.9 must be upgraded to at least 0.1.0-rc3, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_invalid_version_string() {
        let numaflow_version = "v1.abc.7";
        let min_numaflow_version = "1.1.6-z";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "Error parsing Numaflow version: unexpected character 'a' while parsing minor version number"));
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_stable_version_stable_valid() {
        let numaflow_version = "v1.1.7";
        let min_numaflow_version = "1.1.6-z";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_stable_version_stable_invalid() {
        let numaflow_version = "v1.1.6";
        let min_numaflow_version = "1.1.7-z";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "numaflow version 1.1.6 must be upgraded to at least 1.1.7, in order to work with current SDK version"));
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_stable_version_pre_release_valid() {
        let numaflow_version = "1.1.7-rc1";
        let min_numaflow_version = "1.1.6-z";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_stable_version_pre_release_invalid() {
        let numaflow_version = "v1.1.6-rc1";
        let min_numaflow_version = "1.1.6-z";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "numaflow version 1.1.6-rc1 must be upgraded to at least 1.1.6, in order to work with current SDK version"));
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_rc_version_stable_invalid() {
        let numaflow_version = "v1.1.6";
        let min_numaflow_version = "1.1.7-rc1";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "numaflow version 1.1.6 must be upgraded to at least 1.1.7-rc1, in order to work with current SDK version"));
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_rc_version_stable_valid() {
        let numaflow_version = "1.1.7";
        let min_numaflow_version = "1.1.6-rc1";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_rc_version_pre_release_valid() {
        let numaflow_version = "1.1.7-rc3";
        let min_numaflow_version = "1.1.7-rc2";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_numaflow_compatibility_min_rc_version_pre_release_invalid() {
        let numaflow_version = "v1.1.6-rc1";
        let min_numaflow_version = "1.1.6-rc2";

        let result = check_numaflow_compatibility(numaflow_version, min_numaflow_version);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "numaflow version 1.1.6-rc1 must be upgraded to at least 1.1.6-rc2, in order to work with current SDK version"));
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
        let file_path = std::path::PathBuf::from("/invalid/path/server_info.txt");

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
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("server_info.txt");

        let cln_token = CancellationToken::new();
        let _drop_guard = cln_token.clone().drop_guard();

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
        let result = read_server_info(file_path, cln_token).await;
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);

        let server_info = result.unwrap();
        assert_eq!(server_info.protocol, "tcp");
        assert_eq!(server_info.language, "python");
        assert_eq!(server_info.minimum_numaflow_version, "1.2.0-rc4");
        assert_eq!(server_info.version, "1.0.0");
        // Check metadata
        assert!(server_info.metadata.is_some());
        let server_info = server_info.metadata.unwrap();
        assert_eq!(server_info.len(), 1);
        assert_eq!(server_info.get("key1").unwrap(), "value1");
    }

    #[tokio::test]
    async fn test_read_server_info_retry_limit() {
        // Create a temporary directory
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("server_info.txt");

        // Write a partial test file not ending with END marker
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"protocol":"tcp","language":"go","minimum_numaflow_version":"1.2.0-rc4","version":"1.0.0","metadata":{{"key1":"value1"}}}}"#).unwrap();

        let cln_token = CancellationToken::new();
        let _drop_guard = cln_token.clone().drop_guard();

        // Call the read_server_info function
        let result = read_server_info(file_path, cln_token).await;
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

        let _expected_server_info = ServerInfo {
            protocol: "uds".to_string(),
            language: "go".to_string(),
            minimum_numaflow_version: "1.2.0-rc4".to_string(),
            version: "v0.7.0-rc2".to_string(),
            metadata: Some(HashMap::new()), // Expecting an empty HashMap here
        };

        let _parsed_server_info: ServerInfo =
            serde_json::from_str(&json_data).expect("Failed to parse JSON");
    }
}
