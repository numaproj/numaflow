use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
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
const MAP_MODE_KEY: &str = "MAP_MODE";
const HTTP_ENDPOINTS_KEY: &str = "MULTIPROC_ENDPOINTS";

/// A map can be run in different modes.
#[derive(Debug, Clone, PartialEq)]
pub enum MapMode {
    Unary,
    Batch,
    Stream,
}

impl From<&str> for MapMode {
    fn from(s: &str) -> Self {
        match s {
            "unary-map" => MapMode::Unary,
            "stream-map" => MapMode::Stream,
            "batch-map" => MapMode::Batch,
            _ => MapMode::Unary,
        }
    }
}

/// ContainerType represents the type of processor containers used in Numaflow.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum ContainerType {
    Sourcer,
    SourceTransformer,
    Sinker,
    Mapper,
    Reducer,
    ReduceStreamer,
    SessionReducer,
    Accumulator,
    SideInput,
    FbSinker,
    OnsSinker,
    Unknown,
}

impl ContainerType {
    /// Returns the string representation of the [ContainerType].
    pub fn as_str(&self) -> &'static str {
        match self {
            ContainerType::Sourcer => "sourcer",
            ContainerType::SourceTransformer => "sourcetransformer",
            ContainerType::Sinker => "sinker",
            ContainerType::Mapper => "mapper",
            ContainerType::Reducer => "reducer",
            ContainerType::ReduceStreamer => "reducestreamer",
            ContainerType::SessionReducer => "sessionreducer",
            ContainerType::Accumulator => "accumulator",
            ContainerType::SideInput => "sideinput",
            ContainerType::FbSinker => "fb-sinker",
            ContainerType::OnsSinker => "ons-sinker",
            ContainerType::Unknown => "unknown",
        }
    }
}

impl fmt::Display for ContainerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for ContainerType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "sourcer" => ContainerType::Sourcer,
            "sourcetransformer" => ContainerType::SourceTransformer,
            "sinker" => ContainerType::Sinker,
            "mapper" => ContainerType::Mapper,
            "reducer" => ContainerType::Reducer,
            "reducestreamer" => ContainerType::ReduceStreamer,
            "sessionreducer" => ContainerType::SessionReducer,
            "accumulator" => ContainerType::Accumulator,
            "sideinput" => ContainerType::SideInput,
            "fb-sinker" => ContainerType::FbSinker,
            "ons-sinker" => ContainerType::OnsSinker,
            _ => ContainerType::Unknown,
        }
    }
}

/// ServerInfo structure to store server-related information
#[derive(Serialize, Deserialize, Debug)]
pub struct ServerInfo {
    /// Protocol used by the server. This is for multi-proc mode.
    #[serde(default)]
    pub protocol: String,
    /// Language of the SDK/container. This is for publishing metrics.
    #[serde(default)]
    pub language: String,
    #[serde(default)]
    pub minimum_numaflow_version: String,
    /// SDK version used for publishing metrics.
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>, // Metadata is optional
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Protocol {
    #[allow(clippy::upper_case_acronyms)]
    TCP,
    #[allow(clippy::upper_case_acronyms)]
    UDS,
}

impl ServerInfo {
    /// get_map_mode returns the [MapMode] from the metadata.
    pub fn get_map_mode(&self) -> Option<MapMode> {
        if let Some(metadata) = &self.metadata
            && let Some(map_mode) = metadata.get(MAP_MODE_KEY)
        {
            return Some(map_mode.as_str().into());
        }
        None
    }

    /// get_http_endpoints returns the list of http endpoints for multi proc mode
    /// from the metadata.
    pub fn get_http_endpoints(&self) -> Vec<String> {
        if let Some(metadata) = &self.metadata
            && let Some(endpoints) = metadata.get(HTTP_ENDPOINTS_KEY)
        {
            return endpoints.split(',').map(|s| s.to_string()).collect();
        }
        vec![]
    }

    /// get_protocol returns the protocol used by the server. This is for multi-proc mode.
    pub fn get_protocol(&self) -> Protocol {
        match self.protocol.as_str() {
            "tcp" => Protocol::TCP,
            _ => Protocol::UDS,
        }
    }
}

/// sdk_server_info waits until the server info file is ready and check whether the
/// server is compatible with Numaflow.
pub async fn sdk_server_info(
    file_path: PathBuf,
    cln_token: CancellationToken,
) -> error::Result<ServerInfo> {
    // Read the server info file
    let server_info = read_server_info(&file_path, cln_token).await?;

    // Get the container type from the server info file
    let container_type = get_container_type(&file_path).unwrap_or(ContainerType::Unknown);

    // Log the server info
    info!(?container_type, ?server_info, "Server info file");

    // Extract relevant fields from server info
    let sdk_version = &server_info.version;
    let min_numaflow_version = &server_info.minimum_numaflow_version;
    let sdk_language = &server_info.language;

    // Get version information
    let version_info = version::get_version_info();
    let numaflow_version = &version_info.version;

    if min_numaflow_version.is_empty() {
        warn!(
            "Failed to get the minimum numaflow version, skipping numaflow version compatibility check"
        );
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
        check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &container_type,
            min_supported_sdk_versions,
        )?;
    }

    Ok(server_info)
}

/// Checks if the current numaflow version is compatible with the given minimum numaflow version.
fn check_numaflow_compatibility(
    numaflow_version: &str,
    min_numaflow_version: &str,
) -> error::Result<()> {
    // Ensure that the minimum numaflow version is specified
    if min_numaflow_version.is_empty() {
        return Err(Error::ServerInfo("invalid version".to_string()));
    }

    // Strip the 'v' prefix if present.
    let numaflow_version_stripped = numaflow_version.trim_start_matches('v');

    // Parse the provided numaflow version as a semantic version
    let numaflow_version_semver = Version::parse(numaflow_version_stripped)
        .map_err(|e| Error::ServerInfo(format!("Error parsing Numaflow version: {e}")))?;

    // Create a version constraint based on the minimum numaflow version
    let numaflow_constraint = format!(">={min_numaflow_version}");
    check_constraint(&numaflow_version_semver, &numaflow_constraint).map_err(|e| {
        Error::ServerInfo(format!(
            "numaflow version {} must be upgraded to at least {}, in order to work with current SDK version {}",
            numaflow_version_semver, human_readable(min_numaflow_version), e
        ))
    })
}

/// Checks if the current SDK version is compatible with the given language's minimum supported SDK version.
fn check_sdk_compatibility(
    sdk_version: &str,
    sdk_language: &str,
    container_type: &ContainerType,
    min_supported_sdk_versions: &SdkConstraints,
) -> error::Result<()> {
    // Check if the SDK language is present in the minimum supported SDK versions
    if !min_supported_sdk_versions.contains_key(sdk_language) {
        return Err(Error::ServerInfo(format!(
            "SDK version constraint not found for language: {sdk_language}, container type: {container_type}"
        )));
    }
    let empty_map = HashMap::new();
    let lang_constraints = min_supported_sdk_versions
        .get(sdk_language)
        .unwrap_or(&empty_map);
    if let Some(sdk_required_version) = lang_constraints.get(container_type) {
        let sdk_constraint = format!(">={sdk_required_version}");

        // For Python, use Pep440 versioning
        if sdk_language.to_lowercase() == "python" {
            let sdk_version_pep440 = PepVersion::from_str(sdk_version)
                .map_err(|e| Error::ServerInfo(format!("Error parsing SDK version: {e}")))?;

            let specifiers = VersionSpecifier::from_str(&sdk_constraint)
                .map_err(|e| Error::ServerInfo(format!("Error parsing SDK constraint: {e}")))?;

            if !specifiers.contains(&sdk_version_pep440) {
                return Err(Error::ServerInfo(format!(
                    "Python SDK version {} must be upgraded to at least {}, in order to work with the current numaflow version",
                    sdk_version_pep440,
                    human_readable(sdk_required_version)
                )));
            }
        } else {
            // Strip the 'v' prefix if present for non-Python languages
            let sdk_version_stripped = sdk_version.trim_start_matches('v');

            // Parse the SDK version using semver
            let sdk_version_semver = Version::parse(sdk_version_stripped)
                .map_err(|e| Error::ServerInfo(format!("Error parsing SDK version: {e}")))?;

            // Check if the SDK version satisfies the constraint
            check_constraint(&sdk_version_semver, &sdk_constraint).map_err(|_| {
                Error::ServerInfo(format!(
                    "SDK version {} must be upgraded to at least {}, in order to work with the current numaflow version",
                    sdk_version_semver, human_readable(sdk_required_version)
                ))
            })?;
        }
    } else {
        // Language not found in the supported SDK versions
        warn!(
            %sdk_language,
            %container_type, "SDK version constraint not found for language and container type"
        );

        // Return error indicating the language
        return Err(Error::ServerInfo(format!(
            "SDK version constraint not found for language: {sdk_language}, container type: {container_type}"
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
    if let Some(version) = ver.strip_suffix("-z") {
        return version.to_string();
    }
    // PEP 440
    if let Some(version) = ver.strip_suffix("rc100") {
        return version.to_string();
    }
    ver.to_string()
}

/// Checks if the given version meets the specified constraint.
fn check_constraint(version: &Version, constraint: &str) -> error::Result<()> {
    let binding = version.to_string();
    // extract the major.minor.patch version
    let mmp_version =
        Version::parse(binding.split('-').next().unwrap_or_default()).map_err(|e| {
            Error::ServerInfo(format!(
                "Error parsing version: {e}, version string: {binding}"
            ))
        })?;
    let mmp_ver_str_constraint = trim_after_dash(constraint.trim_start_matches(">="));
    let mmp_ver_constraint = format!(">={mmp_ver_str_constraint}");

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
        Error::ServerInfo(format!(
            "Error parsing constraint: {e}, constraint string: {constraint}"
        ))
    })?;

    // Check if the provided version satisfies the parsed constraint
    if !version_req.matches(version) {
        return Err(Error::ServerInfo("invalid version".to_string()));
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

/// Extracts the container type from the server info file.
/// The file name is in the format of <container_type>-server-info.
fn get_container_type(server_info_file: &Path) -> Option<ContainerType> {
    let file_name = server_info_file.file_name()?;
    let container_type = file_name.to_str()?.trim_end_matches("-server-info");
    Some(ContainerType::from(container_type.to_string()))
}

/// Reads the server info file and returns the parsed ServerInfo struct.
/// The cancellation token is used to stop ready-check of server_info file in case it is missing.
/// This cancellation token is closed via the global shutdown handler.
async fn read_server_info(
    file_path: &PathBuf,
    cln_token: CancellationToken,
) -> error::Result<ServerInfo> {
    // Infinite loop to keep checking until the file is ready
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::ServerInfo(format!(
                "Server info file {file_path:?} is not ready. \
                This indicates that the server has not started. \
                For more details https://numaflow.numaproj.io/user-guide/FAQ/#4-i-see-server-info-file-not-ready-log-in-the-numa-container-what-does-this-mean"
            )));
        }

        // Check if the file exists and has content
        if let Ok(metadata) = fs::metadata(file_path.as_path())
            && metadata.len() > 0
        {
            // Break out of the loop if the file is ready (has content)
            break;
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
                }
                warn!("Server info file is incomplete, EOF is missing...");
            }
            Err(e) => {
                warn!(error = ?e, ?file_path, "Failed to read server info file");
            }
        }

        // Retry limit logic
        retry += 1;
        if retry >= 10 {
            // Return an error if the retry limit is reached
            return Err(Error::ServerInfo(format!(
                "server-info reading retry exceeded for file: {file_path:?}"
            )));
        }

        sleep(Duration::from_millis(100)).await; // Sleep before retrying
    }

    // Parse the JSON; if there is an error, return the error
    let server_info: ServerInfo = serde_json::from_str(&contents).map_err(|e| {
        Error::ServerInfo(format!(
            "Failed to parse server-info file: {e}, contents: {contents}"
        ))
    })?;

    Ok(server_info) // Return the parsed server info
}

/// Checks if the given SDK version supports nack functionality
// DEPRECATE(0.12): We added this in 0.11
pub fn supports_nack(sdk_version: &str, sdk_language: &str) -> bool {
    // If version or language is empty, assume no nack support for safety
    if sdk_version.is_empty() || sdk_language.is_empty() {
        return false;
    }

    // Get nack support version constraints
    let nack_constraints = &version::NACK_SUPPORT_SDK_VERSIONS;

    // Check if the SDK language is present in the nack support versions
    if let Some(required_version) = nack_constraints.get(sdk_language) {
        let constraint = format!(">={required_version}");

        // For Python, use Pep440 versioning
        if sdk_language.to_lowercase() == "python" {
            if let Ok(sdk_version_pep440) = PepVersion::from_str(sdk_version)
                && let Ok(specifiers) = VersionSpecifier::from_str(&constraint)
            {
                return specifiers.contains(&sdk_version_pep440);
            }
        } else {
            // Strip the 'v' prefix if present for non-Python languages
            let sdk_version_stripped = sdk_version.trim_start_matches('v');

            // Parse the SDK version using semver
            if let Ok(sdk_version_semver) = Version::parse(sdk_version_stripped) {
                return check_constraint(&sdk_version_semver, &constraint).is_ok();
            }
        }
    }

    // Default to no nack support if we can't determine compatibility
    false
}

/// create a mod for version.rs
mod version {
    use std::collections::HashMap;
    use std::env;
    use std::sync::LazyLock;

    use super::ContainerType;

    pub(crate) type SdkConstraints = HashMap<String, HashMap<ContainerType, String>>;

    // MINIMUM_SUPPORTED_SDK_VERSIONS is the minimum supported version of each SDK for the current numaflow version.
    static MINIMUM_SUPPORTED_SDK_VERSIONS: LazyLock<SdkConstraints> = LazyLock::new(|| {
        // TODO: populate this from a static file and make it part of the release process
        // the value of the map matches `minimumSupportedSDKVersions` in pkg/sdkclient/serverinfo/types.go
        // please follow the instruction there to update the value
        // NOTE: the string content of the keys matches the corresponding server info file name.
        // DO NOT change it unless the server info file name is changed.
        let mut go_version_map: HashMap<ContainerType, String> = HashMap::new();
        go_version_map.insert(ContainerType::Sourcer, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::SourceTransformer, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::Sinker, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::FbSinker, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::Mapper, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::SideInput, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::Reducer, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::ReduceStreamer, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::SessionReducer, "0.9.0-z".to_string());
        go_version_map.insert(ContainerType::Accumulator, "0.10.0-z".to_string());
        go_version_map.insert(ContainerType::OnsSinker, "0.10.2-z".to_string());
        let mut python_version_map = HashMap::new();
        python_version_map.insert(ContainerType::Sourcer, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::SourceTransformer, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::Sinker, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::FbSinker, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::Mapper, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::SideInput, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::Reducer, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::ReduceStreamer, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::SessionReducer, "0.9.0rc100".to_string());
        python_version_map.insert(ContainerType::Accumulator, "0.10.0rc100".to_string());
        python_version_map.insert(ContainerType::OnsSinker, "0.11.1rc100".to_string());
        let mut java_version_map = HashMap::new();
        java_version_map.insert(ContainerType::Sourcer, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::SourceTransformer, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::Sinker, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::FbSinker, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::Mapper, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::SideInput, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::Reducer, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::ReduceStreamer, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::SessionReducer, "0.9.0-z".to_string());
        java_version_map.insert(ContainerType::Accumulator, "0.10.0-z".to_string());
        java_version_map.insert(ContainerType::OnsSinker, "0.10.2-z".to_string());
        let mut rust_version_map = HashMap::new();
        rust_version_map.insert(ContainerType::Sourcer, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::SourceTransformer, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::Sinker, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::FbSinker, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::Mapper, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::SideInput, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::Reducer, "0.2.0-z".to_string());
        rust_version_map.insert(ContainerType::ReduceStreamer, "0.3.0-z".to_string());
        rust_version_map.insert(ContainerType::SessionReducer, "0.3.0-z".to_string());
        rust_version_map.insert(ContainerType::Accumulator, "0.3.0-z".to_string());
        rust_version_map.insert(ContainerType::OnsSinker, "0.4.0-z".to_string());

        let mut m = HashMap::new();
        m.insert("go".to_string(), go_version_map);
        m.insert("python".to_string(), python_version_map);
        m.insert("java".to_string(), java_version_map);
        m.insert("rust".to_string(), rust_version_map);
        m
    });

    // Function to get the minimum supported SDK version hash map
    pub(crate) fn get_minimum_supported_sdk_versions() -> &'static SdkConstraints {
        &MINIMUM_SUPPORTED_SDK_VERSIONS
    }

    // NACK_SUPPORT_SDK_VERSIONS defines the minimum SDK versions that support nack functionality.
    pub(crate) static NACK_SUPPORT_SDK_VERSIONS: LazyLock<HashMap<String, String>> =
        LazyLock::new(|| {
            let mut m = HashMap::new();
            m.insert("go".to_string(), "0.11.0-z".to_string());
            m.insert("python".to_string(), "0.11.0rc100".to_string());
            m.insert("java".to_string(), "0.11.0-z".to_string());
            m.insert("rust".to_string(), "0.4.0-z".to_string());
            m
        });

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

    /// Use std::sync::LazyLock for thread-safe, one-time initialization
    static VERSION_INFO: LazyLock<VersionInfo> = LazyLock::new(VersionInfo::init);

    /// Getter function for VersionInfo
    pub fn get_version_info() -> &'static VersionInfo {
        &VERSION_INFO
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::{collections::HashMap, fs::File};

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    // Constants for the tests
    const MINIMUM_NUMAFLOW_VERSION: &str = "1.2.0-rc4";
    const TCP: &str = "tcp";
    const PYTHON: &str = "python";
    const GOLANG: &str = "go";
    const TEST_CONTAINER_TYPE: ContainerType = ContainerType::Sourcer;

    async fn write_server_info(
        svr_info: &ServerInfo,
        svr_info_file_path: &str,
    ) -> error::Result<()> {
        let serialized = serde_json::to_string(svr_info).unwrap();

        // Remove the existing file if it exists
        if let Err(e) = fs::remove_file(svr_info_file_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(Error::ServerInfo(format!(
                "Failed to remove server-info file: {}",
                e
            )));
        }

        // Create a new file
        let file = File::create(svr_info_file_path);

        // Extract the file from the Result
        let mut file = match file {
            Ok(f) => f,
            Err(e) => {
                return Err(Error::ServerInfo(format!(
                    "Failed to create server-info file: {}",
                    e
                )));
            }
        };

        // Write the serialized data and the END marker to the file
        // Remove the existing file if it exists
        if let Err(e) = file.write_all(serialized.as_bytes()) {
            return Err(Error::ServerInfo(format!(
                "Failed to write server-info file: {}",
                e
            )));
        }
        if let Err(e) = file.write_all(END.as_bytes()) {
            return Err(Error::ServerInfo(format!(
                "Failed to write server-info file: {}",
                e
            )));
        }
        Ok(())
    }

    // Helper function to create a SdkConstraints struct with minimum supported SDK versions all being stable releases
    fn create_sdk_constraints_stable_versions() -> SdkConstraints {
        let mut go_version_map = HashMap::new();
        go_version_map.insert(TEST_CONTAINER_TYPE, "0.10.0-z".to_string());
        let mut python_version_map = HashMap::new();
        python_version_map.insert(TEST_CONTAINER_TYPE, "1.2.0rc100".to_string());
        let mut java_version_map = HashMap::new();
        java_version_map.insert(TEST_CONTAINER_TYPE, "2.0.0-z".to_string());
        let mut rust_version_map = HashMap::new();
        rust_version_map.insert(TEST_CONTAINER_TYPE, "0.1.0-z".to_string());

        let mut m = HashMap::new();
        m.insert("go".to_string(), go_version_map);
        m.insert("python".to_string(), python_version_map);
        m.insert("java".to_string(), java_version_map);
        m.insert("rust".to_string(), rust_version_map);
        m
    }

    // Helper function to create a SdkConstraints struct with minimum supported SDK versions all being pre-releases
    fn create_sdk_constraints_pre_release_versions() -> SdkConstraints {
        let mut go_version_map = HashMap::new();
        go_version_map.insert(TEST_CONTAINER_TYPE, "0.10.0-rc2".to_string());
        let mut python_version_map = HashMap::new();
        python_version_map.insert(TEST_CONTAINER_TYPE, "1.2.0b2".to_string());
        let mut java_version_map = HashMap::new();
        java_version_map.insert(TEST_CONTAINER_TYPE, "2.0.0-rc2".to_string());
        let mut rust_version_map = HashMap::new();
        rust_version_map.insert(TEST_CONTAINER_TYPE, "0.1.0-rc3".to_string());

        let mut m = HashMap::new();
        m.insert("go".to_string(), go_version_map);
        m.insert("python".to_string(), python_version_map);
        m.insert("java".to_string(), java_version_map);
        m.insert("rust".to_string(), rust_version_map);
        m
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_stable_release_valid() {
        let sdk_version = "1.3.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_stable_release_invalid() {
        let sdk_version = "1.1.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_python_pre_release_invalid() {
        let sdk_version = "1.1.0a1";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_java_rc_release_invalid() {
        let sdk_version = "2.0.0-rc1";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_go_pre_release_invalid() {
        let sdk_version = "0.10.0-0.20240913163521-4910018031a7";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_stable_rust_stable_release_invalid() {
        let sdk_version = "0.0.9";
        let sdk_language = "rust";

        let min_supported_sdk_versions = create_sdk_constraints_stable_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "ServerInfo Error - SDK version 0.0.9 must be upgraded to at least 0.1.0, in order to work with the current numaflow version"));
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_stable_release_valid() {
        let sdk_version = "1.3.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_stable_release_invalid() {
        let sdk_version = "1.1.0";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_python_pre_release_invalid() {
        let sdk_version = "1.2.0a1";
        let sdk_language = "python";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_java_rc_release_invalid() {
        let sdk_version = "2.0.0-rc1";
        let sdk_language = "java";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_go_pre_release_invalid() {
        let sdk_version = "0.10.0-0.20240913163521-4910018031a7";
        let sdk_language = "go";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

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
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sdk_compatibility_min_pre_release_rust_stable_release_invalid() {
        let sdk_version = "0.0.9";
        let sdk_language = "rust";

        let min_supported_sdk_versions = create_sdk_constraints_pre_release_versions();
        let result = check_sdk_compatibility(
            sdk_version,
            sdk_language,
            &TEST_CONTAINER_TYPE,
            &min_supported_sdk_versions,
        );

        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains(
                "ServerInfo Error - SDK version 0.0.9 must be upgraded to at least 0.1.0-rc3, in order to work with the current numaflow version"));
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
    async fn test_get_container_type_from_file_valid() {
        let file_path = PathBuf::from("/var/run/numaflow/sourcer-server-info");
        let container_type = get_container_type(&file_path);
        assert_eq!(ContainerType::Sourcer, container_type.unwrap());
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
            matches!(error, Error::ServerInfo(_)),
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
        let result = read_server_info(&file_path, cln_token).await;
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
        let result = read_server_info(&file_path, cln_token).await;
        assert!(result.is_err(), "Expected Err, got {:?}", result);

        let error = result.unwrap_err();
        assert!(
            matches!(error, Error::ServerInfo(_)),
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
