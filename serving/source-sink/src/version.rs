use lazy_static::lazy_static;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

pub(crate) type SdkConstraints = HashMap<String, String>;

// Function to get the minimum supported SDK version hash map
pub(crate) fn get_minimum_supported_sdk_versions() -> &'static SdkConstraints {
    &MINIMUM_SUPPORTED_SDK_VERSIONS
}

// Initialize the minimum supported SDK versions
lazy_static::lazy_static! {
    static ref MINIMUM_SUPPORTED_SDK_VERSIONS: SdkConstraints = {
        let mut m = HashMap::new();
        m.insert("go".to_string(), "0.7.0-rc2".to_string());
        m.insert("python".to_string(), "0.7.0a1".to_string());
        m.insert("java".to_string(), "0.7.2-0".to_string());
        m.insert("rust".to_string(), "0.0.1".to_string());
        m
    };
}

/// Struct to hold version information
#[derive(Debug)]
pub struct VersionInfo {
    pub version: String,
    pub build_date: String,
    pub git_commit: String,
    pub git_tag: String,
    pub git_tree_state: String,
    pub rust_version: String,
    pub compiler: String,
    pub platform: String,
}

impl VersionInfo {
    /// Initializes VersionInfo by reading environment variables or default values
    fn init() -> Self {
        let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "latest".to_string());
        let build_date =
            env::var("BUILD_DATE").unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());
        let git_commit = env::var("GIT_COMMIT").unwrap_or_else(|_| "".to_string());
        let git_tag = env::var("GIT_TAG").unwrap_or_else(|_| "".to_string());
        let git_tree_state = env::var("GIT_TREE_STATE").unwrap_or_else(|_| "".to_string());
        let rust_version = env::var("RUSTC_VERSION").unwrap_or_else(|_| "unknown".to_string());
        let platform = env::var("CARGO_PLATFORM").unwrap_or_else(|_| "unknown/unknown".to_string());

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
            build_date: build_date,
            git_commit: git_commit,
            git_tag: git_tag,
            git_tree_state: git_tree_state,
            rust_version: rust_version,
            compiler: "rustc".to_string(),
            platform: platform,
        }
    }

    // Getter for VersionInfo
    pub(crate) fn get_version_info() -> &'static VersionInfo {
        &VERSION_INFO
    }
}

lazy_static! {
    // Static reference to VersionInfo
    static ref VERSION_INFO: VersionInfo = VersionInfo::init();
}

#[cfg(test)]
mod tests {
    use crate::version::{VersionInfo, VERSION_INFO};
    use std::env;

    fn set_env_vars(
        version: &str,
        build_date: &str,
        git_commit: &str,
        git_tag: &str,
        git_tree_state: &str,
        rust_version: &str,
        platform: &str,
    ) {
        env::set_var("CARGO_PKG_VERSION", version);
        env::set_var("BUILD_DATE", build_date);
        env::set_var("GIT_COMMIT", git_commit);
        env::set_var("GIT_TAG", git_tag);
        env::set_var("GIT_TREE_STATE", git_tree_state);
        env::set_var("RUSTC_VERSION", rust_version);
        env::set_var("CARGO_PLATFORM", platform);
        // Re-initializing the lazy static as it doesn't reinitialize automatically on change
        lazy_static::initialize(&VERSION_INFO);
    }

    fn reset_env_vars() {
        env::remove_var("CARGO_PKG_VERSION");
        env::remove_var("BUILD_DATE");
        env::remove_var("GIT_COMMIT");
        env::remove_var("GIT_TAG");
        env::remove_var("GIT_TREE_STATE");
        env::remove_var("RUSTC_VERSION");
        env::remove_var("CARGO_PLATFORM");
    }

    #[test]
    fn test_version_string_output() {
        let v = VersionInfo {
            version: "1.0.0".to_string(),
            build_date: "2023-05-01T12:00:00Z".to_string(),
            git_commit: "abcdef1234567890".to_string(),
            git_tag: "v1.0.0".to_string(),
            git_tree_state: "clean".to_string(),
            rust_version: "1.55.0".to_string(),
            compiler: "rustc".to_string(),
            platform: "linux/amd64".to_string(),
        };

        let expected = "version: 1.0.0, BuildDate: 2023-05-01T12:00:00Z, GitCommit: abcdef1234567890, GitTag: v1.0.0, GitTreeState: clean, RustVersion: 1.55.0, Compiler: rustc, Platform: linux/amd64";

        // convert VersionInfo to string
        let version_str = format!(
            "version: {}, BuildDate: {}, GitCommit: {}, GitTag: {}, GitTreeState: {}, RustVersion: {}, Compiler: {}, Platform: {}",
            v.version, v.build_date, v.git_commit, v.git_tag, v.git_tree_state, v.rust_version, v.compiler, v.platform
        );
        // check if the string is as expected
        assert_eq!(version_str, expected);
    }

    #[test]
    fn test_get_version_with_clean_tree_and_tag() {
        set_env_vars(
            "dev",
            "2023-05-01T12:00:00Z",
            "1234567890abcdef",
            "v1.2.3",
            "clean",
            "1.55.0",
            "linux/amd64",
        );

        let v = VersionInfo::init();
        assert_eq!(v.version, "v1.2.3");

        reset_env_vars();
    }

    #[test]
    fn test_get_version_with_dirty_tree() {
        set_env_vars(
            "dev",
            "2023-05-01T12:00:00Z",
            "1234567890abcdef",
            "v1.2.3",
            "dirty",
            "1.55.0",
            "linux/amd64",
        );

        let v = VersionInfo::init();
        let expected = "dev+1234567.dirty";
        assert_eq!(v.version, expected);

        reset_env_vars();
    }

    #[test]
    fn test_get_version_with_unknown_commit() {
        set_env_vars(
            "dev",
            "2023-05-01T12:00:00Z",
            "",
            "",
            "clean",
            "1.55.0",
            "linux/amd64",
        );

        let v = VersionInfo::init();
        let expected = "dev+unknown";
        assert_eq!(v.version, expected);

        reset_env_vars();
    }

    #[test]
    fn test_get_version_runtime_info() {
        set_env_vars(
            "dev",
            "2023-05-01T12:00:00Z",
            "",
            "",
            "clean",
            "1.55.0",
            "linux/amd64",
        );

        let v = VersionInfo::init();

        if v.rust_version != env::var("RUSTC_VERSION").unwrap_or_default() {
            panic!(
                "GetVersion().rust_version = {}, want {}",
                v.rust_version,
                env::var("RUSTC_VERSION").unwrap()
            );
        }

        if v.compiler != "rustc" {
            panic!("GetVersion().compiler = {}, want rustc", v.compiler);
        }

        let expected_platform = env::var("CARGO_PLATFORM").unwrap_or_default();
        if v.platform != expected_platform {
            panic!(
                "GetVersion().platform = {}, want {}",
                v.platform, expected_platform
            );
        }
    }
}
