use std::process::Command;
use std::{env, fmt};

#[derive(Debug, Clone)]
pub struct Version {
    pub version: String,
    pub build_date: String,
    pub git_commit: String,
    pub git_tag: String,
    pub git_tree_state: String,
    pub rust_version: String,
    pub platform: String,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Version: {}, BuildDate: {}, GitCommit: {}, GitTag: {}, GitTreeState: {}, RustVersion: {}, Platform: {}",
            self.version,
            self.build_date,
            self.git_commit,
            self.git_tag,
            self.git_tree_state,
            self.rust_version,
            self.platform
        )
    }
}

/// Returns the version information
pub fn get_version() -> Result<Version, Box<dyn std::error::Error>> {
    let version = std::env::var("VERSION").unwrap_or_else(|_| "latest".to_string());
    let build_date = get_build_date();
    let git_commit = get_git_commit()?;

    let git_tree_state = get_git_tree_state()?;
    let git_tag = get_git_tag()?;

    // Get Rust version
    let rust_version = get_rust_version()?;

    let version_str = if !git_commit.is_empty() && !git_tag.is_empty() && git_tree_state == "clean"
    {
        // if we have a clean tree state and the current commit is tagged,
        // this is an official release.
        git_tag.to_string()
    } else {
        // otherwise formulate a version string based on as much metadata
        // information we have available.
        let mut version_str = version.to_string();
        if git_commit.len() >= 7 {
            version_str.push('+');
            version_str.push_str(&git_commit[0..7]);
            if git_tree_state != "clean" {
                version_str.push_str(".dirty");
            }
        } else {
            version_str.push_str("+unknown");
        }
        version_str
    };

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let platform = format!("{}/{}", target_os, target_arch);

    Ok(Version {
        version: version_str,
        build_date: build_date.to_string(),
        git_commit: git_commit.to_string(),
        git_tag: git_tag.to_string(),
        git_tree_state: git_tree_state.to_string(),
        rust_version,
        platform,
    })
}

fn get_build_date() -> String {
    // If the binary is built within a container, this env variable will be set.
    if let Ok(build_date) = std::env::var("BUILD_DATE") {
        return build_date;
    }
    format!("{:?}", chrono::Utc::now())
}

fn get_git_commit() -> Result<String, Box<dyn std::error::Error>> {
    // If the binary is built within a container, this env variable will be set.
    if let Ok(git_commit) = std::env::var("GIT_COMMIT") {
        return Ok(git_commit);
    }
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    let git_commit = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(git_commit)
}

fn get_git_tree_state() -> Result<String, Box<dyn std::error::Error>> {
    // If the binary is built within a container, this env variable will be set.
    if let Ok(git_tree_state) = env::var("GIT_TREE_STATE") {
        return Ok(git_tree_state);
    }
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .output()?;
    let status_output = String::from_utf8(output.stdout)?;
    if status_output.trim().is_empty() {
        Ok("clean".to_string())
    } else {
        Ok("dirty".to_string())
    }
}

fn get_git_tag() -> Result<String, Box<dyn std::error::Error>> {
    // If the binary is built within a container, this env variable will be set.
    if let Ok(git_tag) = std::env::var("GIT_TAG") {
        return Ok(git_tag);
    }
    let output = Command::new("git")
        .args(&["describe", "--exact-match", "--tags", "HEAD"])
        .output()?;
    if output.status.success() {
        let git_tag = String::from_utf8(output.stdout)?.trim().to_string();
        Ok(git_tag)
    } else {
        Ok(String::new())
    }
}

fn get_rust_version() -> Result<String, Box<dyn std::error::Error>> {
    let rustc_binary = std::env::var("RUSTC").unwrap();
    let output = Command::new(rustc_binary).args(["--version"]).output()?;
    let rust_version = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(rust_version)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let version = get_version()?;
    println!("cargo:rustc-env=NUMAFLOW_VERSION_INFO={version}");
    Ok(())
}
