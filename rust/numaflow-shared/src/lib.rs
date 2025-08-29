/// Error exposed by the shared library.
pub mod error;

/// Server Info for parsing the server-info file written by the SDK.
pub mod server_info;

/// ISB utilities.
pub mod isb;

const SECRET_BASE_PATH: &str = "/var/numaflow/secrets";

/// Retrieves the secret value from mounted secret volume
/// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
pub fn get_secret_from_volume(name: &str, key: &str) -> Result<String, String> {
    let path = format!("{SECRET_BASE_PATH}/{name}/{key}");
    let val = std::fs::read_to_string(path.clone())
        .map_err(|e| format!("Reading secret from file {path}: {e:?}"))?;
    Ok(val.trim().into())
}
