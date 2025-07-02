use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, io};
use tracing::{debug, error};

/// CheckFileExists checks if a file with the given fileName exists in the file system.
fn check_file_exists<P: AsRef<Path>>(file_name: P) -> bool {
    file_name.as_ref().exists()
}

/// UpdateSideInputFile writes the given side input value to a new file
/// and updates the side input store path to point to this new file.
pub(super) fn update_side_input_file<P: AsRef<Path>>(
    file_symlink: P,
    value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let file_symlink = file_symlink.as_ref();

    // Generate a new file name using timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("Failed to get timestamp: {e}"))?
        .as_nanos();

    let new_file_name = format!("{}_{}", file_symlink.display(), timestamp);

    // Fetch the current side input value from the file
    let current_value = fetch_side_input_file_value(file_symlink);

    // Check if the current value is same as the new value
    // If true then don't update file again and return
    if let Ok(current) = current_value
        && current == value
    {
        debug!(
            side_input = %file_symlink.display(),
            "Side Input value is same as current value, skipping update"
        );
        return Ok(());
    }

    // atomically write the new file, this is done by creating a tmp file and then renaming it
    fs::write(&new_file_name, value)
        .map_err(|e| format!("Failed to write Side Input file {new_file_name}: {e}"))?;

    let old_file_path = fs::read_link(file_symlink).ok();

    let symlink_path_tmp = format!("{}_temp_{}", file_symlink.display(), timestamp);

    std::os::unix::fs::symlink(&new_file_name, &symlink_path_tmp)
        .map_err(|e| format!("Failed to create temp symlink: {e}"))?;

    // Update the symlink to point to the new file
    fs::rename(&symlink_path_tmp, file_symlink).map_err(|e| {
        format!("Failed to update symlink for Side Input file {new_file_name}: {e}",)
    })?;

    // Remove the old file
    if let Some(old_path) = old_file_path
        && check_file_exists(&old_path)
        && let Err(e) = fs::remove_file(&old_path)
    {
        error!(
            old_file_path = %old_path.display(),
            error = %e,
            "Failed to remove old Side Input file"
        );
    }

    Ok(())
}

/// FetchSideInputFileValue reads a given file and returns the value in bytes
fn fetch_side_input_file_value<P: AsRef<Path>>(file_path: P) -> Result<Vec<u8>, io::Error> {
    let file_path = file_path.as_ref();
    fs::read(file_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "Failed to read Side Input {} file: {}",
                file_path.display(),
                e
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_check_file_exists() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");

        // File doesn't exist initially
        assert!(!check_file_exists(&file_path));

        // Create file
        fs::write(&file_path, b"test content").unwrap();

        // File should exist now
        assert!(check_file_exists(&file_path));
    }

    #[test]
    fn test_fetch_side_input_file_value() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file.txt");
        let test_content = b"test content";

        // Create file with test content
        fs::write(&file_path, test_content).unwrap();

        // Read file content
        let result = fetch_side_input_file_value(&file_path).unwrap();
        assert_eq!(result, test_content);
    }

    #[test]
    fn test_fetch_side_input_file_value_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        // Should return error for non-existent file
        assert!(fetch_side_input_file_value(&file_path).is_err());
    }

    #[test]
    fn test_update_side_input_file() {
        let temp_dir = TempDir::new().unwrap();
        let symlink_path = temp_dir.path().join("test_symlink");
        let test_content = b"test content";

        // Test updating side input file
        let result = update_side_input_file(&symlink_path, test_content);
        assert!(result.is_ok());

        // Verify the symlink was created and points to a file with correct content
        assert!(symlink_path.is_symlink());
        let content = fetch_side_input_file_value(&symlink_path).unwrap();
        assert_eq!(content, test_content);
    }

    #[test]
    fn test_update_side_input_file_same_content() {
        let temp_dir = TempDir::new().unwrap();
        let symlink_path = temp_dir.path().join("test_symlink");
        let test_content = b"test content";

        // First update
        update_side_input_file(&symlink_path, test_content).unwrap();

        // Get the target file before second update
        let first_target = fs::read_link(&symlink_path).unwrap();

        // Second update with same content
        update_side_input_file(&symlink_path, test_content).unwrap();

        // Get the target file after second update
        let second_target = fs::read_link(&symlink_path).unwrap();

        // Should be the same file (no update occurred)
        assert_eq!(first_target, second_target);
    }
}
