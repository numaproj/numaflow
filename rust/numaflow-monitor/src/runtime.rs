use axum::{response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

use crate::{config::info::RuntimeInfoConfig, error::Error};

#[derive(serde::Serialize)]
struct ApiResponse {
    errors: Vec<RuntimeErrorEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RuntimeErrorEntry {
    pub container_name: String,
    pub timestamp: String,
    pub code: String,
    pub message: String,
    pub details: String,
}
pub async fn handle_runtime_app_errors() -> impl IntoResponse {
    let runtime_info_config = RuntimeInfoConfig::default();
    let app_err_path = Path::new(runtime_info_config.app_error_path.as_str());
    let mut errors = Vec::new();
    if !app_err_path.exists() || !app_err_path.is_dir() {
        Error::FileError("returning as app error path does not exist".to_string());
        return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
    }
    let paths = match fs::read_dir(app_err_path) {
        Ok(path) => path,
        Err(e) => {
            Error::FileError(format!("Failed to read directory: {:?}", e));
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // iterate over all subdirectories and its files
    for entry in paths.flatten() {
        // A ud container will have its own directory
        let sub_dir_path = entry.path();
        if sub_dir_path.is_dir() {
            if let Ok(file_paths) = fs::read_dir(&sub_dir_path) {
                for file_entry in file_paths.flatten() {
                    process_file_entry(&file_entry, &mut errors);
                }
            }
        }
    }

    // FIXME: This is a dummy response, need to replace with actual response
    let api_response = ApiResponse { errors };
    (StatusCode::OK, Json(api_response)).into_response()
}

fn process_file_entry(file_entry: &fs::DirEntry, errors: &mut Vec<RuntimeErrorEntry>) {
    let file_path = file_entry.path();
    if !file_path.is_file() || !file_path.exists() {
        return;
    }
    match fs::read(&file_path) {
        Err(e) => {
            Error::FileError(format!("Failed to read file content: {:?}", e));
        }
        Ok(content) => match serde_json::from_slice::<RuntimeErrorEntry>(&content) {
            Ok(payload) => {
                errors.push(RuntimeErrorEntry {
                    container_name: payload.container_name,
                    timestamp: payload.timestamp,
                    code: payload.code,
                    message: payload.message,
                    details: payload.details,
                });
            }
            Err(e) => {
                Error::AppErrHandler(format!("Failed to deserialize content: {:?}", e));
            }
        },
    }
}
