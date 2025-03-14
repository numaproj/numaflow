use axum::{response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use tracing::error;

use crate::{config::info::RuntimeInfoConfig, error::Error};

#[derive(serde::Serialize)]
struct ApiResponse {
    err_mssg: Option<String>,
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

/*
File Structure for application-errors

Root: /var/numaflow/runtime/
                    └── application-errors
                        └── udsource/
                                ├── ts1.json
                                └── ts2.json
                        └── udsink/
                                ├── ts3.json
                                └── ts4.json

*/
pub async fn handle_runtime_app_errors() -> impl IntoResponse {
    let runtime_info_config = RuntimeInfoConfig::default();
    let app_err_path = Path::new(runtime_info_config.app_error_path.as_str());
    let mut errors = Vec::new();
    // no app errors persisted yet
    if !app_err_path.exists() || !app_err_path.is_dir() {
        let err = Error::File("App Err path does not exist".to_string());
        error!("{}", err);
        return (
            StatusCode::NOT_FOUND,
            Json(ApiResponse {
                err_mssg: Some(err.to_string()),
                errors,
            }),
        )
            .into_response();
    }
    let paths = match fs::read_dir(app_err_path) {
        Ok(path) => path,
        Err(e) => {
            let err = Error::File(format!("Failed to read directory: {:?}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    err_mssg: Some(err.to_string()),
                    errors,
                }),
            )
                .into_response();
        }
    };

    // iterate over all subdirectories and its files
    for entry in paths.flatten() {
        // A ud container will have its own directory
        let sub_dir_path = entry.path();
        if !sub_dir_path.is_dir() {
            continue;
        }
        match fs::read_dir(&sub_dir_path) {
            Err(e) => {
                error!(
                    "{}",
                    Error::File(format!("Failed to read subdirectory: {:?}", e))
                );
                continue;
            }
            Ok(file_paths) => {
                for file_entry in file_paths.flatten() {
                    if let Err(e) = process_file_entry(&file_entry, &mut errors) {
                        error!(
                            "{}",
                            Error::File(format!(
                                "error: {} in processing file entry: {:?}",
                                e,
                                file_entry.file_name()
                            ))
                        );
                        continue;
                    }
                }
            }
        }
    }
    (
        StatusCode::OK,
        Json(ApiResponse {
            err_mssg: None,
            errors,
        }),
    )
        .into_response()
}

fn process_file_entry(
    file_entry: &fs::DirEntry,
    errors: &mut Vec<RuntimeErrorEntry>,
) -> Result<(), Error> {
    let file_path = file_entry.path();
    if !file_path.is_file() || !file_path.exists() {
        return Ok(());
    }
    match fs::read(&file_path) {
        Err(e) => {
            let err = Error::File(format!("Failed to read file content: {:?}", e));
            error!("{}", err);
            Err(err)
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
                Ok(())
            }
            Err(e) => {
                let err = Error::Deserialize(format!("Failed to deserialize content: {:?}", e));
                error!("{}", err);
                Err(err)
            }
        },
    }
}
