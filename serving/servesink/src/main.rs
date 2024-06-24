use std::error::Error;

use numaflow::sink::{self, Response, SinkRequest};
use reqwest::Client;
use tracing::{error, warn};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "servesink=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .init();

    sink::Server::new(Logger::new()).start().await
}

struct Logger {
    client: Client,
}

impl Logger {
    fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

#[tonic::async_trait]
impl sink::Sinker for Logger {
    async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        let mut responses: Vec<Response> = Vec::new();

        while let Some(datum) = input.recv().await {
            // do something better, but for now let's just log it.
            // please note that `from_utf8` is working because the input in this
            // example uses utf-8 data.
            let response = match std::str::from_utf8(&datum.value) {
                Ok(_v) => {
                    // record the response
                    Response::ok(datum.id)
                }
                Err(e) => Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e)),
            };
            // return the responses
            responses.push(response);
            let Some(url) = datum.headers.get("X-Numaflow-Callback-Url") else {
                warn!("X-Numaflow-Callback-Url header is not found in the payload");
                continue;
            };
            let Some(numaflow_id) = datum.headers.get("X-Numaflow-Id") else {
                warn!("X-Numaflow-Id header is not found in the payload");
                continue;
            };
            let resp = self
                .client
                .post(format!("{}_{}", url, "save"))
                .header("X-Numaflow-Id", numaflow_id)
                .header("id", numaflow_id)
                .body(datum.value)
                .send()
                .await;
            if let Err(e) = resp {
                error!(error=?e, url=url, "Sending result to numaserve")
            }
        }
        responses
    }
}
