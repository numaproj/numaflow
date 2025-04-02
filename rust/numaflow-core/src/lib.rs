use std::{sync::Arc, time::Duration};

use async_nats::{jetstream, ConnectOptions};
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::{config, CustomResourceType};

/// Custom Error handling.
mod error;
pub(crate) use crate::error::{Error, Result};

/// [MonoVertex] is a simplified version of the [Pipeline] spec which is ideal for high TPS, low latency
/// use-cases which do not require [ISB].
///
/// [MonoVertex]: https://numaflow.numaproj.io/core-concepts/monovertex/
/// [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
/// [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/
pub mod monovertex;

/// Parse configs, including Numaflow specifications.
mod config;

/// Internal message structure that is passed around.
mod message;
/// Shared entities that can be used orthogonal to different modules.
mod shared;
/// [Sink] serves as the endpoint for processed data that has been outputted from the platform,
/// which is then sent to an external system or application.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
mod sink;
/// [Source] is responsible for reliable reading data from an unbounded source into Numaflow.
///
/// [Source]: https://numaflow.numaproj.io/user-guide/sources/overview/
mod source;
/// Transformer is a feature that allows users to execute custom code to transform their data at
/// [source].
///
/// [Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/
mod transformer;

/// Reads from a stream.
mod reader;

pub(crate) mod metrics;
/// [Pipeline]
///
/// [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
mod pipeline;

/// Tracker to track the completeness of message processing.
mod tracker;

/// [Map] is a feature that allows users to execute custom code to transform their data.
///
/// [Map]: https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/
mod mapper;

/// Serving store to store the result of the serving pipeline.
mod serving_store;

/// [Watermark] _is a monotonically increasing timestamp of the oldest work/event not yet completed_
///
///
/// [Watermark]: https://numaflow.numaproj.io/core-concepts/watermarks/
mod watermark;

use numaflow_monitor::runtime::Runtime;

pub async fn run() -> Result<()> {
    let cln_token = CancellationToken::new();
    let shutdown_cln_token = cln_token.clone();

    // Initialize runtime for persisting errors
    let runtime = Runtime::new(None);

    // wait for SIG{INT,TERM} and invoke cancellation token.
    let shutdown_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_cln_token.cancel();
        Ok(())
    });

    let crd_type = config().custom_resource_type.clone();
    match crd_type {
        CustomResourceType::Serving(config) => {
            let mut opts = ConnectOptions::new()
                .max_reconnects(None) // unlimited reconnects
                .ping_interval(Duration::from_secs(3))
                .retry_on_initial_connect();

            if let Some((user, password)) = config.nats_basic_auth.as_ref().cloned() {
                opts = opts.user_and_password(user, password);
            }

            let js_client = async_nats::connect_with_options(&config.jetstream_url, opts)
                .await
                .map_err(|e| error::Error::Connection(e.to_string()))?;

            let js_context = jetstream::new(js_client);
            serving::start(js_context, Arc::new(config)).await.unwrap();
            todo!("Serving is not implemented");
        }
        CustomResourceType::MonoVertex(config) => {
            info!("Starting monovertex forwarder with config: {:#?}", config);
            // Run the forwarder with cancellation token.
            if let Err(e) = monovertex::start_forwarder(cln_token, &config).await {
                if let Error::Grpc(e) = e {
                    error!(error=?e, "Monovertex failed because of UDF failure");
                    runtime.persist_application_error(e)
                } else {
                    error!(?e, "Error running monovertex");
                }
                // abort the signal handler task since we have an error and we are shutting down
                if !shutdown_handle.is_finished() {
                    shutdown_handle.abort();
                }
            }
        }
        CustomResourceType::Pipeline(config) => {
            info!("Starting pipeline forwarder with config: {:#?}", config);
            if let Err(e) = pipeline::start_forwarder(cln_token, config).await {
                if let Error::Grpc(e) = e {
                    error!(error=?e, "Pipeline failed because of UDF failure");
                    runtime.persist_application_error(e)
                } else {
                    error!(?e, "Error running pipeline");
                }
                // abort the signal handler task since we have an error and we are shutting down
                if !shutdown_handle.is_finished() {
                    shutdown_handle.abort();
                }
            }
        }
    }

    info!("Gracefully Exiting...");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        info!("Received terminate signal");
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
