//! Runs the user-defined side-input generator at specified intervals (cron expr).
use crate::create_js_context;
use crate::error::{Error, Result};
use crate::manager::client::UserDefinedSideInputClient;
use async_nats::jetstream;
use bytes::Bytes;
use chrono_tz::{Tz, UTC};
use cron::Schedule;
use numaflow_shared::isb::jetstream::config::ClientConfig;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// gRPC client to interact with the user-defined side-input generator.
pub(super) mod client;

/// Cron expression for the side-input trigger.
#[derive(Debug, Clone)]
pub(crate) struct SideInputTrigger {
    /// The schedule to trigger the creation of the side input data.
    pub(crate) schedule: Schedule,
    /// Timezone for the schedule, defaults to [UTC].
    pub(crate) timezone: Tz,
}

impl SideInputTrigger {
    /// Creates a new SideInputTrigger from the schedule string and optional timezone string.
    pub fn new(schedule_str: &'static str, timezone_str: Option<&'static str>) -> Result<Self> {
        let schedule = Schedule::from_str(schedule_str).map_err(|e| {
            Error::Schedule(format!(
                "Failed to parse cron expression: {}",
                e.to_string()
            ))
        })?;

        // Default to UTC if no timezone is provided
        let timezone = match timezone_str {
            Some(tz_str) => {
                if let Ok(tz) = tz_str.parse::<Tz>() {
                    tz
                } else {
                    UTC
                }
            }
            None => UTC,
        };

        Ok(Self { schedule, timezone })
    }
}

/// Manager creates the side-input content by running the user-defined code.
pub(crate) struct SideInputManager {
    side_input_store: &'static str,
    key: &'static str,
    user_defined_side_input_client: UserDefinedSideInputClient,
    cancellation_token: CancellationToken,
}

impl SideInputManager {
    pub(crate) fn new(
        side_input_store: &'static str,
        key: &'static str,
        user_defined_side_input_client: UserDefinedSideInputClient,
        cancellation_token: CancellationToken,
    ) -> Self {
        SideInputManager {
            side_input_store,
            key,
            user_defined_side_input_client,
            cancellation_token,
        }
    }

    pub(crate) async fn run(
        mut self,
        js_client_config: ClientConfig,
        side_input_trigger: SideInputTrigger,
    ) -> Result<()> {
        // Wait for the side-input client to be ready
        client::wait_until_sideinput_ready(
            &self.cancellation_token,
            &mut self.user_defined_side_input_client,
        )
        .await?;

        let js_context = create_js_context(js_client_config).await?;

        let bucket = js_context
            .get_key_value(self.side_input_store)
            .await
            .map_err(|e| {
                Error::SideInput(format!(
                    "Failed to get kv bucket {}: {}",
                    self.side_input_store, e
                ))
            })?;

        // Create a schedule from the cron expression
        self.run_schedule(bucket, side_input_trigger).await;

        Ok(())
    }

    /// Runs the side-input generation schedule. It will run the first time immediately and then
    /// run based on the schedule. It will not retry failures but will wait for next run to fix it.
    /// It honors [CancellationToken] to stop the schedule.
    async fn run_schedule(
        &mut self,
        bucket: jetstream::kv::Store,
        side_input_trigger: SideInputTrigger,
    ) {
        // do the first run before running the schedule
        info!("Running first side-input generation");
        if let Err(e) = self.generate_side_input(&bucket).await {
            error!(?e, "Failed to generate the first, initial side input");
        }

        loop {
            let datetime = side_input_trigger
                .schedule
                .upcoming(side_input_trigger.timezone)
                .next()
                .expect("cron schedule should have at least one next time");

            info!(scheduled=?datetime, "Next side-input generation scheduled");

            let duration_until = (datetime.with_timezone(&chrono::Utc) - chrono::Utc::now())
                .to_std()
                .unwrap_or(Duration::from_secs(0));

            // sleep until the next run time
            let sleep_util = tokio::time::sleep_until(tokio::time::Instant::from_std(
                Instant::now() + duration_until,
            ));

            // check if we have been cancelled
            tokio::select! {
                _ = sleep_util => {
                    debug!(scheduled=?datetime, "running schedule for side-input generation");
                }
                _ = self.cancellation_token.cancelled() => {
                    info!("Cancellation token received, exiting side-input manager");
                    break;
                }
            }

            // call the user-defined side-input client and store it in the bucket
            let result = self.generate_side_input(&bucket).await;
            if let Err(e) = result {
                warn!(
                    ?e,
                    "Failed to generate side input, hopefully, the next run will fix it."
                );
            }
        }
    }

    /// Calls the user-defined side-input client to generate the side-input data and stores it in
    /// the bucket.
    async fn generate_side_input(&mut self, bucket: &jetstream::kv::Store) -> Result<()> {
        let side_input_response = self
            .user_defined_side_input_client
            .retrieve_side_input()
            .await
            .map_err(|e| Error::SideInput(format!("Failed to retrieve side input: {e:?}")))?;

        if !side_input_response.no_broadcast {
            // store the side-input data in the bucket
            bucket
                .put(self.key, Bytes::from(side_input_response.value))
                .await
                .map_err(|e| Error::SideInput(format!("Failed to store side input: {e:?}")))?;
            info!("Side input stored in the bucket");
        } else {
            info!("Side input is not broadcasted since no_broadcast is set to true");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::sideinput;

    use numaflow::sideinput::SideInputer;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    struct SideInputHandler {}

    impl SideInputHandler {
        fn new() -> Self {
            Self {}
        }
    }

    #[tonic::async_trait]
    impl SideInputer for SideInputHandler {
        async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
            Some(b"test".to_vec())
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn side_input_operations() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sideinput.sock");
        let server_info_file = tmp_dir.path().join("sideinput-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        // start the server on a different thread
        let handle = tokio::spawn(async move {
            sideinput::Server::new(SideInputHandler::new())
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        let cancel = CancellationToken::new();

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let server_info_path = Box::leak(
            server_info_file
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );
        let client =
            UserDefinedSideInputClient::new(sock_file, (&server_info_path).into(), cancel.clone())
                .await?;

        // create trigger
        let side_input_trigger = SideInputTrigger::new("* * * * * *", None)?;

        // create manager
        let manager = SideInputManager::new(
            "test-side-input-manager-store",
            "input-1",
            client,
            cancel.clone(),
        );

        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-side-input-manager-store";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let manager_handle = tokio::spawn(async move {
            manager
                .run(
                    ClientConfig {
                        url: "localhost:4222".to_string(),
                        ..Default::default()
                    },
                    side_input_trigger,
                )
                .await
                .unwrap();
        });

        // sleep for 100ms and make sure the side-input is generated
        tokio::time::sleep(Duration::from_millis(100)).await;

        let entries = kv_store.get("input-1").await.unwrap();
        assert!(entries.is_some());
        let entries = entries.unwrap();
        assert_eq!(entries, Bytes::from("test"));

        cancel.cancel();

        manager_handle.await.unwrap();

        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        Ok(())
    }
}
