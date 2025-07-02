//! Runs the user-defined side-input generator at specified intervals (cron expr).

use crate::error::{Error, Result};
use crate::manager::client::UserDefinedSideInputClient;
use crate::{create_js_context, isb};
use async_nats::jetstream;
use bytes::Bytes;
use chrono_tz::{Tz, UTC};
use cron::Schedule;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// gRPC client to interact with the user-defined side-input generator.
mod client;

/// Cron expression for the side-input trigger.
#[derive(Debug, Clone)]
pub(crate) struct SideInputTrigger {
    /// The schedule to trigger the creation of the side input data.
    schedule: Schedule,
    /// Timezone for the schedule, defaults to [UTC].
    timezone: Tz,
}

impl SideInputTrigger {
    /// Creates a new SideInputTrigger from the schedule string and optional timezone string.
    pub fn new(schedule_str: &'static str, timezone_str: Option<&'static str>) -> Result<Self> {
        let schedule =
            Schedule::from_str(schedule_str).map_err(|e| Error::Schedule(e.to_string()))?;

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
    user_defined_side_input_client: UserDefinedSideInputClient,
    cancellation_token: CancellationToken,
}

impl SideInputManager {
    pub(crate) fn new(
        side_input_store: &'static str,
        user_defined_side_input_client: UserDefinedSideInputClient,
        cancellation_token: CancellationToken,
    ) -> Self {
        SideInputManager {
            side_input_store,
            user_defined_side_input_client,
            cancellation_token,
        }
    }

    pub(crate) async fn run(
        mut self,
        js_client_config: isb::ClientConfig,
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

            // sleep until the next run time
            let sleep_util = tokio::time::sleep_until(tokio::time::Instant::from_std(
                Instant::now() + Duration::from_micros(datetime.timestamp_micros() as u64),
            ));

            // check if we have been cancelled
            tokio::select! {
                _ = sleep_util => {
                    info!(?datetime, "running schedule for side-input generation");
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
            .map_err(|e| Error::SideInput(format!("Failed to retrieve side input: {:?}", e)))?;

        if !side_input_response.no_broadcast {
            // store the side-input data in the bucket
            bucket
                .put(
                    self.side_input_store,
                    Bytes::from(side_input_response.value),
                )
                .await
                .map_err(|e| Error::SideInput(format!("Failed to store side input: {:?}", e)))?;
            debug!("Side input stored in the bucket");
        } else {
            info!("Side input is not broadcasted since no_broadcast is set to true");
        }

        Ok(())
    }
}
