const DEFAULT_METRICS_PORT: u16 = 2469;
const DEFAULT_LAG_CHECK_INTERVAL_IN_SECS: u16 = 5;
const DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS: u16 = 3;
const DEFAULT_LOOKBACK_WINDOW_IN_SECS: u16 = 120;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MetricsConfig {
    // TODO(lookback) - using new implementation for monovertex right now,
    // remove extra fields from here once new corresponding pipeline changes
    // in the daemon are done.
    pub metrics_server_listen_port: u16,
    pub lag_check_interval_in_secs: u16,
    pub lag_refresh_interval_in_secs: u16,
    pub lookback_window_in_secs: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            metrics_server_listen_port: DEFAULT_METRICS_PORT,
            lag_check_interval_in_secs: DEFAULT_LAG_CHECK_INTERVAL_IN_SECS,
            lag_refresh_interval_in_secs: DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS,
            lookback_window_in_secs: DEFAULT_LOOKBACK_WINDOW_IN_SECS,
        }
    }
}

impl MetricsConfig {
    pub(crate) fn with_lookback_window_in_secs(lookback_window_in_secs: u16) -> Self {
        MetricsConfig {
            lookback_window_in_secs,
            ..Default::default()
        }
    }
}
