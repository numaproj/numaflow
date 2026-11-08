//! Output rendering: a backend-neutral model plus text/json/raw renderers.

pub mod json;
pub mod raw;
pub mod text;

use std::time::Duration;

use numaflow_core::local::OutputEvent;

use crate::cli::OutputFormat;

/// Everything a subcommand produces, rendered by the chosen format.
pub struct Rendered {
    /// Output events read from the run (empty for terminal sinks).
    pub events: Vec<OutputEvent>,
    /// Number of events fed in.
    pub sent: usize,
    /// Wall-clock elapsed for the run.
    pub elapsed: Duration,
    /// Non-zero only on a drain timeout: how many messages were still stuck.
    pub stuck: usize,
}

impl Rendered {
    pub fn results(&self) -> usize {
        self.events.len()
    }
}

/// Render to stdout in the requested format.
pub fn render(rendered: &Rendered, format: OutputFormat) {
    match format {
        OutputFormat::Text => text::render(rendered),
        OutputFormat::Json => json::render(rendered),
        OutputFormat::Raw => raw::render(rendered),
    }
}
