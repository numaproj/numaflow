//! Output rendering: text (default), JSON lines, and raw payload bytes; plus the running
//! tally that drives the summary line and the process exit code.

use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::cli::{OutputFormat, OutputOpts};
use crate::exit::{CliError, CliResult, ExitCode};

/// The backslash-prefixed sentinel tags used for conditional forwarding.
pub const DROP_TAG: &str = "\\__DROP__";
pub const NACK_TAG: &str = "\\__NACK__";
const ENCODED_DROP_TAG: &str = "U+005C__DROP__";
const ENCODED_NACK_TAG: &str = "U+005C__NACK__";

/// A single response result emitted by a UDF, normalized across all types.
#[derive(Debug, Clone, Default)]
pub struct ResultRecord {
    pub keys: Vec<String>,
    pub tags: Vec<String>,
    pub value: Vec<u8>,
    /// Present for transformer / accumulator results that carry an (re-assigned) event time.
    pub event_time: Option<DateTime<Utc>>,
    /// Present for accumulator (echoed id).
    pub id: Option<String>,
    /// Optional nack delay in ms (from NackOptions), for rendering NACKED(delay=…).
    pub nack_delay_ms: Option<u64>,
    /// Optional nack reason.
    pub nack_reason: Option<String>,
}

impl ResultRecord {
    pub fn is_drop(&self) -> bool {
        self.tags
            .iter()
            .any(|t| t == DROP_TAG || t == ENCODED_DROP_TAG)
    }
    pub fn is_nack(&self) -> bool {
        self.tags
            .iter()
            .any(|t| t == NACK_TAG || t == ENCODED_NACK_TAG)
    }
}

/// Running tally for the summary line and exit code.
#[derive(Debug, Default)]
pub struct Tally {
    pub sent: usize,
    pub results: usize,
    pub dropped: usize,
    pub nacked: usize,
    /// UDF-reported failures (sink FAILURE, etc.) → drives exit code 4.
    pub failed: usize,
}

/// The reporter owns the output format and the tally; drivers push events into it.
pub struct Reporter {
    pub format: OutputFormat,
    pub verbose: bool,
    pub quiet: bool,
    pub tally: Tally,
}

impl Reporter {
    pub fn new(opts: &OutputOpts) -> Self {
        Reporter {
            format: opts.output,
            verbose: opts.verbose,
            quiet: opts.quiet,
            tally: Tally::default(),
        }
    }

    /// A wire-level trace line, only shown with `-v`.
    pub fn trace(&self, msg: impl AsRef<str>) {
        if self.verbose && self.format == OutputFormat::Text {
            eprintln!("· {}", msg.as_ref());
        }
    }

    /// A top-level status line (e.g. "✓ ready … handshake ok"), suppressed in quiet/json/raw.
    pub fn status(&self, msg: impl AsRef<str>) {
        if !self.quiet && self.format == OutputFormat::Text {
            println!("{}", msg.as_ref());
        }
    }

    /// Print a group header (e.g. `[msg-1] 2 results (3.1ms)` or a window header).
    pub fn group_header(&self, msg: impl AsRef<str>) {
        if !self.quiet && self.format == OutputFormat::Text {
            println!("{}", msg.as_ref());
        }
    }

    /// Emit a single result. `context` carries the correlation info (id/window) for JSON.
    pub fn result(&mut self, r: &ResultRecord, index: usize, context: &ResultContext) {
        if r.is_drop() {
            self.tally.dropped += 1;
        } else if r.is_nack() {
            self.tally.nacked += 1;
        } else {
            self.tally.results += 1;
        }

        match self.format {
            OutputFormat::Text => self.result_text(r, index),
            OutputFormat::Json => self.result_json(r, context),
            OutputFormat::Raw => self.result_raw(r),
        }
    }

    fn result_text(&self, r: &ResultRecord, index: usize) {
        if self.quiet {
            return;
        }
        let mut line = format!(
            "  {index}: keys={} tags={}",
            fmt_list(&r.keys),
            fmt_list(&r.tags)
        );
        if let Some(et) = r.event_time {
            line.push_str(&format!(" eventTime={}", et.to_rfc3339()));
        }
        if let Some(id) = &r.id {
            line.push_str(&format!(" id={id}"));
        }
        if r.is_drop() {
            line.push_str(" DROPPED");
        } else if r.is_nack() {
            let delay = r
                .nack_delay_ms
                .map(|d| format!("delay={d}ms"))
                .unwrap_or_default();
            let reason = r.nack_reason.as_deref().unwrap_or("");
            line.push_str(&format!(
                " NACKED({reason}{}{delay})",
                if reason.is_empty() { "" } else { ", " }
            ));
        } else {
            line.push_str(&format!(" payload={}", render_payload(&r.value)));
        }
        println!("{line}");
    }

    #[allow(clippy::unused_self)]
    fn result_json(&self, r: &ResultRecord, context: &ResultContext) {
        let mut obj = json!({
            "type": "result",
            "keys": r.keys,
            "tags": r.tags,
            "payloadBase64": BASE64.encode(&r.value),
        });
        let map = obj.as_object_mut().expect("object");
        if let Some(id) = &context.id {
            map.insert("id".to_string(), json!(id));
        }
        if let Some(w) = &context.window {
            map.insert("window".to_string(), json!(w));
        }
        if let Some(et) = r.event_time {
            map.insert("eventTime".to_string(), json!(et.to_rfc3339()));
        }
        if let Some(id) = &r.id {
            map.insert("resultId".to_string(), json!(id));
        }
        if r.is_drop() {
            map.insert("dropped".to_string(), json!(true));
        }
        if r.is_nack() {
            map.insert("nacked".to_string(), json!(true));
            if let Some(d) = r.nack_delay_ms {
                map.insert("nackDelayMs".to_string(), json!(d));
            }
        }
        // Also include UTF-8 payload if valid, for readability.
        if let Ok(s) = std::str::from_utf8(&r.value) {
            map.insert("payload".to_string(), json!(s));
        }
        print_json(&obj);
    }

    #[allow(clippy::unused_self)]
    fn result_raw(&self, r: &ResultRecord) {
        if r.is_drop() || r.is_nack() {
            return;
        }
        let mut stdout = std::io::stdout().lock();
        let _ = stdout.write_all(&r.value);
    }

    /// Emit a generic JSON event (for source messages, sink statuses, etc.).
    pub fn json_event(&self, obj: &Value) {
        if self.format == OutputFormat::Json {
            print_json(obj);
        }
    }

    /// Print the final summary line (text mode) or a summary JSON object (json mode).
    pub fn summary(&self, extra: &[(&str, String)]) {
        match self.format {
            OutputFormat::Text => {
                // In quiet mode this summary line is the only text output.
                let mut parts = vec![
                    format!("sent={}", self.tally.sent),
                    format!("results={}", self.tally.results),
                ];
                if self.tally.dropped > 0 {
                    parts.push(format!("dropped={}", self.tally.dropped));
                }
                if self.tally.nacked > 0 {
                    parts.push(format!("nacked={}", self.tally.nacked));
                }
                parts.push(format!("failed={}", self.tally.failed));
                for (k, v) in extra {
                    parts.push(format!("{k}={v}"));
                }
                if !self.quiet {
                    println!("──");
                }
                println!("{}", parts.join(" · "));
            }
            OutputFormat::Json => {
                let mut obj = json!({
                    "type": "summary",
                    "sent": self.tally.sent,
                    "results": self.tally.results,
                    "dropped": self.tally.dropped,
                    "nacked": self.tally.nacked,
                    "failed": self.tally.failed,
                });
                let map = obj.as_object_mut().expect("object");
                for (k, v) in extra {
                    map.insert((*k).to_string(), json!(v));
                }
                print_json(&obj);
            }
            OutputFormat::Raw => {}
        }
    }

    pub fn exit_result(&self) -> CliResult<()> {
        let mut parts = Vec::new();
        if self.tally.failed > 0 {
            parts.push(format!("{} failure(s)", self.tally.failed));
        }
        if self.tally.nacked > 0 {
            parts.push(format!("{} nack(s)", self.tally.nacked));
        }
        if parts.is_empty() {
            return Ok(());
        }
        Err(CliError::new(
            ExitCode::UdfFailure,
            anyhow::anyhow!("UDF reported {}", parts.join(" and ")),
        ))
    }
}

/// Correlation info attached to a result for JSON output.
#[derive(Debug, Default)]
pub struct ResultContext {
    pub id: Option<String>,
    pub window: Option<String>,
}

impl ResultContext {
    pub fn id(id: impl Into<String>) -> Self {
        ResultContext {
            id: Some(id.into()),
            window: None,
        }
    }
    pub fn window(w: impl Into<String>) -> Self {
        ResultContext {
            id: None,
            window: Some(w.into()),
        }
    }
}

/// Render a payload as UTF-8 if valid, else base64 with a marker.
pub fn render_payload(value: &[u8]) -> String {
    match std::str::from_utf8(value) {
        Ok(s) => s.to_string(),
        Err(_) => format!("{} (base64)", BASE64.encode(value)),
    }
}

fn fmt_list(items: &[String]) -> String {
    format!("[{}]", items.join(", "))
}

fn print_json(obj: &Value) {
    println!("{obj}");
}
