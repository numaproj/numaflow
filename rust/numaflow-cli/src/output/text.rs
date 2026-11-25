//! Human-readable text output: one line per event plus a summary line.

use base64::Engine;
use numaflow_core::local::OutputEvent;

use crate::output::Rendered;

/// Render an event's payload as UTF-8 if valid, else base64 (prefixed so it's unambiguous).
fn render_payload(ev: &OutputEvent) -> String {
    match std::str::from_utf8(&ev.payload) {
        Ok(s) => s.to_string(),
        Err(_) => format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(&ev.payload)
        ),
    }
}

pub fn render(r: &Rendered) {
    for ev in &r.events {
        let mut line = format!(
            "[{}] keys={:?} eventTime={} payload={}",
            ev.id,
            ev.keys,
            ev.event_time.to_rfc3339(),
            render_payload(ev)
        );
        if let Some(meta) = &ev.metadata_summary {
            line.push_str(&format!(" metadata=[{meta}]"));
        }
        println!("{line}");
    }

    let mut summary = format!(
        "sent={} · results={} · elapsed={:.2?}",
        r.sent,
        r.results(),
        r.elapsed
    );
    // A shortfall between sent and results implies drops (non-flatmap). We print both numbers and
    // an approximate drop count without trying to model flatmap fan-out (design §9.1).
    if r.results() < r.sent {
        summary.push_str(&format!(" · dropped≈{}", r.sent - r.results()));
    }
    if r.stuck > 0 {
        summary.push_str(&format!(" · stuck={}", r.stuck));
    }
    eprintln!("{summary}");
}
