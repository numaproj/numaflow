//! JSONL output: one JSON object per event, then a summary object.

use base64::Engine;
use serde_json::json;

use crate::output::Rendered;

pub fn render(r: &Rendered) {
    for ev in &r.events {
        let obj = json!({
            "type": "result",
            "id": ev.id,
            "keys": ev.keys,
            "eventTime": ev.event_time.to_rfc3339(),
            "payloadBase64": base64::engine::general_purpose::STANDARD.encode(&ev.payload),
            "headers": ev.headers,
            "metadata": ev.metadata_summary,
        });
        println!("{obj}");
    }

    let summary = json!({
        "type": "summary",
        "sent": r.sent,
        "results": r.results(),
        "elapsedMs": r.elapsed.as_millis() as u64,
        "stuck": r.stuck,
    });
    println!("{summary}");
}
