//! Raw output: concatenated payload bytes to stdout, diagnostics to stderr.

use std::io::Write;

use crate::output::Rendered;

pub fn render(r: &Rendered) {
    let mut stdout = std::io::stdout().lock();
    for ev in &r.events {
        // Payloads go out verbatim; a trailing newline separates them for readability.
        let _ = stdout.write_all(&ev.payload);
        let _ = stdout.write_all(b"\n");
    }
    let _ = stdout.flush();

    eprintln!(
        "sent={} results={} elapsed={:.2?} stuck={}",
        r.sent,
        r.results(),
        r.elapsed,
        r.stuck
    );
}
