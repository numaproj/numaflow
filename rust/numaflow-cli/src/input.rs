//! Parsing test messages: the YAML multi-document file format and the inline single-message
//! flags. Produces a `Vec<Message>` of fully-resolved messages in file order.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::cli::InputOpts;
use crate::message::{Message, parse_time};

/// One document in the YAML message stream. Unknown fields are a hard error (typo-catching).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RawMessage {
    payload: Option<String>,
    payload_base64: Option<String>,
    payload_file: Option<String>,
    #[serde(default)]
    keys: Vec<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    event_time: Option<String>,
    watermark: Option<String>,
    id: Option<String>,
    #[serde(default)]
    user_metadata: HashMap<String, HashMap<String, String>>,
    previous_vertex: Option<String>,
    repeat: Option<u32>,
}

/// How the driver wants messages: whether ids are meaningful for this UDF type.
#[derive(Debug, Clone, Copy, Default)]
pub struct LoadOptions {
    /// True for reduce/session-reduce whose payloads carry no id → warn if the file sets `id`.
    pub ignores_id: bool,
}

/// Resolve the base time: explicit `--base-time`, else the provided fallback (CLI now, or a
/// window-truncated now for the reduce family).
pub fn resolve_base_time(
    input: &InputOpts,
    fallback: DateTime<Utc>,
) -> anyhow::Result<DateTime<Utc>> {
    match &input.base_time {
        Some(s) => {
            let parsed = DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("invalid --base-time {s:?} (expected RFC3339)"))?;
            Ok(parsed.with_timezone(&Utc))
        }
        None => Ok(fallback),
    }
}

/// Load messages from either the inline flags or the `-f` file. Exactly one source must be
/// present (enforced here so error messages are clear).
pub fn load_messages(
    input: &InputOpts,
    base_time: DateTime<Utc>,
    opts: LoadOptions,
) -> anyhow::Result<Vec<Message>> {
    let has_inline =
        input.payload.is_some() || input.payload_file.is_some() || input.payload_base64.is_some();

    match (&input.file, has_inline) {
        (Some(_), true) => bail!(
            "-f/--file provides all message fields; inline payload flags and --key/--header/--event-time/--watermark/--id do not apply (put them in the YAML documents)"
        ),
        (None, false) => {
            bail!(
                "a payload is required: pass -f <file> or one of --payload/--payload-file/--payload-base64"
            )
        }
        (Some(file), false) => {
            if has_file_mode_inline_fields(input) {
                bail!(
                    "-f/--file provides all message fields; --key/--header/--event-time/--watermark/--id do not apply (put them in the YAML documents)"
                );
            }
            load_from_file(file, base_time, opts)
        }
        (None, true) => Ok(vec![load_inline(input, base_time)?]),
    }
}

fn has_file_mode_inline_fields(input: &InputOpts) -> bool {
    !input.keys.is_empty()
        || !input.headers.is_empty()
        || input.event_time.is_some()
        || input.watermark.is_some()
        || input.id.is_some()
}

/// Build the single inline message from flags.
fn load_inline(input: &InputOpts, base_time: DateTime<Utc>) -> anyhow::Result<Message> {
    let value = if let Some(p) = &input.payload {
        p.clone().into_bytes()
    } else if let Some(path) = &input.payload_file {
        std::fs::read(path).with_context(|| format!("reading --payload-file {path:?}"))?
    } else if let Some(b64) = &input.payload_base64 {
        BASE64
            .decode(b64.trim())
            .context("decoding --payload-base64")?
    } else {
        // load_messages already guarantees a source exists.
        unreachable!("inline payload source missing")
    };

    let headers = parse_inline_headers(&input.headers)?;

    let event_time = match &input.event_time {
        Some(s) => parse_time(s, base_time)?,
        None => base_time,
    };
    let watermark = match &input.watermark {
        Some(s) => parse_time(s, base_time)?,
        None => event_time,
    };

    Ok(Message {
        id: input.id.clone().unwrap_or_else(|| "msg-1".to_string()),
        keys: input.keys.clone(),
        value,
        event_time,
        watermark,
        headers,
        user_metadata: HashMap::new(),
        previous_vertex: String::new(),
    })
}

/// Parse repeated `--header k=v` flags into a map.
fn parse_inline_headers(raw: &[String]) -> anyhow::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for h in raw {
        let (k, v) = h
            .split_once('=')
            .ok_or_else(|| anyhow!("--header must be k=v, got {h:?}"))?;
        map.insert(k.to_string(), v.to_string());
    }
    Ok(map)
}

/// Load and expand messages from a YAML multi-doc file (or stdin when path is `-`).
fn load_from_file(
    file: &str,
    base_time: DateTime<Utc>,
    opts: LoadOptions,
) -> anyhow::Result<Vec<Message>> {
    let (raw_text, base_dir) = read_file_source(file)?;

    let mut out = Vec::new();
    let mut id_doc_numbers = Vec::new();
    let mut warned_id = false;
    // 1-based sequence across the *expanded* stream, for auto ids.
    let mut seq = 0usize;

    for (doc_idx, doc) in serde_yaml_ng::Deserializer::from_str(&raw_text).enumerate() {
        let raw = RawMessage::deserialize(doc)
            .with_context(|| format!("parsing message document #{}", doc_idx + 1))?;

        if raw.id.is_some() && opts.ignores_id && !warned_id {
            eprintln!("warning: `id` fields are ignored for this UDF type (payloads carry no id)");
            warned_id = true;
        }

        let value = resolve_payload(&raw, base_dir.as_deref(), doc_idx)?;
        let headers = raw.headers.clone();

        let event_time = match &raw.event_time {
            Some(s) => parse_time(s, base_time)
                .with_context(|| format!("in message document #{}", doc_idx + 1))?,
            None => base_time,
        };
        let watermark = match &raw.watermark {
            Some(s) => parse_time(s, base_time)
                .with_context(|| format!("in message document #{}", doc_idx + 1))?,
            None => event_time,
        };

        let repeat = raw.repeat.unwrap_or(1);
        if repeat < 1 {
            bail!("`repeat` must be >= 1 in message document #{}", doc_idx + 1);
        }
        if raw.id.is_some() && repeat > 1 {
            eprintln!(
                "warning: message document #{}: explicit `id` ignored because repeat={} (auto ids used)",
                doc_idx + 1,
                repeat
            );
        }

        for _ in 0..repeat {
            seq += 1;
            // Explicit id honored only for the first copy of a non-repeated message; repeated
            // copies always get auto ids so they stay unique.
            let id = match (&raw.id, repeat) {
                (Some(id), 1) => id.clone(),
                _ => format!("msg-{seq}"),
            };
            out.push(Message {
                id,
                keys: raw.keys.clone(),
                value: value.clone(),
                event_time,
                watermark,
                headers: headers.clone(),
                user_metadata: raw.user_metadata.clone(),
                previous_vertex: raw.previous_vertex.clone().unwrap_or_default(),
            });
            id_doc_numbers.push(doc_idx + 1);
        }
    }

    if out.is_empty() {
        bail!("no messages found in {file}");
    }
    if !opts.ignores_id {
        let mut seen: HashMap<String, usize> = HashMap::new();
        for (msg, doc_no) in out.iter().zip(id_doc_numbers.iter().copied()) {
            if let Some(first) = seen.insert(msg.id.clone(), doc_no) {
                bail!(
                    "duplicate message id {:?} (documents #{} and #{}); ids must be unique for response correlation",
                    msg.id,
                    first,
                    doc_no
                );
            }
        }
    }
    Ok(out)
}

/// Read the message source: a file path or `-` for stdin. Returns text and the directory used
/// to resolve relative `payloadFile` paths (None for stdin).
fn read_file_source(file: &str) -> anyhow::Result<(String, Option<PathBuf>)> {
    if file == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("reading message stream from stdin")?;
        Ok((buf, None))
    } else {
        let text = std::fs::read_to_string(file)
            .with_context(|| format!("reading message file {file:?}"))?;
        let dir = Path::new(file).parent().map(|p| p.to_path_buf());
        Ok((text, dir))
    }
}

/// Resolve exactly one payload source from a document.
fn resolve_payload(
    raw: &RawMessage,
    base_dir: Option<&Path>,
    doc_idx: usize,
) -> anyhow::Result<Vec<u8>> {
    let n = raw.payload.is_some() as u8
        + raw.payload_base64.is_some() as u8
        + raw.payload_file.is_some() as u8;
    if n == 0 {
        bail!(
            "message document #{} has no payload (need one of payload/payloadBase64/payloadFile)",
            doc_idx + 1
        );
    }
    if n > 1 {
        bail!(
            "message document #{} has multiple payload fields; provide exactly one",
            doc_idx + 1
        );
    }

    if let Some(p) = &raw.payload {
        Ok(p.clone().into_bytes())
    } else if let Some(b64) = &raw.payload_base64 {
        BASE64.decode(b64.trim()).with_context(|| {
            format!(
                "decoding payloadBase64 in message document #{}",
                doc_idx + 1
            )
        })
    } else if let Some(rel) = &raw.payload_file {
        let path = match base_dir {
            Some(dir) => dir.join(rel),
            None => PathBuf::from(rel),
        };
        std::fs::read(&path).with_context(|| {
            format!(
                "reading payloadFile {path:?} in message document #{}",
                doc_idx + 1
            )
        })
    } else {
        unreachable!()
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn base() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 6, 0, 0, 0).unwrap()
    }

    fn opts_with_file(text: &str) -> (InputOpts, tempdir::TempFile) {
        let tf = tempdir::TempFile::new(text);
        let input = InputOpts {
            file: Some(tf.path.to_string_lossy().to_string()),
            payload: None,
            payload_file: None,
            payload_base64: None,
            keys: vec![],
            headers: vec![],
            event_time: None,
            watermark: None,
            id: None,
            base_time: None,
        };
        (input, tf)
    }

    #[test]
    fn parses_multi_doc_with_repeat() {
        let yaml = r#"
payload: hello
keys: [alice]
eventTime: "+1s"
---
payload: world
keys: [bob]
eventTime: "+20s"
repeat: 2
"#;
        let (input, _tf) = opts_with_file(yaml);
        let msgs = load_messages(&input, base(), LoadOptions::default()).unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].id, "msg-1");
        assert_eq!(msgs[0].value, b"hello");
        assert_eq!(msgs[0].keys, vec!["alice".to_string()]);
        assert_eq!(msgs[0].event_time, base() + chrono::Duration::seconds(1));
        // repeated copies get auto ids
        assert_eq!(msgs[1].id, "msg-2");
        assert_eq!(msgs[2].id, "msg-3");
        assert_eq!(msgs[1].value, b"world");
        assert_eq!(msgs[2].value, b"world");
        // watermark defaults to event time
        assert_eq!(msgs[1].watermark, msgs[1].event_time);
    }

    #[test]
    fn multiline_block_scalar_preserved() {
        let yaml = "payload: |-\n  line1\n  line2\n";
        let (input, _tf) = opts_with_file(yaml);
        let msgs = load_messages(&input, base(), LoadOptions::default()).unwrap();
        assert_eq!(msgs[0].value, b"line1\nline2");
    }

    #[test]
    fn base64_payload_decoded() {
        // 0xDEADBEEF
        let yaml = "payloadBase64: \"3q2+7w==\"\n";
        let (input, _tf) = opts_with_file(yaml);
        let msgs = load_messages(&input, base(), LoadOptions::default()).unwrap();
        assert_eq!(msgs[0].value, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn unknown_field_is_error() {
        let yaml = "payload: hi\nbogusField: 1\n";
        let (input, _tf) = opts_with_file(yaml);
        let err = load_messages(&input, base(), LoadOptions::default()).unwrap_err();
        assert!(
            format!("{err:#}").contains("bogusField")
                || format!("{err:#}").contains("unknown field")
        );
    }

    #[test]
    fn missing_payload_is_error() {
        let yaml = "keys: [x]\n";
        let (input, _tf) = opts_with_file(yaml);
        let err = load_messages(&input, base(), LoadOptions::default()).unwrap_err();
        assert!(format!("{err:#}").contains("no payload"));
    }

    #[test]
    fn multiple_payloads_is_error() {
        let yaml = "payload: a\npayloadBase64: YQ==\n";
        let (input, _tf) = opts_with_file(yaml);
        let err = load_messages(&input, base(), LoadOptions::default()).unwrap_err();
        assert!(format!("{err:#}").contains("multiple payload"));
    }

    #[test]
    fn file_rejects_inline_message_fields() {
        let yaml = "payload: hi\n";
        let (mut input, _tf) = opts_with_file(yaml);
        input.keys = vec!["k".to_string()];
        let err = load_messages(&input, base(), LoadOptions::default()).unwrap_err();
        assert!(format!("{err:#}").contains("--key"));
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let yaml = "payload: a\nid: same\n---\npayload: b\nid: same\n";
        let (input, _tf) = opts_with_file(yaml);
        let err = load_messages(&input, base(), LoadOptions::default()).unwrap_err();
        assert!(format!("{err:#}").contains("same"));
        assert!(format!("{err:#}").contains("duplicate"));
    }

    #[test]
    fn duplicate_ids_are_allowed_when_ids_are_ignored() {
        let yaml = "payload: a\nid: same\n---\npayload: b\nid: same\n";
        let (input, _tf) = opts_with_file(yaml);
        let msgs = load_messages(&input, base(), LoadOptions { ignores_id: true }).unwrap();
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn inline_message_defaults() {
        let input = InputOpts {
            file: None,
            payload: Some("hi".to_string()),
            payload_file: None,
            payload_base64: None,
            keys: vec!["k1".to_string()],
            headers: vec!["a=b".to_string()],
            event_time: None,
            watermark: None,
            id: None,
            base_time: None,
        };
        let msgs = load_messages(&input, base(), LoadOptions::default()).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "msg-1");
        assert_eq!(msgs[0].value, b"hi");
        assert_eq!(msgs[0].event_time, base());
        assert_eq!(msgs[0].headers.get("a"), Some(&"b".to_string()));
    }

    #[test]
    fn user_metadata_and_previous_vertex() {
        let yaml = r#"
payload: x
userMetadata:
  grp:
    k: v
previousVertex: prev-vtx
"#;
        let (input, _tf) = opts_with_file(yaml);
        let msgs = load_messages(&input, base(), LoadOptions::default()).unwrap();
        let md = msgs[0].metadata().unwrap();
        assert_eq!(md.previous_vertex, "prev-vtx");
        assert_eq!(
            md.user_metadata.get("grp").unwrap().key_value.get("k"),
            Some(&b"v".to_vec())
        );
    }

    // Minimal temp-file helper so tests don't need an external crate.
    mod tempdir {
        use std::path::PathBuf;

        pub struct TempFile {
            pub path: PathBuf,
        }

        impl TempFile {
            pub fn new(contents: &str) -> Self {
                let mut path = std::env::temp_dir();
                // unique-ish name without extra deps
                let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let pid = std::process::id();
                path.push(format!("nfcli-test-{pid}-{n}.yaml"));
                std::fs::write(&path, contents).unwrap();
                TempFile { path }
            }
        }

        impl Drop for TempFile {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    }
}
