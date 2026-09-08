//! Input event assembly: turn file docs / inline flags into `InputEvent`s.

pub mod time;
pub mod yaml;

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use base64::Engine;
use bytes::Bytes;
use chrono::Utc;
use numaflow_core::local::InputEvent;

use crate::cli::InputArgs;
use crate::error::{CliError, CliResult};
use crate::input::yaml::EventDoc;

/// Whether the current subcommand belongs to the reduce family (so `watermark` is meaningful).
#[derive(Clone, Copy)]
pub struct InputContext {
    pub reduce_family: bool,
    /// Window length/slide/gap used to floor the base time; `None` for non-reduce kinds.
    pub align_to: Option<std::time::Duration>,
}

/// Build the full list of input events for a data subcommand, from either `-f` or inline flags.
/// Applies `repeat` expansion, auto-generates ids, and rejects duplicate ids (G2).
pub fn build_events(args: &InputArgs, ctx: InputContext) -> CliResult<Vec<InputEvent>> {
    let base_time = time::compute_base_time(args.base_time.as_deref(), ctx.align_to, Utc::now())?;

    // Exactly one input source: a file XOR one inline payload flag.
    let has_file = args.file.is_some();
    let has_inline =
        args.payload.is_some() || args.payload_file.is_some() || args.payload_base64.is_some();

    let docs = if has_file {
        if has_inline {
            return Err(CliError::Usage(
                "cannot combine -f/--file with inline --payload* flags".to_string(),
            ));
        }
        let (content, dir) = read_file_arg(args.file.as_deref().expect("file present"))?;
        let docs = yaml::parse_docs(&content)?;
        (docs, dir)
    } else if has_inline {
        (vec![inline_doc(args)?], std::env::current_dir().ok())
    } else {
        return Err(CliError::Usage(
            "no input: provide -f <file> or an inline --payload* flag".to_string(),
        ));
    };
    let (docs, file_dir) = docs;

    let mut events = Vec::new();
    let mut seen_ids: HashSet<String> = HashSet::new();
    let mut auto_index = 0usize;

    for doc in &docs {
        if doc.repeat == 0 {
            return Err(CliError::Usage("repeat must be >= 1".to_string()));
        }
        if !ctx.reduce_family && doc.watermark.is_some() {
            tracing::warn!("watermark is ignored for non-reduce subcommands");
        }

        let payload = decode_payload(doc, file_dir.as_deref())?;
        let event_time = match &doc.event_time {
            Some(s) => time::parse_time(s, base_time)?,
            None => base_time,
        };
        let watermark = match &doc.watermark {
            Some(s) => Some(time::parse_time(s, base_time)?),
            None => None,
        };

        for copy in 0..doc.repeat {
            auto_index += 1;
            // Ids: `<explicit-or-msg-N>`; repeat copies always get a fresh unique suffix so a
            // single explicit id across repeats does not collide (and silently dedup — G2).
            let id = generate_id(doc.id.as_deref(), auto_index, copy, doc.repeat);
            if !seen_ids.insert(id.clone()) {
                return Err(CliError::Usage(format!(
                    "duplicate message id '{id}' — ids must be unique (they are the ISB dedup key)"
                )));
            }

            events.push(InputEvent {
                payload: payload.clone(),
                keys: doc.keys.clone(),
                headers: doc.headers.clone(),
                event_time,
                watermark,
                id,
                user_metadata: metadata_opt(&doc.user_metadata),
                previous_vertex: doc.previous_vertex.clone(),
            });
        }
    }

    Ok(events)
}

/// Turn the inline flags into a single synthetic `EventDoc` so the file and inline paths share the
/// same assembly logic.
fn inline_doc(args: &InputArgs) -> CliResult<EventDoc> {
    let headers = parse_kv_headers(&args.headers)?;
    let user_metadata = HashMap::new();
    Ok(EventDoc {
        payload: args.payload.clone(),
        payload_base64: args.payload_base64.clone(),
        payload_file: args.payload_file.clone(),
        keys: args.keys.clone(),
        headers,
        event_time: args.event_time.clone(),
        watermark: args.watermark.clone(),
        id: args.id.clone(),
        user_metadata,
        previous_vertex: None,
        repeat: 1,
    })
}

/// Parse `K=V` header strings.
fn parse_kv_headers(pairs: &[String]) -> CliResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    for pair in pairs {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| CliError::Usage(format!("invalid header '{pair}', expected K=V")))?;
        map.insert(k.to_string(), v.to_string());
    }
    Ok(map)
}

/// Read the `-f` argument (`-` = stdin), returning content plus the directory relative payload
/// files resolve against.
fn read_file_arg(arg: &str) -> CliResult<(String, Option<PathBuf>)> {
    if arg == "-" {
        let mut content = String::new();
        std::io::stdin()
            .read_to_string(&mut content)
            .map_err(|e| CliError::Usage(format!("failed to read stdin: {e}")))?;
        Ok((content, std::env::current_dir().ok()))
    } else {
        let path = PathBuf::from(arg);
        let content = std::fs::read_to_string(&path)
            .map_err(|e| CliError::Usage(format!("failed to read {}: {e}", path.display())))?;
        let dir = path.parent().map(|p| p.to_path_buf());
        Ok((content, dir))
    }
}

/// Decode a document's payload (exactly one of the three payload fields must be set).
fn decode_payload(doc: &EventDoc, file_dir: Option<&Path>) -> CliResult<Bytes> {
    let set = [
        doc.payload.is_some(),
        doc.payload_base64.is_some(),
        doc.payload_file.is_some(),
    ]
    .iter()
    .filter(|b| **b)
    .count();
    if set != 1 {
        return Err(CliError::Usage(
            "each message must set exactly one of payload / payloadBase64 / payloadFile"
                .to_string(),
        ));
    }

    if let Some(p) = &doc.payload {
        return Ok(Bytes::from(p.clone().into_bytes()));
    }
    if let Some(b64) = &doc.payload_base64 {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .map_err(|e| CliError::Usage(format!("invalid base64 payload: {e}")))?;
        return Ok(Bytes::from(bytes));
    }
    let rel = doc.payload_file.as_ref().expect("payload_file present");
    // Resolve relative to the message file's directory.
    let path = if rel.is_absolute() {
        rel.clone()
    } else {
        file_dir.map(|d| d.join(rel)).unwrap_or_else(|| rel.clone())
    };
    let bytes = std::fs::read(&path).map_err(|e| {
        CliError::Usage(format!(
            "failed to read payloadFile {}: {e}",
            path.display()
        ))
    })?;
    Ok(Bytes::from(bytes))
}

/// Generate a unique id for one (possibly repeated) event.
fn generate_id(explicit: Option<&str>, auto_index: usize, copy: usize, repeat: usize) -> String {
    match explicit {
        // A single (unrepeated) explicit id is used verbatim.
        Some(id) if repeat == 1 => id.to_string(),
        // Repeated explicit ids get a `-<k>` suffix so each copy is unique.
        Some(id) => format!("{id}-{copy}"),
        None => format!("msg-{auto_index}"),
    }
}

/// `HashMap` → `Option`, treating empty metadata as absent.
fn metadata_opt(
    m: &HashMap<String, HashMap<String, String>>,
) -> Option<HashMap<String, HashMap<String, String>>> {
    if m.is_empty() { None } else { Some(m.clone()) }
}
