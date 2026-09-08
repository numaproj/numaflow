//! Strict multi-document YAML message-file schema and parsing.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{CliError, CliResult};

fn default_repeat() -> usize {
    1
}

/// One message document. `deny_unknown_fields` makes typos a hard error rather than a silent
/// no-op.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventDoc {
    pub payload: Option<String>,
    pub payload_base64: Option<String>,
    /// Path (relative to the message file's directory) whose bytes are the payload.
    pub payload_file: Option<PathBuf>,
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// RFC3339 or `+dur`.
    pub event_time: Option<String>,
    /// RFC3339 or `+dur`; reduce-family only (a warning is emitted elsewhere).
    pub watermark: Option<String>,
    pub id: Option<String>,
    /// `group -> key -> value`.
    #[serde(default)]
    pub user_metadata: HashMap<String, HashMap<String, String>>,
    pub previous_vertex: Option<String>,
    /// How many copies of this document to emit (each gets a fresh unique id).
    #[serde(default = "default_repeat")]
    pub repeat: usize,
}

/// Parse a multi-document YAML string into its docs, tagging parse errors with the 1-based
/// document index.
pub fn parse_docs(content: &str) -> CliResult<Vec<EventDoc>> {
    let mut docs = Vec::new();
    for (idx, de) in serde_yaml::Deserializer::from_str(content).enumerate() {
        let doc = EventDoc::deserialize(de)
            .map_err(|e| CliError::Usage(format!("error in message document #{}: {e}", idx + 1)))?;
        docs.push(doc);
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multi_doc() {
        let yaml = "payload: hello\nkeys: [a]\n---\npayload: world\nrepeat: 2\n";
        let docs = parse_docs(yaml).unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs.first().unwrap().payload.as_deref(), Some("hello"));
        assert_eq!(docs.get(1).unwrap().repeat, 2);
    }

    #[test]
    fn rejects_unknown_field() {
        let yaml = "payload: hello\nbogus: 1\n";
        assert!(parse_docs(yaml).is_err());
    }
}
