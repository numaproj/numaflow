use std::env;
use std::sync::OnceLock;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};

const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";

pub fn pipeline_spec() -> &'static Pipeline {
    static PIPELINE: OnceLock<Pipeline> = OnceLock::new();
    PIPELINE.get_or_init(|| match Pipeline::load() {
        Ok(pipeline) => pipeline,
        Err(e) => panic!("Failed to load pipeline: {:?}", e),
    })
}

// OperatorType is an enum that contains the types of operators
// that can be used in the conditions for the edge.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum OperatorType {
    #[serde(rename = "and")]
    And,
    #[serde(rename = "or")]
    Or,
    #[serde(rename = "not")]
    Not,
}

#[allow(dead_code)]
impl OperatorType {
    fn as_str(&self) -> &'static str {
        match self {
            OperatorType::And => "and",
            OperatorType::Or => "or",
            OperatorType::Not => "not",
        }
    }
}

// Tag is a struct that contains the information about the tags for the edge
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tag {
    pub operator: Option<OperatorType>,
    pub values: Vec<String>,
}

// Conditions is a struct that contains the information about the conditions for the edge
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Conditions {
    pub tags: Option<Tag>,
}

// Edge is a struct that contains the information about the edge in the pipeline.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub conditions: Option<Conditions>,
}

// Pipeline is a struct that contains the information about the pipeline.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde()]
pub struct Pipeline {
    pub vertices: Vec<Vertex>,
    pub edges: Vec<Edge>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Vertex {
    pub name: String,
}

impl Pipeline {
    pub fn load() -> crate::Result<Self> {
        let pipeline = match env::var(ENV_MIN_PIPELINE_SPEC) {
            Ok(env_value) => {
                // If the environment variable is set, decode and parse the pipeline
                let decoded = BASE64_STANDARD
                    .decode(env_value.as_bytes())
                    .map_err(|e| format!("decoding pipeline from env: {e:?}"))?;

                serde_json::from_slice::<Pipeline>(&decoded)
                    .map_err(|e| format!("parsing pipeline from env: {e:?}"))?
            }
            Err(_) => {
                // If the environment variable is not set, read the pipeline from a file
                let file_path = "./config/pipeline_spec.json";
                let file_contents = std::fs::read_to_string(file_path)
                    .map_err(|e| format!("reading pipeline file: {e:?}"))?;
                serde_json::from_str::<Pipeline>(&file_contents)
                    .map_err(|e| format!("parsing pipeline file: {e:?}"))?
            }
        };
        Ok(pipeline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_load() {
        let pipeline = pipeline_spec();

        assert_eq!(pipeline.vertices.len(), 3);
        assert_eq!(pipeline.edges.len(), 2);
        assert_eq!(pipeline.vertices[0].name, "in");
        assert_eq!(pipeline.edges[0].from, "in");
        assert_eq!(pipeline.edges[0].to, "cat");
        assert!(pipeline.edges[0].conditions.is_none());

        assert_eq!(pipeline.vertices[1].name, "cat");
        assert_eq!(pipeline.vertices[2].name, "out");
        assert_eq!(pipeline.edges[1].from, "cat");
        assert_eq!(pipeline.edges[1].to, "out");
    }
}
