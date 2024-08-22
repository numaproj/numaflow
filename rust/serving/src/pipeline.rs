use std::env;
use std::sync::OnceLock;

use crate::Error::ParseConfig;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use numaflow_models::models::PipelineSpec;
use serde::{Deserialize, Serialize};

const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";

pub fn min_pipeline_spec() -> &'static MinPipelineSpec {
    static PIPELINE: OnceLock<MinPipelineSpec> = OnceLock::new();
    PIPELINE.get_or_init(|| match MinPipelineSpec::load() {
        Ok(pipeline) => pipeline,
        Err(e) => panic!("Failed to load minimum pipeline spec: {:?}", e),
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

impl From<String> for OperatorType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "and" => OperatorType::And,
            "or" => OperatorType::Or,
            "not" => OperatorType::Not,
            _ => panic!("Invalid operator type: {}", s),
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

// MinPipelineSpec is a struct that contains the minimum information about the pipeline
// that is required to construct the dag.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde()]
pub struct MinPipelineSpec {
    pub vertices: Vec<Vertex>,
    pub edges: Vec<Edge>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Vertex {
    pub name: String,
}

impl MinPipelineSpec {
    pub fn load() -> crate::Result<Self> {
        let full_pipeline_spec = match env::var(ENV_MIN_PIPELINE_SPEC) {
            Ok(env_value) => {
                // If the environment variable is set, decode and parse the pipeline
                let decoded = BASE64_STANDARD
                    .decode(env_value.as_bytes())
                    .map_err(|e| ParseConfig(format!("decoding pipeline from env: {e:?}")))?;

                serde_json::from_slice::<PipelineSpec>(&decoded)
                    .map_err(|e| ParseConfig(format!("parsing pipeline from env: {e:?}")))?
            }
            Err(_) => {
                // If the environment variable is not set, read the pipeline from a file
                let file_path = "./config/pipeline_spec.json";
                let file_contents = std::fs::read_to_string(file_path)
                    .map_err(|e| ParseConfig(format!("reading pipeline file: {e:?}")))?;
                serde_json::from_str::<PipelineSpec>(&file_contents)
                    .map_err(|e| ParseConfig(format!("parsing pipeline file: {e:?}")))?
            }
        };

        let vertices: Vec<Vertex> = full_pipeline_spec
            .vertices
            .ok_or(ParseConfig("missing vertices in pipeline spec".to_string()))?
            .iter()
            .map(|v| Vertex {
                name: v.name.clone(),
            })
            .collect();

        let edges: Vec<Edge> = full_pipeline_spec
            .edges
            .ok_or(ParseConfig("missing edges in pipeline spec".to_string()))?
            .iter()
            .map(|e| {
                let conditions = e.conditions.clone().map(|c| Conditions {
                    tags: Some(Tag {
                        operator: c.tags.operator.map(|o| o.into()),
                        values: c.tags.values.clone(),
                    }),
                });

                Edge {
                    from: e.from.clone(),
                    to: e.to.clone(),
                    conditions,
                }
            })
            .collect();

        Ok(MinPipelineSpec { vertices, edges })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_load() {
        let pipeline = min_pipeline_spec();
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
