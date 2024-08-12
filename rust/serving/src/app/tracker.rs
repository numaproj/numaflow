use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::app::callback::CallbackRequest;
use crate::pipeline::{Edge, OperatorType, Pipeline};
use crate::Error;

fn compare_slice(operator: &OperatorType, a: &[String], b: &[String]) -> bool {
    match operator {
        OperatorType::And => a.iter().all(|val| b.contains(val)),
        OperatorType::Or => a.iter().any(|val| b.contains(val)),
        OperatorType::Not => !a.iter().any(|val| b.contains(val)),
    }
}

type Graph = HashMap<String, Vec<Edge>>;

#[derive(Serialize, Deserialize, Debug)]
struct Subgraph {
    id: String,
    blocks: Vec<Block>,
}

const DROP: &str = "U+005C__DROP__";

/// MessageGraph is a struct that generates the graph from the source vertex to the downstream vertices
/// for a message using the given callbacks.
pub(crate) struct MessageGraph {
    dag: Graph,
}

/// Block is a struct that contains the information about the block in the subgraph.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Block {
    from: String,
    to: String,
    cb_time: u64,
}

// CallbackRequestWrapper is a struct that contains the information about the callback request and
// whether it has been visited or not. It is used to keep track of the visited callbacks.
#[derive(Debug)]
struct CallbackRequestWrapper {
    callback_request: Arc<CallbackRequest>,
    visited: bool,
}

impl MessageGraph {
    /// This function generates a sub graph from a list of callbacks.
    /// It first creates a HashMap to map each vertex to its corresponding callbacks.
    /// Then it finds the source vertex by checking if the vertex and from_vertex fields are the same.
    /// Finally, it calls the `generate_subgraph` function to generate the subgraph from the source vertex.
    pub(crate) fn generate_subgraph_from_callbacks(
        &self,
        id: String,
        callbacks: Vec<Arc<CallbackRequest>>,
    ) -> Result<Option<String>, Error> {
        // Create a HashMap to map each vertex to its corresponding callbacks
        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();
        let mut source_vertex = None;

        // Find the source vertex, source vertex is the vertex that has the same vertex and from_vertex
        for callback in callbacks {
            // Check if the vertex is present in the graph
            if !self.dag.contains_key(&callback.from_vertex) {
                return Err(Error::SubGraphInvalidInput(format!(
                    "Invalid callback: {}, vertex: {}",
                    callback.id, callback.from_vertex
                )));
            }

            if callback.vertex == callback.from_vertex {
                source_vertex = Some(callback.vertex.clone());
            }
            callback_map
                .entry(callback.vertex.clone())
                .or_default()
                .push(CallbackRequestWrapper {
                    callback_request: callback.clone(),
                    visited: false,
                });
        }

        // If there is no source vertex, return None
        let source_vertex = match source_vertex {
            Some(vertex) => vertex,
            None => return Ok(None),
        };

        // Create a new subgraph.
        let mut subgraph: Subgraph = Subgraph {
            id,
            blocks: Vec::new(),
        };
        // Call the `generate_subgraph` function to generate the subgraph from the source vertex
        let result = self.generate_subgraph(
            source_vertex.clone(),
            source_vertex,
            &mut callback_map,
            &mut subgraph,
        );

        // If the subgraph is generated successfully, serialize it into a JSON string and return it.
        // Otherwise, return None
        if result {
            match serde_json::to_string(&subgraph) {
                Ok(json) => Ok(Some(json)),
                Err(e) => Err(Error::SubGraphGeneratorError(e.to_string())),
            }
        } else {
            Ok(None)
        }
    }

    // generate_subgraph function is a recursive function that generates the sub graph from the source vertex for
    // the given list of callbacks. The function returns true if the subgraph is generated successfully(if we are
    // able to find a subgraph for the message using the given callbacks), it
    // updates the subgraph with the path from the source vertex to the downstream vertices.
    fn generate_subgraph(
        &self,
        current: String,
        from: String,
        callback_map: &mut HashMap<String, Vec<CallbackRequestWrapper>>,
        subgraph: &mut Subgraph,
    ) -> bool {
        let mut current_callback: Option<Arc<CallbackRequest>> = None;

        // we need to borrow the callback_map as mutable to update the visited flag of the callback
        // so that next time when we visit the same callback, we can skip it. Because there can be cases
        // where there will be multiple callbacks for the same vertex.
        // If there are no callbacks for the current vertex, we should not continue
        // because it means we have not received all the callbacks for the given message.
        let Some(callbacks) = callback_map.get_mut(&current) else {
            return false;
        };

        // iterate over the callbacks for the current vertex and find the one that has not been visited,
        // and it is coming from the same vertex as the current vertex
        for callback in callbacks {
            if callback.callback_request.from_vertex == from && !callback.visited {
                callback.visited = true;
                current_callback = Some(callback.callback_request.clone());
                break;
            }
        }

        // If there is no callback which is not visited and its parent is the "from" vertex, then we should
        // return false because we have not received all the callbacks for the given message.
        let Some(current_callback) = current_callback else {
            return false;
        };

        // add the current block to the subgraph
        subgraph.blocks.push(Block {
            from: current_callback.from_vertex.clone(),
            to: current_callback.vertex.clone(),
            cb_time: current_callback.cb_time,
        });

        // if the current vertex has a DROP tag, then we should not proceed further
        // and return true
        if current_callback
            .tags
            .as_ref()
            .map_or(false, |tags| tags.contains(&DROP.to_string()))
        {
            return true;
        }

        // recursively invoke the downstream vertices of the current vertex, if any
        if let Some(edges) = self.dag.get(&current) {
            for edge in edges {
                // check if the edge should proceed based on the conditions
                // if there are no conditions, we should proceed with the edge
                // if there are conditions, we should check the tags of the current callback
                // with the tags of the edge and the operator of the tags to decide if we should
                // proceed with the edge
                let should_proceed = edge
                    .conditions
                    .as_ref()
                    // If the edge has conditions, get the tags
                    .and_then(|conditions| conditions.tags.as_ref())
                    // If there are no conditions or tags, default to true (i.e., proceed with the edge)
                    // If there are tags, compare the tags with the current callback's tags and the operator
                    // to decide if we should proceed with the edge.
                    .map_or(true, |tags| {
                        current_callback
                            .tags
                            .as_ref()
                            // If the current callback has no tags we should not proceed with the edge for "and" and "or" operators
                            // because we expect the current callback to have tags specified in the edge.
                            // If the current callback has no tags we should proceed with the edge for "not" operator.
                            // because we don't expect the current callback to have tags specified in the edge.
                            .map_or(
                                tags.operator.as_ref() == Some(&OperatorType::Not),
                                |callback_tags| {
                                    tags.operator.as_ref().map_or(false, |operator| {
                                        // If there is no operator, default to false (i.e., do not proceed with the edge)
                                        // If there is an operator, compare the current callback's tags with the edge's tags
                                        compare_slice(operator, callback_tags, &tags.values)
                                    })
                                },
                            )
                    });

                // if the conditions are not met, then proceed to the next edge
                if !should_proceed {
                    continue;
                }

                // proceed to the downstream vertex
                // if any of the downstream vertex returns false, then we should return false.
                if !self.generate_subgraph(edge.to.clone(), current.clone(), callback_map, subgraph)
                {
                    return false;
                }
            }
        }
        // if there are no downstream vertices, or all the downstream vertices returned true,
        // we can return true
        true
    }

    // from_env reads the pipeline stored in the environment variable and creates a MessageGraph from it.
    pub(crate) fn from_pipeline(pipeline_spec: &Pipeline) -> Result<Self, Error> {
        let mut dag = Graph::with_capacity(pipeline_spec.edges.len());
        for edge in &pipeline_spec.edges {
            dag.entry(edge.from.clone()).or_default().push(edge.clone());
        }

        Ok(MessageGraph { dag })
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{Conditions, Tag, Vertex};

    use super::*;

    #[test]
    fn test_no_subgraph() {
        let mut dag: Graph = HashMap::new();
        dag.insert(
            "a".to_string(),
            vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string(),
                    conditions: None,
                },
            ],
        );
        let message_graph = MessageGraph { dag };

        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();
        callback_map.insert(
            "a".to_string(),
            vec![CallbackRequestWrapper {
                callback_request: Arc::new(CallbackRequest {
                    id: "uuid1".to_string(),
                    vertex: "a".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    tags: None,
                }),
                visited: false,
            }],
        );

        let mut subgraph: Subgraph = Subgraph {
            id: "uuid1".to_string(),
            blocks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            "a".to_string(),
            "a".to_string(),
            &mut callback_map,
            &mut subgraph,
        );

        assert!(!result);
    }

    #[test]
    fn test_generate_subgraph() {
        let mut dag: Graph = HashMap::new();
        dag.insert(
            "a".to_string(),
            vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string(),
                    conditions: None,
                },
            ],
        );
        let message_graph = MessageGraph { dag };

        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();
        callback_map.insert(
            "a".to_string(),
            vec![CallbackRequestWrapper {
                callback_request: Arc::new(CallbackRequest {
                    id: "uuid1".to_string(),
                    vertex: "a".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    tags: None,
                }),
                visited: false,
            }],
        );

        callback_map.insert(
            "b".to_string(),
            vec![CallbackRequestWrapper {
                callback_request: Arc::new(CallbackRequest {
                    id: "uuid1".to_string(),
                    vertex: "b".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    tags: None,
                }),
                visited: false,
            }],
        );

        callback_map.insert(
            "c".to_string(),
            vec![CallbackRequestWrapper {
                callback_request: Arc::new(CallbackRequest {
                    id: "uuid1".to_string(),
                    vertex: "c".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    tags: None,
                }),
                visited: false,
            }],
        );

        let mut subgraph: Subgraph = Subgraph {
            id: "uuid1".to_string(),
            blocks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            "a".to_string(),
            "a".to_string(),
            &mut callback_map,
            &mut subgraph,
        );

        assert!(result);
    }

    #[test]
    fn test_generate_subgraph_complex() {
        let pipeline = Pipeline {
            vertices: vec![
                Vertex {
                    name: "a".to_string(),
                },
                Vertex {
                    name: "b".to_string(),
                },
                Vertex {
                    name: "c".to_string(),
                },
                Vertex {
                    name: "d".to_string(),
                },
                Vertex {
                    name: "e".to_string(),
                },
                Vertex {
                    name: "f".to_string(),
                },
                Vertex {
                    name: "g".to_string(),
                },
                Vertex {
                    name: "h".to_string(),
                },
                Vertex {
                    name: "i".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "b".to_string(),
                    to: "d".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "c".to_string(),
                    to: "e".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "d".to_string(),
                    to: "f".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "e".to_string(),
                    to: "f".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "f".to_string(),
                    to: "g".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "g".to_string(),
                    to: "h".to_string(),
                    conditions: Some(Conditions {
                        tags: Some(Tag {
                            operator: Some(OperatorType::And),
                            values: vec!["even".to_string()],
                        }),
                    }),
                },
                Edge {
                    from: "g".to_string(),
                    to: "i".to_string(),
                    conditions: Some(Conditions {
                        tags: Some(Tag {
                            operator: Some(OperatorType::Or),
                            values: vec!["odd".to_string()],
                        }),
                    }),
                },
            ],
        };

        let message_graph = MessageGraph::from_pipeline(&pipeline).unwrap();
        let source_vertex = "a".to_string();

        let raw_callback = r#"[
            {
                "id": "xxxx",
                "vertex": "a",
                "from_vertex": "a",
                "cb_time": 123456789
            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456867
            },
            {
                "id": "xxxx",
                "vertex": "c",
                "from_vertex": "a",
                "cb_time": 123456819
            },
            {
                "id": "xxxx",
                "vertex": "d",
                "from_vertex": "b",
                "cb_time": 123456840
            },
            {
                "id": "xxxx",
                "vertex": "e",
                "from_vertex": "c",
                "cb_time": 123456843
            },
            {
                "id": "xxxx",
                "vertex": "f",
                "from_vertex": "d",
                "cb_time": 123456854
            },
            {
                "id": "xxxx",
                "vertex": "f",
                "from_vertex": "e",
                "cb_time": 123456886
            },
            {
                "id": "xxxx",
                "vertex": "g",
                "from_vertex": "f",
                "tags": ["even"],
                "cb_time": 123456885
            },
            {
                "id": "xxxx",
                "vertex": "g",
                "from_vertex": "f",
                "tags": ["even"],
                "cb_time": 123456888
            },
            {
                "id": "xxxx",
                "vertex": "h",
                "from_vertex": "g",
                "cb_time": 123456889
            },
            {
                "id": "xxxx",
                "vertex": "h",
                "from_vertex": "g",
                "cb_time": 123456890
            }
        ]"#;

        let callbacks: Vec<CallbackRequest> = serde_json::from_str(raw_callback).unwrap();
        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();

        for callback in callbacks {
            callback_map
                .entry(callback.vertex.clone())
                .or_default()
                .push(CallbackRequestWrapper {
                    callback_request: Arc::new(callback),
                    visited: false,
                });
        }

        let mut subgraph: Subgraph = Subgraph {
            id: "xxxx".to_string(),
            blocks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            source_vertex.clone(),
            source_vertex,
            &mut callback_map,
            &mut subgraph,
        );

        assert!(result);
    }

    #[test]
    fn test_simple_dropped_message() {
        let pipeline = Pipeline {
            vertices: vec![
                Vertex {
                    name: "a".to_string(),
                },
                Vertex {
                    name: "b".to_string(),
                },
                Vertex {
                    name: "c".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                    conditions: None,
                },
            ],
        };

        let message_graph = MessageGraph::from_pipeline(&pipeline).unwrap();
        let source_vertex = "a".to_string();

        let raw_callback = r#"
        [
            {
                "id": "xxxx",
                "vertex": "a",
                "from_vertex": "a",
                "cb_time": 123456789
            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456867,
                "tags": ["U+005C__DROP__"]
            }
       ]"#;

        let callbacks: Vec<CallbackRequest> = serde_json::from_str(raw_callback).unwrap();
        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();

        for callback in callbacks {
            callback_map
                .entry(callback.vertex.clone())
                .or_default()
                .push(CallbackRequestWrapper {
                    callback_request: Arc::new(callback),
                    visited: false,
                });
        }

        let mut subgraph: Subgraph = Subgraph {
            id: "xxxx".to_string(),
            blocks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            source_vertex.clone(),
            source_vertex,
            &mut callback_map,
            &mut subgraph,
        );

        assert!(result);
    }

    #[test]
    fn test_complex_dropped_message() {
        let pipeline = Pipeline {
            vertices: vec![
                Vertex {
                    name: "a".to_string(),
                },
                Vertex {
                    name: "b".to_string(),
                },
                Vertex {
                    name: "c".to_string(),
                },
                Vertex {
                    name: "d".to_string(),
                },
                Vertex {
                    name: "e".to_string(),
                },
                Vertex {
                    name: "f".to_string(),
                },
                Vertex {
                    name: "g".to_string(),
                },
                Vertex {
                    name: "h".to_string(),
                },
                Vertex {
                    name: "i".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "b".to_string(),
                    to: "d".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "c".to_string(),
                    to: "e".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "d".to_string(),
                    to: "f".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "e".to_string(),
                    to: "f".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "f".to_string(),
                    to: "g".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "g".to_string(),
                    to: "h".to_string(),
                    conditions: Some(Conditions {
                        tags: Some(Tag {
                            operator: Some(OperatorType::And),
                            values: vec!["even".to_string()],
                        }),
                    }),
                },
                Edge {
                    from: "g".to_string(),
                    to: "i".to_string(),
                    conditions: Some(Conditions {
                        tags: Some(Tag {
                            operator: Some(OperatorType::Or),
                            values: vec!["odd".to_string()],
                        }),
                    }),
                },
            ],
        };

        let message_graph = MessageGraph::from_pipeline(&pipeline).unwrap();
        let source_vertex = "a".to_string();

        let raw_callback = r#"
        [
            {
                "id": "xxxx",
                "vertex": "a",
                "from_vertex": "a",
                "cb_time": 123456789
            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456867
            },
            {
                "id": "xxxx",
                "vertex": "c",
                "from_vertex": "a",
                "cb_time": 123456819,
                "tags": ["U+005C__DROP__"]
            },
            {
                "id": "xxxx",
                "vertex": "d",
                "from_vertex": "b",
                "cb_time": 123456840
            },
            {
                "id": "xxxx",
                "vertex": "f",
                "from_vertex": "d",
                "cb_time": 123456854
            },
            {
                "id": "xxxx",
                "vertex": "g",
                "from_vertex": "f",
                "tags": ["even"],
                "cb_time": 123456885
            },
            {
                "id": "xxxx",
                "vertex": "h",
                "from_vertex": "g",
                "cb_time": 123456889
            }
        ]"#;

        let callbacks: Vec<CallbackRequest> = serde_json::from_str(raw_callback).unwrap();
        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();

        for callback in callbacks {
            callback_map
                .entry(callback.vertex.clone())
                .or_default()
                .push(CallbackRequestWrapper {
                    callback_request: Arc::new(callback),
                    visited: false,
                });
        }

        let mut subgraph: Subgraph = Subgraph {
            id: "xxxx".to_string(),
            blocks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            source_vertex.clone(),
            source_vertex,
            &mut callback_map,
            &mut subgraph,
        );

        assert!(result);
    }

    #[test]
    fn test_simple_cycle_pipeline() {
        let pipeline = Pipeline {
            vertices: vec![
                Vertex {
                    name: "a".to_string(),
                },
                Vertex {
                    name: "b".to_string(),
                },
                Vertex {
                    name: "c".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    conditions: None,
                },
                Edge {
                    from: "b".to_string(),
                    to: "a".to_string(),
                    conditions: Some(Conditions {
                        tags: Some(Tag {
                            operator: Some(OperatorType::Not),
                            values: vec!["failed".to_string()],
                        }),
                    }),
                },
            ],
        };

        let message_graph = MessageGraph::from_pipeline(&pipeline).unwrap();
        let source_vertex = "a".to_string();

        let raw_callback = r#"
        [
            {
                "id": "xxxx",
                "vertex": "a",
                "from_vertex": "a",
                "cb_time": 123456789
            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456867,
                "tags": ["failed"]
            },
            {
                "id": "xxxx",
                "vertex": "a",
                "from_vertex": "b",
                "cb_time": 123456819
            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456819
            },
            {
                "id": "xxxx",
                "vertex": "c",
                "from_vertex": "b",
                "cb_time": 123456819
            }
        ]"#;

        let callbacks: Vec<CallbackRequest> = serde_json::from_str(raw_callback).unwrap();
        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();

        for callback in callbacks {
            callback_map
                .entry(callback.vertex.clone())
                .or_default()
                .push(CallbackRequestWrapper {
                    callback_request: Arc::new(callback),
                    visited: false,
                });
        }

        let mut subgraph: Subgraph = Subgraph {
            id: "xxxx".to_string(),
            blocks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            source_vertex.clone(),
            source_vertex,
            &mut callback_map,
            &mut subgraph,
        );

        assert!(result);
    }

    #[test]
    fn test_generate_subgraph_from_callbacks_with_invalid_vertex() {
        // Create a simple graph
        let mut dag: Graph = HashMap::new();
        dag.insert(
            "a".to_string(),
            vec![Edge {
                from: "a".to_string(),
                to: "b".to_string(),
                conditions: None,
            }],
        );
        let message_graph = MessageGraph { dag };

        // Create a callback with an invalid vertex
        let callbacks = vec![Arc::new(CallbackRequest {
            id: "test".to_string(),
            vertex: "invalid_vertex".to_string(),
            from_vertex: "invalid_vertex".to_string(),
            cb_time: 1,
            tags: None,
        })];

        // Call the function with the invalid callback
        let result = message_graph.generate_subgraph_from_callbacks("test".to_string(), callbacks);

        // Check that the function returned an error
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::SubGraphInvalidInput(_))));
    }
}
