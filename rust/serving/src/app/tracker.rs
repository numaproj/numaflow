use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::callback::Callback;
use crate::pipeline::{Edge, OperatorType, PipelineDCG};

const DROP: &str = "U+005C__DROP__";

fn compare_slice(operator: &OperatorType, a: &[String], b: &[String]) -> bool {
    match operator {
        OperatorType::And => a.iter().all(|val| b.contains(val)),
        OperatorType::Or => a.iter().any(|val| b.contains(val)),
        OperatorType::Not => !a.iter().any(|val| b.contains(val)),
    }
}

/// hash map of vertex and its edges. The key of the HashMap and `Edge.from` are same, `Edge.to` will
/// help find the adjacent vertex.
type Graph = HashMap<String, Vec<Edge>>;

#[derive(Serialize, Deserialize, Debug)]
struct Subgraph {
    id: String,
    edge_callbacks: Vec<EdgeCallback>,
}

/// MessageGraph is a struct that generates the graph from the source vertex to the downstream vertices
/// for a message using the given callbacks.
pub(crate) struct MessageGraph {
    dag: Graph,
}

/// Block is a struct that contains the information about the edge-information and callback info in
/// the subgraph.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct EdgeCallback {
    from: String,
    to: String,
    cb_time: u64,
}

// CallbackRequestWrapper is a struct that contains the information about the callback request and
// whether it has been visited or not. It is used to keep track of the visited callbacks.
#[derive(Debug)]
struct CallbackRequestWrapper {
    callback_request: Arc<Callback>,
    visited: bool,
}

impl MessageGraph {
    /// Generates a sub graph from a list of [Callback].
    /// It first creates a HashMap to map each vertex to its corresponding callbacks.
    /// NOTE: it finds checks whether the callback is from the originating source vertex by checking
    /// if the vertex and from_vertex fields are the same.
    /// Finally, it calls the [Self::generate_subgraph] function to generate the subgraph from the source
    /// vertex.
    pub(crate) fn generate_subgraph_from_callbacks(
        &self,
        id: String,
        callbacks: Vec<Arc<Callback>>,
    ) -> Result<Option<String>, Error> {
        // For a monovertex, there will only be 1 callback.
        // Transformer will only do a callback if the message is dropped.
        if self.is_monovertex() && callbacks.len() == 1 {
            // We return a json object similar to that returned by pipeline.
            let cb = callbacks.first();
            let mut subgraph: Subgraph = Subgraph {
                id,
                edge_callbacks: Vec::new(),
            };
            if let Some(cb) = cb {
                subgraph.edge_callbacks.push(EdgeCallback {
                    from: cb.from_vertex.clone(),
                    to: cb.vertex.clone(),
                    cb_time: cb.cb_time,
                });
            }
            return match serde_json::to_string(&subgraph) {
                Ok(json) => Ok(Some(json)),
                Err(e) => Err(Error::SubGraphGenerator(e.to_string())),
            };
        }

        // Create a HashMap to map each vertex to its corresponding callbacks
        let mut callback_map: HashMap<String, Vec<CallbackRequestWrapper>> = HashMap::new();
        let mut source_vertex = None;

        // Find the source vertex, source vertex is the vertex that has the same vertex and from_vertex
        // or if the from_vertex is empty
        for callback in callbacks {
            // If the vertex and from_vertex are the same, it is the source vertex
            // OR if the from_vertex is empty, it is the source vertex
            if callback.vertex == callback.from_vertex || callback.from_vertex.is_empty() {
                source_vertex = Some(callback.vertex.clone());
            }
            callback_map
                .entry(callback.vertex.clone())
                .or_default()
                .push(CallbackRequestWrapper {
                    callback_request: Arc::clone(&callback),
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
            edge_callbacks: Vec::new(),
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
                Err(e) => Err(Error::SubGraphGenerator(e.to_string())),
            }
        } else {
            Ok(None)
        }
    }

    /// generate_subgraph function is a recursive function that generates the sub graph from the source
    /// vertex for the given list of callbacks. The function returns true if the subgraph is
    /// generated successfully (if we are able to find a subgraph for the message using the given
    /// callbacks), it updates the subgraph with the path from the source vertex to the downstream
    /// vertices.
    /// It uses the pipeline DAG [Graph] and the [Callback]'s HashMap to check whether sub-graph is
    /// complete.
    fn generate_subgraph(
        &self,
        current: String,
        from: String,
        callback_map: &mut HashMap<String, Vec<CallbackRequestWrapper>>,
        subgraph: &mut Subgraph,
    ) -> bool {
        let mut current_callback: Option<Arc<Callback>> = None;

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
            // from_vertex will be empty for the source vertex
            if (callback.callback_request.from_vertex.is_empty()
                || callback.callback_request.from_vertex == from)
                && !callback.visited
            {
                callback.visited = true;
                current_callback = Some(Arc::clone(&callback.callback_request));
                break;
            }
        }

        // If there is no callback which is not visited and its parent is the "from" vertex, then we should
        // return false because we have not received all the callbacks for the given message.
        let Some(current_callback) = current_callback else {
            return false;
        };

        // add the current block to the subgraph
        subgraph.edge_callbacks.push(EdgeCallback {
            from: current_callback.from_vertex.clone(),
            to: current_callback.vertex.clone(),
            cb_time: current_callback.cb_time,
        });

        // if there are no responses or if any of the response contains drop tag means the message
        // was dropped we can return true.
        if current_callback.responses.is_empty()
            || current_callback.responses.iter().any(|response| {
                response
                    .tags
                    .as_ref()
                    .is_some_and(|tags| tags.contains(&DROP.to_string()))
            })
        {
            return true;
        }

        if current_callback.responses.is_empty() {
            return true;
        }

        // iterate over the responses of the current callback, for flatmap operation there can
        // more than one response, so we need to make sure all the responses are processed.
        // For example a -> b -> c, lets say vertex a has 2 responses. We will have to recursively
        // find the subgraph for both the responses.
        for response in current_callback.responses.iter() {
            // recursively invoke the downstream vertices of the current vertex, if any
            if let Some(edges) = self.dag.get(&current) {
                for edge in edges {
                    // check if the edge should proceed based on the conditions
                    // if there are no conditions, we should proceed with the edge
                    // if there are conditions, we should check the tags of the current callback
                    // with the tags of the edge and the operator of the tags to decide if we should
                    // proceed with the edge
                    let will_continue_the_path = edge
                        .conditions
                        .as_ref()
                        // If the edge has conditions, get the tags
                        .and_then(|conditions| conditions.tags.as_ref())
                        // If there are no conditions or tags, default to true (i.e., proceed with the edge)
                        // If there are tags, compare the tags with the current callback's tags and the operator
                        // to decide if we should proceed with the edge.
                        .is_none_or(|tags| {
                            response
                                .tags
                                .as_ref()
                                // If the current callback has no tags we should not proceed with the edge for "and" and "or" operators
                                // because we expect the current callback to have tags specified in the edge.
                                // If the current callback has no tags we should proceed with the edge for "not" operator.
                                // because we don't expect the current callback to have tags specified in the edge.
                                .map_or(
                                    tags.operator.as_ref() == Some(&OperatorType::Not),
                                    |callback_tags| {
                                        tags.operator.as_ref().is_some_and(|operator| {
                                            // If there is no operator, default to false (i.e., do not proceed with the edge)
                                            // If there is an operator, compare the current callback's tags with the edge's tags
                                            compare_slice(operator, callback_tags, &tags.values)
                                        })
                                    },
                                )
                        });

                    // if the conditions are not met, then proceed to the next edge because conditions
                    // for forwarding the message did not match the conditional forwarding requirements.
                    if !will_continue_the_path {
                        // let's move on to the next edge
                        continue;
                    }

                    // recursively proceed to the next downstream vertex
                    // if any of the downstream vertex returns false, then we should return false.
                    if !self.generate_subgraph(
                        edge.to.clone(),
                        current.clone(),
                        callback_map,
                        subgraph,
                    ) {
                        return false;
                    }
                }
            }
            // if there are no downstream vertices, or all the downstream vertices returned true,
            // we can return true
        }
        true
    }

    // from_env reads the pipeline stored in the environment variable and creates a MessageGraph from it.
    pub(crate) fn from_pipeline(pipeline_spec: &PipelineDCG) -> Result<Self, Error> {
        let mut dag = Graph::with_capacity(pipeline_spec.edges.len());
        for edge in &pipeline_spec.edges {
            dag.entry(edge.from.clone()).or_default().push(edge.clone());
        }
        Ok(MessageGraph { dag })
    }

    pub(crate) fn is_monovertex(&self) -> bool {
        // The DAG is created from the edges. For a monovertex, edges will be empty.
        self.dag.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback::Response;
    use crate::pipeline::{Conditions, Tag, Vertex};

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
                callback_request: Arc::new(Callback {
                    id: "uuid1".to_string(),
                    vertex: "a".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    responses: vec![Response { tags: None }],
                }),
                visited: false,
            }],
        );

        let mut subgraph: Subgraph = Subgraph {
            id: "uuid1".to_string(),
            edge_callbacks: Vec::new(),
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
                callback_request: Arc::new(Callback {
                    id: "uuid1".to_string(),
                    vertex: "a".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    responses: vec![Response { tags: None }],
                }),
                visited: false,
            }],
        );

        callback_map.insert(
            "b".to_string(),
            vec![CallbackRequestWrapper {
                callback_request: Arc::new(Callback {
                    id: "uuid1".to_string(),
                    vertex: "b".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    responses: vec![Response { tags: None }],
                }),
                visited: false,
            }],
        );

        callback_map.insert(
            "c".to_string(),
            vec![CallbackRequestWrapper {
                callback_request: Arc::new(Callback {
                    id: "uuid1".to_string(),
                    vertex: "c".to_string(),
                    cb_time: 1,
                    from_vertex: "a".to_string(),
                    responses: vec![Response { tags: None }],
                }),
                visited: false,
            }],
        );

        let mut subgraph: Subgraph = Subgraph {
            id: "uuid1".to_string(),
            edge_callbacks: Vec::new(),
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
        let pipeline = PipelineDCG {
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
            "from_vertex": "",
            "cb_time": 123456789,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "b",
            "from_vertex": "a",
            "cb_time": 123456867,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "a",
            "cb_time": 123456819,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "d",
            "from_vertex": "b",
            "cb_time": 123456840,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "e",
            "from_vertex": "c",
            "cb_time": 123456843,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "d",
            "cb_time": 123456854,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "e",
            "cb_time": 123456886,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456885,
            "responses": [{"tags": ["even"]}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456888,
            "responses": [{"tags": ["even"]}]
        },
        {
            "id": "xxxx",
            "vertex": "h",
            "from_vertex": "g",
            "cb_time": 123456889,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "h",
            "from_vertex": "g",
            "cb_time": 123456890,
            "responses": [{"tags": null}]
        }
    ]"#;

        let callbacks: Vec<Callback> = serde_json::from_str(raw_callback).unwrap();
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
            edge_callbacks: Vec::new(),
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
        let pipeline = PipelineDCG {
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
            "from_vertex": "",
            "cb_time": 123456789,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "b",
            "from_vertex": "a",
            "cb_time": 123456867,
            "responses": []
        }
    ]"#;

        let callbacks: Vec<Callback> = serde_json::from_str(raw_callback).unwrap();
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
            edge_callbacks: Vec::new(),
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
        let pipeline = PipelineDCG {
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
            "from_vertex": "",
            "cb_time": 123456789,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "b",
            "from_vertex": "a",
            "cb_time": 123456867,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "a",
            "cb_time": 123456819,
            "responses": []
        },
        {
            "id": "xxxx",
            "vertex": "d",
            "from_vertex": "b",
            "cb_time": 123456840,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "d",
            "cb_time": 123456854,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456885,
            "responses": [{"tags": ["even"]}]
        },
        {
            "id": "xxxx",
            "vertex": "h",
            "from_vertex": "g",
            "cb_time": 123456889,
            "responses": [{"tags": null}]
        }
    ]"#;

        let callbacks: Vec<Callback> = serde_json::from_str(raw_callback).unwrap();
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
            edge_callbacks: Vec::new(),
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
        let pipeline = PipelineDCG {
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
                "from_vertex": "",
                "cb_time": 123456789,
                "responses": [{"tags": null}]
            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456867,
                "responses": [{"tags": ["failed"]}]
            },
            {
                "id": "xxxx",
                "vertex": "a",
                "from_vertex": "b",
                "cb_time": 123456819,
                 "responses": [{"tags": null}]

            },
            {
                "id": "xxxx",
                "vertex": "b",
                "from_vertex": "a",
                "cb_time": 123456819,
                "responses": [{"tags": null}]

            },
            {
                "id": "xxxx",
                "vertex": "c",
                "from_vertex": "b",
                "cb_time": 123456819,
                "responses": [{"tags": null}]

            }
        ]"#;

        let callbacks: Vec<Callback> = serde_json::from_str(raw_callback).unwrap();
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
            edge_callbacks: Vec::new(),
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
    fn test_flatmap_operation_with_simple_dag() {
        let pipeline = PipelineDCG {
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

        let raw_callback = r#"[
        {
            "id": "xxxx",
            "vertex": "a",
            "from_vertex": "",
            "cb_time": 123456789,
            "responses": [
                {"tags": null},
                {"index": 1, "tags": null}
            ]
        },
        {
            "id": "xxxx",
            "vertex": "b",
            "from_vertex": "a",
            "cb_time": 123456867,
            "responses": [
                {"tags": null},
                {"index": 1, "tags": null}
            ]
        },
        {
            "id": "xxxx",
            "vertex": "b",
            "from_vertex": "a",
            "cb_time": 123456868,
            "responses": [
                {"tags": null},
                {"index": 1, "tags": null}
            ]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "b",
            "cb_time": 123456869,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "b",
            "cb_time": 123456870,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "b",
            "cb_time": 123456871,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "b",
            "cb_time": 123456872,
            "responses": [{"tags": null}]
        }
    ]"#;

        let callbacks: Vec<Callback> = serde_json::from_str(raw_callback).unwrap();
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
            edge_callbacks: Vec::new(),
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
    fn test_flatmap_operation_with_complex_dag() {
        let pipeline = PipelineDCG {
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
            "from_vertex": "",
            "cb_time": 123456789,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "b",
            "from_vertex": "a",
            "cb_time": 123456867,
            "responses": [
                {"tags": null},
                {"index": 1, "tags": null}
            ]
        },
        {
            "id": "xxxx",
            "vertex": "c",
            "from_vertex": "a",
            "cb_time": 123456819,
            "responses": [
                {"tags": null},
                {"index": 1, "tags": null},
                {"index": 2, "tags": null}
            ]
        },
        {
            "id": "xxxx",
            "vertex": "d",
            "from_vertex": "b",
            "cb_time": 123456840,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "d",
            "from_vertex": "b",
            "cb_time": 123456841,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "e",
            "from_vertex": "c",
            "cb_time": 123456843,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "e",
            "from_vertex": "c",
            "cb_time": 123456844,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "e",
            "from_vertex": "c",
            "cb_time": 123456845,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "d",
            "cb_time": 123456854,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "d",
            "cb_time": 123456854,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "e",
            "cb_time": 123456886,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "e",
            "cb_time": 123456887,
            "responses": [{"index": 1, "tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "f",
            "from_vertex": "e",
            "cb_time": 123456888,
            "responses": [{"index": 2, "tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456885,
            "responses": [{"tags": ["even"]}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456886,
            "responses": [{"index": 1, "tags": ["even"]}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456887,
            "responses": [{"index": 2, "tags": ["odd"]}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456888,
            "responses": [{"index": 3, "tags": ["odd"]}]
        },
        {
            "id": "xxxx",
            "vertex": "g",
            "from_vertex": "f",
            "cb_time": 123456889,
            "responses": [{"index": 4, "tags": ["odd"]}]
        },
        {
            "id": "xxxx",
            "vertex": "h",
            "from_vertex": "g",
            "cb_time": 123456890,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "h",
            "from_vertex": "g",
            "cb_time": 123456891,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "i",
            "from_vertex": "g",
            "cb_time": 123456892,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "i",
            "from_vertex": "g",
            "cb_time": 123456893,
            "responses": [{"tags": null}]
        },
        {
            "id": "xxxx",
            "vertex": "i",
            "from_vertex": "g",
            "cb_time": 123456894,
            "responses": [{"tags": null}]
        }
    ]"#;

        let callbacks: Vec<Callback> = serde_json::from_str(raw_callback).unwrap();
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
            edge_callbacks: Vec::new(),
        };
        let result = message_graph.generate_subgraph(
            source_vertex.clone(),
            source_vertex,
            &mut callback_map,
            &mut subgraph,
        );

        assert!(result);
    }
}
