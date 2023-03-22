import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { Edge, Node } from "reactflow";
import Graph from "./graph/Graph";
import { usePipelineFetch } from "../../utils/fetchWrappers/pipelineFetch";
import { useEdgesInfoFetch } from "../../utils/fetchWrappers/edgeInfoFetch";
import { VertexMetrics, EdgeWatermark } from "../../utils/models/pipeline";
import "./Pipeline.css";
import { notifyError } from "../../utils/error";

export function Pipeline() {
  const [vertexPods, setVertexPods] =
    useState<Map<string, number>>(null);

  const [vertexMetrics, setVertexMetrics] =
    useState<Map<string, VertexMetrics>>(null);

  const [edgeWatermark, setEdgeWatermark] =
    useState<Map<string, EdgeWatermark>>(null)

  const [podsErr, setPodsErr] =
    useState<any[]>(null);

  const [metricsErr, setMetricsErr] =
    useState<any[]>(null);

  const [watermarkErr, setWatermarkErr] =
    useState<any[]>(null);

  const [pipelineRequestKey, setPipelineRequestKey] = useState(
    `${Date.now()}`
  );

  const [edgesInfoRequestKey, setEdgesInfoRequestKey] = useState(
    `${Date.now()}`
  );

  const { namespaceId, pipelineId } = useParams();

  const { pipeline, error: pipelineError } = usePipelineFetch(
    namespaceId,
    pipelineId,
    pipelineRequestKey
  );

  // This useEffect notifies about the errors while querying for the vertices of the pipeline
  useEffect(() => {
    if (pipelineError){
      notifyError([{
        error: "Failed to fetch the pipeline vertices",
        options: {toastId: "pl-vertex", autoClose: false}
      }]);
    }
  }, [pipelineError]);

  useEffect(() => {
    // Refresh pipeline info every x ms
    const interval = setInterval(() => {
      setPipelineRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  const { edgesInfo, error: edgesInfoError } = useEdgesInfoFetch(
    namespaceId,
    pipelineId,
    edgesInfoRequestKey
  );

  // This useEffect notifies about the errors while querying for the edges of the pipeline
  useEffect(() => {
    if (edgesInfoError) {
      notifyError([{
        error: "Failed to fetch the pipeline edges",
        options: {toastId: "pl-edge", autoClose: false}
      }]);
    }
  }, [edgesInfoError]);

  useEffect(() => {
    // Refresh edgesInfo every x ms
    const interval = setInterval(() => {
      setEdgesInfoRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  // This useEffect is used to obtain all the pods for a given vertex in a pipeline.
  useEffect(() => {
    const vertexToPodsMap = new Map();
    const podsErr: any[] = [];

    if (pipeline?.spec?.vertices) {
      Promise.allSettled(
        pipeline?.spec?.vertices.map((vertex) => {
          return fetch(
            `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertex.name}/pods`
          )
            .then((response) => {
              if (response.ok) {
                return response.json();
              } else {
                return Promise.reject({response, vertex:vertex.name});
              }
            })
            .then((json) => {
              vertexToPodsMap.set(vertex.name, json.length);
            });
        })
      )
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              podsErr.push({
                error: `${result.reason.response.status}: Failed to get pods count for ${result.reason.vertex} vertex`,
                options: {toastId: `${result.reason.vertex}-pods`, autoClose: 5000}
              });
            }
          })
          setPodsErr(podsErr);
        })
        .then(() => {setVertexPods(vertexToPodsMap)})
        .catch(console.error)
    }
  }, [pipeline]);

  // This useEffect notifies about the errors while querying for the pod count of a given vertex
  useEffect(() => {
    notifyError(podsErr);
  }, [podsErr]);

  const getMetrics = useCallback(() => {
    const vertexToMetricsMap = new Map();
    const metricsErr: any[] = [];

    if (pipeline?.spec?.vertices) {
      Promise.allSettled(
        pipeline?.spec?.vertices.map((vertex) => {
          return fetch(
            `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertex.name}/metrics`
          )
            .then((response) => {
              if (response.ok) {
                return response.json();
              } else {
                return Promise.reject({response, vertex:vertex.name});
              }
            })
            .then((json) => {
              const vertexMetrics = {ratePerMin: "0.00", ratePerFiveMin: "0.00", ratePerFifteenMin: "0.00", podMetrics: null, error: false} as VertexMetrics;
              let ratePerMin = 0.0, ratePerFiveMin = 0.0, ratePerFifteenMin = 0.0;
              // keeping processing rates as summation of pod values
              json.map((pod) => {
                if ("processingRates" in pod) {
                  if ("1m" in pod["processingRates"]) {
                    ratePerMin += pod["processingRates"]["1m"];
                  }
                  if ("5m" in pod["processingRates"]) {
                    ratePerFiveMin += pod["processingRates"]["5m"];
                  }
                  if ("15m" in pod["processingRates"]) {
                    ratePerFifteenMin += pod["processingRates"]["15m"];
                  }
                } else {
                  if (vertexPods && vertexPods.get(vertex.name) !== 0) {
                    vertexMetrics.error = true;
                    metricsErr.push({
                      error: `404: Failed to get metrics for ${vertex.name} vertex`,
                      options: {toastId: `${vertex.name}-metrics`, autoClose: 5000}
                    });
                  }
                }
              })
              vertexMetrics.ratePerMin = ratePerMin.toFixed(2);
              vertexMetrics.ratePerFiveMin = ratePerFiveMin.toFixed(2);
              vertexMetrics.ratePerFifteenMin = ratePerFifteenMin.toFixed(2);
              if (vertexPods && vertexPods.get(vertex.name) !== 0) {
                vertexMetrics.podMetrics = json;
              }
              vertexToMetricsMap.set(vertex.name, vertexMetrics);
            });
        })
      )
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              metricsErr.push({
                error: `${result.reason.response.status}: Failed to get metrics for ${result.reason.vertex} vertex`,
                options: {toastId: `${result.reason.vertex}-metrics`, autoClose: 5000}
              });
            }
          })
          setMetricsErr(metricsErr);
        })
        .then(() => setVertexMetrics(vertexToMetricsMap))
        .catch(console.error);
    }
  }, [pipeline, vertexPods]);

  // This useEffect is used to obtain metrics for a given vertex in a pipeline and refreshes every 5 minutes
  useEffect(() => {
    getMetrics();
    const interval = setInterval(() => {
      getMetrics();
    }, 300000);

    return () => clearInterval(interval);
  }, [getMetrics]);

  // This useEffect notifies about the errors while querying for the metrics of a given vertex
  useEffect(() => {
    notifyError(metricsErr);
  }, [metricsErr]);

  // This is used to obtain the watermark of a given pipeline
  const getPipelineWatermarks = useCallback(() => {
    const edgeToWatermarkMap = new Map();
    const watermarkErr: any[] = [];

    if (pipeline?.spec?.edges) {
      if (pipeline?.spec?.watermark?.disabled === true) {
        setEdgeWatermark(edgeToWatermarkMap)
      } else {
        Promise.allSettled( [
              fetch(
                  `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/watermarks`
              )
                  .then((response) => {
                    if (response.ok) {
                      return response.json();
                    } else {
                      return Promise.reject(response);
                    }
                  })
                  .then((json) => {
                    json.map((edge) => {
                      const edgeWatermark = {} as EdgeWatermark;
                      edgeWatermark.isWaterMarkEnabled = edge["isWatermarkEnabled"];
                      edgeWatermark.watermarks = edge["watermarks"];
                      edgeToWatermarkMap.set(edge.edge, edgeWatermark);
                    })
                  })
            ]
        )
          .then((results) => {
            results.forEach((result) => {
              if (result && result?.status === "rejected") {
                watermarkErr.push({
                  error: `${result.reason.status}: Failed to get watermarks for some vertices`,
                  options: {toastId: "vertex-watermarks", autoClose: 5000}
                });
              }
            })
            setWatermarkErr(watermarkErr);
          })
          .then(() => setEdgeWatermark(edgeToWatermarkMap))
          .catch(console.error);
      }
    }
  }, [pipeline]);

  // This useEffect is used to obtain watermark for a given vertex in a pipeline and refreshes every 1 minute
  useEffect(() => {
    getPipelineWatermarks();
    const interval = setInterval(() => {
      getPipelineWatermarks();
    }, 60000);

    return () => clearInterval(interval);
  }, [getPipelineWatermarks]);

  // This useEffect notifies about the errors while querying for the watermark of the pipeline
  useEffect(() => {
    notifyError(watermarkErr);
  }, [watermarkErr]);

  const vertices = useMemo(() => {
    const newVertices: Node[] = [];
    if (
      pipeline?.spec?.vertices &&
      vertexPods &&
      vertexMetrics
    ) {
      pipeline.spec.vertices.map((vertex) => {
        const newNode = {} as Node;
        newNode.id = vertex.name;
        newNode.data = { name: vertex.name};
        newNode.data.podnum = vertexPods.has(vertex.name) ? vertexPods.get(vertex.name) : 0;
        newNode.position = { x: 0, y: 0 };
        // change this in the future if you would like to make it draggable
        newNode.draggable = false;
        if (vertex.source) {
          newNode.type = "source";
          newNode.data.source = vertex;
        } else if (vertex.sink) {
          newNode.type = "sink";
          newNode.data.sink = vertex;
          newNode.data.test = vertex.name;
        } else {
          newNode.data.udf = vertex;
          newNode.type = "udf";
        }
        newNode.data.vertexMetrics = vertexMetrics.has(vertex.name)
          ? vertexMetrics.get(vertex.name)
          : null;
        newVertices.push(newNode);
      });
    }
    return newVertices;
  }, [pipeline, vertexPods, vertexMetrics]);

  const edges = useMemo(() => {
    const newEdges: Edge[] = [];
    if (
        pipeline?.spec?.edges &&
        edgesInfo &&
        edgeWatermark
    ) {
      // for an edge it is the sum of backpressure between vertices - the value we see on the edge
      // map from edge-id( from-Vertex - to-Vertex ) to sum of backpressure
      const edgeBackpressureLabel = new Map();

      edgesInfo.forEach((edge) => {
        const id = edge.fromVertex + "-" + edge.toVertex;
        if (edgeBackpressureLabel.get(id) === undefined) edgeBackpressureLabel.set(id, Number(edge.totalMessages));
        else edgeBackpressureLabel.set(id, edgeBackpressureLabel.get(id) + Number(edge.totalMessages));
      });

      pipeline.spec.edges.map((edge) => {
        edgesInfo.map((edgeInfo) => {
          if (
            edgeInfo.fromVertex === edge.from &&
            edgeInfo.toVertex === edge.to
          ) {
            const id = edge.from + "-" + edge.to;
            const pipelineEdge = {
              id,
              source: edge.from,
              target: edge.to,
              data: {
                ...edgeInfo,
                conditions: edge.conditions,
                pending: edgeInfo.pendingCount,
                ackPending: edgeInfo.ackPendingCount,
                bufferLength: edgeInfo.bufferLength,
                isFull: edgeInfo.isFull,
                backpressureLabel: edgeBackpressureLabel.get(id),
              },
            } as Edge;
            pipelineEdge.data.edgeWatermark = edgeWatermark.has(pipelineEdge.id)
                ? edgeWatermark.get(pipelineEdge.id)
                : null;
            pipelineEdge.animated = true;
            pipelineEdge.type = 'custom';
            newEdges.push(pipelineEdge);
          }
        });
      });
    }
    return newEdges;
  }, [pipeline, edgesInfo, edgeWatermark]);

  if (pipelineError || edgesInfoError) {
    return <div>Error</div>;
  }

  return (
    <div data-testid={"pipeline"} style={{ overflow: "scroll !important" }}>
      {pipeline?.spec &&
        edgesInfo.length > 0 &&
        edges.length > 0 &&
        vertices.length > 0 && (
          <Graph
            data={{
              ...pipeline.spec,
              edges: edges,
              vertices: vertices,
              pipeline: pipeline,
            }}
            namespaceId={namespaceId}
            pipelineId={pipelineId}
          />
        )}
    </div>
  );
}
