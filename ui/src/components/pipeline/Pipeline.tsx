import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { ConnectionLineType, Edge, Node } from "react-flow-renderer";
import Graph from "./graph/Graph";
import { usePipelineFetch } from "../../utils/fetchWrappers/pipelineFetch";
import { useEdgesInfoFetch } from "../../utils/fetchWrappers/edgeInfoFetch";
import { VertexMetrics, VertexWatermark } from "../../utils/models/pipeline";
import "./Pipeline.css";

export function Pipeline() {
  const { namespaceId, pipelineId } = useParams();

  const [pipelineRequestKey, setPipelineRequestKey] = useState(`${Date.now()}`);

  const [vertexPods, setVertexPods] = useState<Map<string, number>>(null);

  const [vertexMetrics, setVertexMetrics] =
    useState<Map<string, VertexMetrics>>(null);

  const [vertexWatermark, setVertexWatermark] =
    useState<Map<string, VertexWatermark>>(null);

  const { pipeline, error: pipelineError } = usePipelineFetch(
    namespaceId,
    pipelineId,
    pipelineRequestKey
  );

  const [edgesInfoRequestKey, setEdgesInfoRequestKey] = useState(
    `${Date.now()}`
  );

  const { edgesInfo, error: edgesInfoError } = useEdgesInfoFetch(
    namespaceId,
    pipelineId,
    edgesInfoRequestKey
  );

  useEffect(() => {
    // Refresh pipeline info every x ms
    const interval = setInterval(() => {
      setPipelineRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

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
    if (pipeline?.spec?.vertices) {
      Promise.all(
        pipeline?.spec?.vertices.map((vertex) => {
          return fetch(
            `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertex.name}/pods`
          )
            .then((response) => response.json())
            .then((json) => {
              vertexToPodsMap.set(vertex.name, json.length);
            });
        })
      ).then(() => setVertexPods(vertexToPodsMap));
    }
  }, [pipeline]);

  const getMetrics = useCallback(() => {
    const vertexToMetricsMap = new Map();

    if (pipeline?.spec?.vertices) {
      Promise.all(
        pipeline?.spec?.vertices.map((vertex) => {
          return fetch(
            `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertex.name}/metrics`
          )
            .then((response) => response.json())
            .then((json) => {
              const vertexMetrics = {ratePerMin: 0, ratePerFiveMin: 0, ratePerFifteenMin: 0} as VertexMetrics;
              if ("processingRates" in json) {
                if ("1m" in json["processingRates"]) {
                  vertexMetrics.ratePerMin =
                  json["processingRates"]["1m"].toFixed(2);
                }
                if ("5m" in json["processingRates"]) {
                  vertexMetrics.ratePerFiveMin =
                  json["processingRates"]["5m"].toFixed(2);
                }
                if ("15m" in json["processingRates"]) {
                  vertexMetrics.ratePerFifteenMin =
                  json["processingRates"]["15m"].toFixed(2);
                }
              }
              vertexToMetricsMap.set(vertex.name, vertexMetrics);
            });
        })
      )
        .then(() => setVertexMetrics(vertexToMetricsMap))
        .catch(console.error);
    }
  }, [pipeline]);

  // This useEffect is used to obtain metrics for a given vertex in a pipeline and refreshes every 5 minutes
  useEffect(() => {
    getMetrics();
    const interval = setInterval(() => {
      getMetrics();
    }, 300000);

    return () => clearInterval(interval);
  }, [getMetrics]);

  // This is used to obtain the watermark of a given vertex
  const getWatermark = useCallback(() => {
    const vertexToWatermarkMap = new Map();
    if (pipeline?.spec?.vertices) {
      Promise.all(
        pipeline?.spec?.vertices.map((vertex) => {
          return fetch(
            `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertex.name}/watermark`
          )
            .then((response) => response.json())
            .then((json) => {
              const vertexWatermark = {} as VertexWatermark;
              vertexWatermark.isWaterMarkEnabled = json["isWatermarkEnabled"];
              vertexWatermark.watermark = json["watermark"];
              vertexWatermark.watermarkLocalTime = new Date(
                vertexWatermark.watermark
              ).toISOString();
              vertexToWatermarkMap.set(vertex.name, vertexWatermark);
            });
        })
      )
        .then(() => setVertexWatermark(vertexToWatermarkMap))
        .catch(console.error);
    }
  }, [pipeline]);

  // This useEffect is used to obtain watermark for a given vertex in a pipeline and refreshes every 1 minute
  useEffect(() => {
    getWatermark();
    const interval = setInterval(() => {
      getWatermark();
    }, 60000);

    return () => clearInterval(interval);
  }, [getWatermark]);

  const vertices = useMemo(() => {
    const newVertices: Node[] = [];
    if (
      pipeline?.spec?.vertices &&
      vertexPods &&
      vertexMetrics &&
      vertexWatermark
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
          : 0;
        newNode.data.vertexWatermark = vertexWatermark.has(vertex.name)
          ? vertexWatermark.get(vertex.name)
          : 0;
        newVertices.push(newNode);
      });
    }
    return newVertices;
  }, [pipeline, vertexPods, vertexMetrics, vertexWatermark]);

  const edges = useMemo(() => {
    const newEdges: Edge[] = [];
    if (pipeline?.spec?.edges && edgesInfo) {
      pipeline.spec.edges.map((edge) => {
        edgesInfo.map((edgeInfo) => {
          if (
            edgeInfo.fromVertex === edge.from &&
            edgeInfo.toVertex === edge.to
          ) {
            const label = `${edgeInfo.ackPendingCount + edgeInfo.pendingCount}`;
            const pipelineEdge = {
              id: edge.from + "-" + edge.to,
              label,
              source: edge.from,
              target: edge.to,
              labelStyle: {
                cursor: "pointer",
                fontFamily: "IBM Plex Sans",
                fontWeight: 400,
                fontSize: "0.50rem",
              },
              data: {
                ...edgeInfo,
                conditions: edge.conditions,
                pending: edgeInfo.pendingCount,
                ackPending: edgeInfo.ackPendingCount,
                bufferLength: edgeInfo.bufferLength,
              },
            } as Edge;
            // Color the edge on isFull
            if (edgeInfo.isFull) {
              pipelineEdge.style = {
                stroke: "red",
                strokeWidth: "4px",
                fontWeight: 700,
              };
            }
            pipelineEdge.animated = true;
            pipelineEdge.type = ConnectionLineType.SmoothStep;
            newEdges.push(pipelineEdge);
          }
        });
      });
    }
    return newEdges;
  }, [pipeline, edgesInfo]);

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
