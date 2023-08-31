import { useCallback, useEffect, useMemo, useState } from "react";
import { Edge, MarkerType, Node } from "reactflow";
import { isEqual } from "lodash";
import {
  BufferInfo,
  EdgeWatermark,
  Pipeline,
  Spec,
  VertexMetrics,
} from "../../types/declarations/pipeline";

export const usePipelineViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined
) => {
  const [requestKey, setRequestKey] = useState(`${Date.now()}`);
  const [pipeline, setPipeline] = useState<Pipeline | undefined>(undefined);
  const [ns_pl, setNS_PL] = useState("");
  const [spec, setSpec] = useState<Spec | undefined>(undefined);
  const [buffers, setBuffers] = useState<BufferInfo[]>([]);
  const [vertexPods, setVertexPods] = useState<Map<string, number>>(new Map());
  const [vertexMetrics, setVertexMetrics] = useState<
    Map<string, VertexMetrics>
  >(new Map());
  const [edgeWatermark, setEdgeWatermark] = useState<
    Map<string, EdgeWatermark>
  >(new Map());
  const [selfEdges, setSelfEdges] = useState<Set<string>>(new Set());
  const [backEdges, setBackEdges] = useState<Set<string>>(new Set());
  const [fwdEdges, setFwdEdges] = useState<Set<string>>(new Set());
  const [selfVertices, setSelfVertices] = useState<Set<string>>(new Set());
  const [prevVertices, setPrevVertices] = useState<Set<string>>(new Set());
  const [nextVertices, setNextVertices] = useState<Set<string>>(new Set());
  const [backEdgesHeight, setBackEdgesHeight] = useState<Map<string, number>>(
    new Map()
  );
  const [nodeOutDegree, setNodeOutDegree] = useState<Map<string, number>>(
    new Map()
  );
  const [pipelineErr, setPipelineErr] = useState<any[] | undefined>(undefined);
  const [buffersErr, setBuffersErr] = useState<any[] | undefined>(undefined);
  const [podsErr, setPodsErr] = useState<any[] | undefined>(undefined);
  const [metricsErr, setMetricsErr] = useState<any[] | undefined>(undefined);
  const [watermarkErr, setWatermarkErr] = useState<any[] | undefined>(
    undefined
  );
  const [loading, setLoading] = useState(true);

  const BASE_API = `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}`;

  // call to get pipeline
  useEffect(() => {
    const fetchPipeline = async () => {
      try {
        const response = await fetch(`${BASE_API}?refreshKey=${requestKey}`);
        if (response.ok) {
          const data = await response.json();
          setPipeline(data);
          setNS_PL(`${data?.metadata?.namespace}-${data?.metadata?.name}-`);
          if (!isEqual(spec, data?.spec)) setSpec(data?.spec);
        } else {
          setPipelineErr([
            {
              error: "Failed to fetch the pipeline details",
              options: { toastId: "pl-details", autoClose: false },
            },
          ]);
        }
      } catch {
        setPipelineErr([
          {
            error: "Failed to fetch the pipeline details",
            options: { toastId: "pl-details", autoClose: false },
          },
        ]);
      }
    };

    fetchPipeline();
  }, [requestKey]);

  // call to get buffers
  useEffect(() => {
    const fetchBuffers = async () => {
      try {
        const response = await fetch(
          `${BASE_API}/buffers?refreshKey=${requestKey}`
        );
        if (response.ok) {
          const data = await response.json();
          setBuffers(data);
        } else {
          setBuffersErr([
            {
              error: "Failed to fetch the pipeline buffers",
              options: { toastId: "pl-buffer", autoClose: false },
            },
          ]);
        }
      } catch (e: any) {
        setBuffersErr([
          {
            error: "Failed to fetch the pipeline buffers",
            options: { toastId: "pl-buffer", autoClose: false },
          },
        ]);
      }
    };

    fetchBuffers();
  }, [requestKey]);

  // Refresh pipeline and buffer info every 30 sec
  useEffect(() => {
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  // This useEffect is used to obtain all the pods for a given vertex in a pipeline.
  useEffect(() => {
    const vertexToPodsMap = new Map();
    const podsErr: any[] = [];

    if (spec?.vertices) {
      Promise.allSettled(
        spec.vertices.map((vertex: any) => {
          return fetch(`${BASE_API}/vertices/${vertex.name}/pods`)
            .then((response) => {
              if (response.ok) {
                return response.json();
              } else {
                return Promise.reject({ response, vertex: vertex.name });
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
                options: {
                  toastId: `${result.reason.vertex}-pods`,
                  autoClose: 5000,
                },
              });
            }
          });
          if (podsErr.length > 0) setPodsErr(podsErr);
        })
        .then(() => {
          if (!isEqual(vertexPods, vertexToPodsMap)) {
            setVertexPods(vertexToPodsMap);
          }
        })
        .catch(console.error);
    }
  }, [spec, requestKey]);

  const getVertexMetrics = useCallback(() => {
    const vertexToMetricsMap = new Map();
    const metricsErr: any[] = [];

    if (spec?.vertices && vertexPods.size > 0) {
      Promise.allSettled(
        spec.vertices.map((vertex: any) => {
          return fetch(`${BASE_API}/vertices/${vertex.name}/metrics`)
            .then((response) => {
              if (response.ok) {
                return response.json();
              } else {
                return Promise.reject({ response, vertex: vertex.name });
              }
            })
            .then((json) => {
              const vertexMetrics = {
                ratePerMin: "0.00",
                ratePerFiveMin: "0.00",
                ratePerFifteenMin: "0.00",
                podMetrics: [],
                error: false,
              } as VertexMetrics;
              let ratePerMin = 0.0,
                ratePerFiveMin = 0.0,
                ratePerFifteenMin = 0.0;
              // keeping processing rates as summation of pod values
              json.forEach((pod: any) => {
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
                  if (
                    vertexPods.has(vertex.name) &&
                    vertexPods.get(vertex.name) !== 0
                  ) {
                    vertexMetrics.error = true;
                    metricsErr.push({
                      error: `404: Failed to get metrics for ${vertex.name} vertex`,
                      options: {
                        toastId: `${vertex.name}-metrics`,
                        autoClose: 5000,
                      },
                    });
                  }
                }
              });
              vertexMetrics.ratePerMin = ratePerMin.toFixed(2);
              vertexMetrics.ratePerFiveMin = ratePerFiveMin.toFixed(2);
              vertexMetrics.ratePerFifteenMin = ratePerFifteenMin.toFixed(2);
              if (
                vertexPods.has(vertex.name) &&
                vertexPods.get(vertex.name) !== 0
              ) {
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
                options: {
                  toastId: `${result.reason.vertex}-metrics`,
                  autoClose: 5000,
                },
              });
            }
          });
          if (metricsErr.length > 0) setMetricsErr(metricsErr);
        })
        .then(() => setVertexMetrics(vertexToMetricsMap))
        .catch(console.error);
    }
  }, [spec, vertexPods]);

  // This useEffect is used to obtain metrics for a given vertex in a pipeline and refreshes every 1 minute
  useEffect(() => {
    getVertexMetrics();
    const interval = setInterval(() => {
      getVertexMetrics();
    }, 60000);
    return () => clearInterval(interval);
  }, [getVertexMetrics]);

  // This is used to obtain the watermark of a given pipeline
  const getPipelineWatermarks = useCallback(() => {
    const edgeToWatermarkMap = new Map();
    const watermarkErr: any[] = [];

    if (spec?.edges) {
      if (spec?.watermark?.disabled === true) {
        setEdgeWatermark(edgeToWatermarkMap);
      } else {
        Promise.allSettled([
          fetch(`${BASE_API}/watermarks`)
            .then((response) => {
              if (response.ok) {
                return response.json();
              } else {
                return Promise.reject(response);
              }
            })
            .then((json) => {
              json.forEach((edge: any) => {
                const edgeWatermark = {} as EdgeWatermark;
                edgeWatermark.isWaterMarkEnabled = edge["isWatermarkEnabled"];
                edgeWatermark.watermarks = edge["watermarks"];
                edgeWatermark.WMFetchTime = Date.now();
                edgeToWatermarkMap.set(edge.edge, edgeWatermark);
              });
            }),
        ])
          .then((results) => {
            results.forEach((result) => {
              if (result && result?.status === "rejected") {
                watermarkErr.push({
                  error: `${result.reason.status}: Failed to get watermarks for some vertices`,
                  options: { toastId: "vertex-watermarks", autoClose: 5000 },
                });
              }
            });
            if (watermarkErr.length > 0) setWatermarkErr(watermarkErr);
          })
          .then(() => setEdgeWatermark(edgeToWatermarkMap))
          .catch(console.error);
      }
    }
  }, [spec]);

  // This useEffect is used to obtain watermark for a given vertex in a pipeline and refreshes every 1 minute
  useEffect(() => {
    getPipelineWatermarks();
    const interval = setInterval(() => {
      getPipelineWatermarks();
    }, 60000);
    return () => clearInterval(interval);
  }, [getPipelineWatermarks]);

  const setPipelineDetails = useCallback(() => {
    const sourceVertices: string[] = [];
    spec?.vertices?.forEach((vertex) => {
      if (vertex?.source) {
        sourceVertices.push(vertex?.name);
      }
    });

    // directed graph is represented as an adjacency list
    const adjacencyList: { [key: string]: string[] } = {};
    if (spec?.vertices && spec?.edges) {
      spec?.edges?.forEach((edge) => {
        if (!adjacencyList[edge?.from]) {
          adjacencyList[edge?.from] = [];
        }

        adjacencyList[edge?.from].push(edge?.to);
      });

      const selfEdges: Set<string> = new Set();
      const backEdges: Set<string> = new Set();
      const forwardEdges: Set<string> = new Set();
      const selfVertices: Set<string> = new Set();
      const prevVertices: Set<string> = new Set();
      const nextVertices: Set<string> = new Set();
      const backEdgesHeight = new Map();

      const visited: Set<string> = new Set();
      const recStack: Set<string> = new Set();
      let height = 1;
      const dfs = (node: string) => {
        visited.add(node);
        recStack.add(node);
        adjacencyList[node]?.forEach((child: string) => {
          const id = `${node}-${child}`;
          if (node === child) {
            selfEdges.add(id);
            selfVertices.add(node);
            return;
          }
          if (recStack.has(child)) {
            backEdges.add(id);
            backEdgesHeight.set(id, height);
            height++;
            nextVertices.add(node);
            prevVertices.add(child);
            return;
          }
          if (!recStack.has(child)) {
            forwardEdges.add(id);
            if (!visited.has(child)) dfs(child);
          }
        });
        recStack.delete(node);
      };
      sourceVertices?.forEach((vertex: any) => {
        if (!visited.has(vertex)) {
          dfs(vertex);
        }
      });

      const nodeOutDegree: Map<string, number> = new Map();
      spec?.edges.forEach((edge) => {
        if (forwardEdges.has(`${edge?.from}-${edge?.to}`)) {
          nodeOutDegree.set(
            edge?.from,
            (nodeOutDegree.get(edge?.from) || 0) + 1
          );
        }
      });

      setSelfEdges(selfEdges);
      setBackEdges(backEdges);
      setFwdEdges(forwardEdges);
      setSelfVertices(selfVertices);
      setPrevVertices(prevVertices);
      setNextVertices(nextVertices);
      setBackEdgesHeight(backEdgesHeight);
      setNodeOutDegree(nodeOutDegree);
    }
  }, [spec]);

  // This useEffect is used to update edges and vertices types
  useEffect(() => {
    setPipelineDetails();
  }, [setPipelineDetails]);

  const vertices = useMemo(() => {
    const newVertices: Node[] = [];
    if (spec?.vertices && buffers && vertexPods && vertexMetrics) {
      spec.vertices.forEach((vertex: any) => {
        const newNode = {} as Node;
        newNode.id = vertex?.name;
        newNode.data = { name: vertex?.name };
        newNode.data.podnum = vertexPods.has(vertex?.name)
          ? vertexPods.get(vertex?.name)
          : 0;
        newNode.position = { x: 0, y: 0 };
        // change this in the future if you would like to make it draggable
        newNode.draggable = false;
        newNode.type = "custom";
        newNode.data.nodeInfo = vertex;
        if (vertex?.source) {
          newNode.data.type = "source";
        } else if (vertex?.sink) {
          newNode.data.type = "sink";
          newNode.data.test = vertex.name;
        } else {
          newNode.data.type = "udf";
        }
        newNode.data.vertexMetrics = vertexMetrics.has(vertex?.name)
          ? vertexMetrics.get(vertex?.name)
          : null;
        newNode.data.buffers = [];
        buffers?.forEach((buffer) => {
          const sidx = ns_pl.length;
          const eidx = buffer?.bufferName?.lastIndexOf("-");
          const bufferName = buffer?.bufferName?.substring(sidx, eidx);
          if (vertex?.name === bufferName) {
            newNode?.data?.buffers.push(buffer);
          }
        });
        if (newNode.data.buffers.length === 0) newNode.data.buffers = null;
        // added handles(connector points) for self loops and cycles
        newNode.data.centerSourceHandle = nextVertices.has(newNode.id);
        newNode.data.centerTargetHandle = prevVertices.has(newNode.id);
        newNode.data.quadHandle = selfVertices.has(newNode.id);
        newVertices.push(newNode);
      });
    }
    return newVertices;
  }, [
    spec,
    buffers,
    vertexPods,
    vertexMetrics,
    ns_pl,
    prevVertices,
    selfVertices,
    nextVertices,
  ]);

  const edges = useMemo(() => {
    const newEdges: Edge[] = [];
    if (spec?.edges && buffers && edgeWatermark) {
      // backpressure for a buffer is the count of total pending message
      // map from edge-id( from-Vertex - to-Vertex ) to sum of backpressure
      const edgeBackpressureLabel = new Map();
      const edgeIsFull = new Map();

      buffers.forEach((buffer) => {
        const sidx = ns_pl.length;
        const eidx = buffer?.bufferName?.lastIndexOf("-");
        const id = buffer?.bufferName?.substring(sidx, eidx);
        // condition check is similar for isFull so combining into one
        if (edgeBackpressureLabel.get(id) === undefined) {
          edgeBackpressureLabel.set(id, Number(buffer?.totalMessages));
          edgeIsFull.set(id, buffer?.isFull);
        } else {
          edgeBackpressureLabel.set(
            id,
            edgeBackpressureLabel.get(id) + Number(buffer?.totalMessages)
          );
          if (buffer?.isFull === true && buffer?.isFull !== edgeIsFull.get(id))
            edgeIsFull.set(id, buffer.isFull);
        }
      });

      spec.edges.forEach((edge: any) => {
        const id = edge?.from + "-" + edge?.to;
        const markerEnd = {
          type: MarkerType.Arrow,
          width: 15,
          height: 15,
          color: "black",
        };
        const pipelineEdge = {
          id,
          source: edge?.from,
          target: edge?.to,
          data: {
            conditions: edge?.conditions,
            backpressureLabel: edgeBackpressureLabel.get(edge?.to),
            isFull: edgeIsFull.get(edge?.to),
            source: edge?.from,
            target: edge?.to,
            fwdEdge: fwdEdges.has(id),
            backEdge: backEdges.has(id),
            selfEdge: selfEdges.has(id),
            backEdgeHeight: backEdgesHeight.get(id) || 0,
            fromNodeOutDegree: nodeOutDegree.get(edge?.from) || 0,
          },
        } as Edge;
        pipelineEdge.data.edgeWatermark = edgeWatermark.has(pipelineEdge.id)
          ? edgeWatermark.get(pipelineEdge.id)
          : null;
        pipelineEdge.animated = true;
        pipelineEdge.type = "custom";
        if (backEdges.has(id)) {
          pipelineEdge.sourceHandle = "1";
          pipelineEdge.targetHandle = "1";
          pipelineEdge.markerEnd = markerEnd;
        } else if (selfEdges.has(id)) {
          pipelineEdge.sourceHandle = "2";
          pipelineEdge.targetHandle = "2";
          pipelineEdge.markerEnd = markerEnd;
        } else if (fwdEdges.has(id)) {
          pipelineEdge.sourceHandle = "0";
          pipelineEdge.targetHandle = "0";
        }
        newEdges.push(pipelineEdge);
      });
    }
    return newEdges;
  }, [spec, buffers, edgeWatermark, ns_pl, backEdges, selfEdges, fwdEdges]);

  //sets loading variable
  useEffect(() => {
    if (
      pipeline &&
      buffers?.length > 0 &&
      vertices?.length > 0 &&
      edges?.length > 0
    ) {
      setLoading(false);
    }
  }, [pipeline, vertices, edges]);

  return {
    pipeline,
    vertices,
    edges,
    pipelineErr,
    buffersErr,
    podsErr,
    metricsErr,
    watermarkErr,
    loading,
  };
};
