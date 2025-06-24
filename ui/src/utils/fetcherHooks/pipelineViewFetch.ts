import { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { Edge, MarkerType, Node } from "@xyflow/react";
import { isEqual } from "lodash";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import {
  BufferInfo,
  EdgeWatermark,
  Pipeline,
  Spec,
  VertexMetrics,
} from "../../types/declarations/pipeline";

export const usePipelineViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  addError: (error: string) => void
) => {
  const [requestKey, setRequestKey] = useState("");
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
  const [generatorToColorIdxMap, setGeneratorToColorIdxMap] = useState<
    Map<string, string>
  >(new Map());
  const [pipelineErr, setPipelineErr] = useState<string | undefined>(undefined);
  const [buffersErr, setBuffersErr] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const { host } = useContext<AppContextProps>(AppContext);

  const BASE_API = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}`;

  const refresh = useCallback(() => {
    setRequestKey(`${Date.now()}`);
  }, []);

  // Call to get pipeline
  useEffect(() => {
    const fetchPipeline = async () => {
      try {
        const response = await fetch(`${BASE_API}?refreshKey=${requestKey}`);
        if (response.ok) {
          const json = await response.json();
          if (json?.data) {
            // Update pipeline state with data from the response
            setPipeline(json.data?.pipeline);
            // Update NS_PL state with metadata from the pipeline
            setNS_PL(
              `${json.data?.pipeline?.metadata?.namespace}-${json.data.pipeline?.metadata?.name}-`
            );
            // Update spec state if it is not equal to the spec from the response
            if (!isEqual(spec, json.data?.pipeline?.spec)) {
              setSpec(json.data.pipeline.spec);
            }
            setPipelineErr(undefined);
          } else if (json?.errMsg) {
            // pipeline API call returns an error message
            if (requestKey === "") {
              setPipelineErr(json.errMsg);
            } else {
              addError(json.errMsg);
            }
          }
        } else {
          // Handle the case when the response is not OK
          if (requestKey === "") {
            if (response.status === 403) {
              // Unauthorized user, display given or default error message
              const data = await response.json();
              if (data.errMsg) {
                setPipelineErr(`Error: ${data.errMsg}`);
              } else {
                setPipelineErr(
                  `Error: user is not authorized to execute the requested action.`
                );
              }
            } else {
              setPipelineErr(`Response code: ${response.status}`);
            }
          } else {
            addError(`Failed with code: ${response.status}`);
          }
        }
      } catch (e: any) {
        // Handle any errors that occur during the fetch request
        if (requestKey === "") {
          setPipelineErr(e.message);
        } else {
          addError(e.message);
        }
      }
    };

    fetchPipeline();
  }, [requestKey, addError]);

  // Call to get buffers
  useEffect(() => {
    const fetchBuffers = async () => {
      try {
        const response = await fetch(
          `${BASE_API}/isbs?refreshKey=${requestKey}`
        );
        if (response.ok) {
          const json = await response.json();
          if (json?.data) {
            // Update buffers state with data from the response
            setBuffers(json.data);
            setBuffersErr(undefined);
          } else if (json?.errMsg) {
            // Buffer API call returns an error message
            if (requestKey === "") {
              setBuffersErr(json.errMsg);
            } else {
              addError(json.errMsg);
            }
          }
        } else {
          // Handle the case when the response is not OK
          if (requestKey === "") {
            setBuffersErr(`Failed with code: ${response.status}`);
          } else {
            addError(`Failed with code: ${response.status}`);
          }
        }
      } catch (e: any) {
        // Handle any errors that occur during the fetch request
        if (requestKey === "") {
          setBuffersErr(e.message);
        } else {
          addError(e.message);
        }
      }
    };

    fetchBuffers();
  }, [requestKey, addError]);

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
    if (spec?.vertices) {
      // Fetch pods count for each vertex in parallel
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
              if (json?.data) {
                // Update vertexToPodsMap with the number of pods for the current vertex
                vertexToPodsMap.set(vertex.name, json.data.length);
              } else if (json?.errMsg) {
                // Pods API call returns an error message
                addError(json.errMsg);
              }
            });
        })
      )
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              // Handle rejected promises and add error messages to podsErr
              addError(`Failed to get pods: ${result.reason.response.status}`);
            }
          });
        })
        .then(() => {
          if (!isEqual(vertexPods, vertexToPodsMap)) {
            // Update vertexPods state if it is not equal to vertexToPodsMap
            setVertexPods(vertexToPodsMap);
          }
        })
        .catch((e: any) => {
          addError(`Error: ${e.message}`);
        });
    }
  }, [spec, requestKey, addError]);

  const getVertexMetrics = useCallback(() => {
    const vertexToMetricsMap = new Map();

    if (spec?.vertices && vertexPods.size > 0) {
      // Fetch metrics for all vertices together
      Promise.allSettled([
        fetch(`${BASE_API}/vertices/metrics`)
          .then((response) => {
            if (response.ok) {
              return response.json();
            } else {
              return Promise.reject(response);
            }
          })
          .then((json) => {
            if (json?.data) {
              const vertices = json.data;
              Object.values(vertices).forEach((vertex: any) => {
                const vertexName = vertex[0].vertex;
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
                // Calculate processing rates as summation of pod values
                vertex.forEach((pod: any) => {
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
                      vertexPods.has(vertexName) &&
                      vertexPods.get(vertexName) !== 0
                    ) {
                      // Handle case when processingRates are not available for a vertex
                      vertexMetrics.error = true;
                      addError(
                        `Failed to get metrics for ${vertexName} vertex`
                      );
                    }
                  }
                });
                vertexMetrics.ratePerMin = ratePerMin.toFixed(2);
                vertexMetrics.ratePerFiveMin = ratePerFiveMin.toFixed(2);
                vertexMetrics.ratePerFifteenMin = ratePerFifteenMin.toFixed(2);
                if (
                  vertexPods.has(vertexName) &&
                  vertexPods.get(vertexName) !== 0
                ) {
                  vertexMetrics.podMetrics = json;
                }
                vertexToMetricsMap.set(vertexName, vertexMetrics);
              });
            } else if (json?.errMsg) {
              // Metrics API call returns an error message
              addError(json.errMsg);
            }
          }),
      ])
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              // Handle rejected promises and add error messages to metricsErr
              addError(
                `Failed to get metrics: ${result.reason.response.status}`
              );
            }
          });
        })
        .then(() => setVertexMetrics(vertexToMetricsMap))
        .catch((e: any) => {
          addError(`Error: ${e.message}`);
        });
    }
  }, [spec, vertexPods, addError]);

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

    if (spec?.edges) {
      if (spec?.watermark?.disabled === true) {
        // Set edgeWatermark to empty map if watermark is disabled
        setEdgeWatermark(edgeToWatermarkMap);
      } else {
        // Fetch watermarks for each edge together
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
              if (json?.data) {
                json.data.forEach((edge: any) => {
                  const edgeWatermark = {} as EdgeWatermark;
                  edgeWatermark.isWaterMarkEnabled = edge["isWatermarkEnabled"];
                  edgeWatermark.watermarks = edge["watermarks"];
                  edgeWatermark.WMFetchTime = Date.now();
                  edgeToWatermarkMap.set(edge.edge, edgeWatermark);
                });
              } else if (json?.errMsg) {
                // Watermarks API call returns an error message
                addError(json.errMsg);
              }
            }),
        ])
          .then((results) => {
            results.forEach((result) => {
              if (result && result?.status === "rejected") {
                // Handle rejected promises and add error messages to watermarkErr
                addError(`Failed to get watermarks: ${result.reason.status}`);
              }
            });
          })
          .then(() => setEdgeWatermark(edgeToWatermarkMap))
          .catch((e: any) => {
            addError(`Error: ${e.message}`);
          });
      }
    }
  }, [spec, addError]);

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
        const newNode = {} as Node<Record<string, any>>;
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
    //creating side input nodes && wrapper generator node
    const generatorToColorIdx = new Map();
    if (spec?.sideInputs && spec.sideInputs.length) {
      const newNode = {} as Node;
      newNode.id = "generator";
      newNode.data = {
        sideInputCount: spec.sideInputs.length,
      };
      newNode.position = { x: 0, y: 0 };
      newNode.draggable = false;
      newNode.type = "custom";
      newNode.data.type = "generator";
      newVertices.push(newNode);
      spec.sideInputs.forEach((sideInput, idx) => {
        const newNode = {} as Node;
        newNode.id = sideInput?.name;
        newNode.data = { name: sideInput?.name };
        newNode.position = { x: 0, y: 0 };
        newNode.draggable = false;
        newNode.type = "custom";
        newNode.data.nodeInfo = sideInput;
        newNode.data.type = "sideInput";
        newNode.data.sideHandle = true;
        newVertices.push(newNode);
        generatorToColorIdx.set(sideInput?.name, `${idx % 5}`);
      });
      setGeneratorToColorIdxMap(generatorToColorIdx);
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
      // backpressure for a buffer is the count of pending messages
      // map from edge-id( from-Vertex - to-Vertex ) to sum of pending and ackPending counts
      const edgePendingLabel = new Map();
      const edgeAckPendingLabel = new Map();
      const edgeIsFull = new Map();

      buffers.forEach((buffer) => {
        const sidx = ns_pl.length;
        const eidx = buffer?.bufferName?.lastIndexOf("-");
        const id = buffer?.bufferName?.substring(sidx, eidx);
        
        // Initialize or accumulate pending count
        if (edgePendingLabel.get(id) === undefined) {
          const pendingCount = Number(buffer?.pendingCount);
          const ackPendingCount = Number(buffer?.ackPendingCount);
          edgePendingLabel.set(id, isNaN(pendingCount) ? 0 : pendingCount);
          edgeAckPendingLabel.set(id, isNaN(ackPendingCount) ? 0 : ackPendingCount);
          edgeIsFull.set(id, buffer?.isFull);
        } else {
          const pendingCount = Number(buffer?.pendingCount);
          const ackPendingCount = Number(buffer?.ackPendingCount);
          edgePendingLabel.set(
            id,
            edgePendingLabel.get(id) + (isNaN(pendingCount) ? 0 : pendingCount)
          );
          edgeAckPendingLabel.set(
            id,
            edgeAckPendingLabel.get(id) + (isNaN(ackPendingCount) ? 0 : ackPendingCount)
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
          color: edgeIsFull.get(edge?.to) ? "#DB334D" : "#8D9096",
        };
        const pipelineEdge = {
          id,
          source: edge?.from,
          target: edge?.to,
          data: {
            conditions: edge?.conditions,
            pendingLabel: edgePendingLabel.get(edge?.to),
            ackPendingLabel: edgeAckPendingLabel.get(edge?.to),
            isFull: edgeIsFull.get(edge?.to),
            source: edge?.from,
            target: edge?.to,
            fwdEdge: fwdEdges.has(id),
            backEdge: backEdges.has(id),
            selfEdge: selfEdges.has(id),
            backEdgeHeight: backEdgesHeight.get(id) || 0,
            fromNodeOutDegree: nodeOutDegree.get(edge?.from) || 0,
            edgeWatermark: edgeWatermark.has(id)
              ? edgeWatermark.get(id)
              : null,
          },
        } as Edge;
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
          pipelineEdge.markerEnd = markerEnd;
        }
        newEdges.push(pipelineEdge);
      });
    }
    //creating side input edges
    if (spec?.sideInputs && spec?.vertices) {
      const generatorToVertexMap: { [key: string]: string[] } = {};
      const vertexToHandleMap: { [key: string]: string } = {};

      spec.vertices.forEach((vertex) => {
        if (vertex?.sideInputs) {
          vertex?.sideInputs.forEach((sideInput) => {
            if (!generatorToVertexMap[sideInput]) {
              generatorToVertexMap[sideInput] = [];
            }
            generatorToVertexMap[sideInput].push(vertex?.name);
          });
        }
      });

      spec.sideInputs.forEach((sideInput) => {
        generatorToVertexMap[sideInput?.name]?.forEach((vertex) => {
          const id = `${sideInput.name}-${vertex}`;
          const pipelineEdge = {
            id,
            source: sideInput.name,
            target: vertex,
            data: {
              source: sideInput.name,
              target: vertex,
              sideInputEdge: true,
            },
          } as Edge;
          pipelineEdge.animated = true;
          pipelineEdge.type = "custom";
          pipelineEdge.sourceHandle = "2";
          if (vertex in vertexToHandleMap) {
            const handleID = vertexToHandleMap[vertex];
            const idSplit = handleID.split("-");
            vertexToHandleMap[vertex] = "3-" + (Number(idSplit[1]) + 1);
          } else {
            vertexToHandleMap[vertex] = "3-0";
          }
          pipelineEdge.targetHandle = vertexToHandleMap[vertex];
          newEdges.push(pipelineEdge);
        });
      });
    }
    return newEdges;
  }, [
    spec,
    buffers,
    edgeWatermark,
    ns_pl,
    backEdges,
    selfEdges,
    fwdEdges,
    backEdgesHeight,
    nodeOutDegree,
  ]);

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
    generatorToColorIdxMap,
    pipelineErr,
    buffersErr,
    loading,
    refresh,
  };
};
