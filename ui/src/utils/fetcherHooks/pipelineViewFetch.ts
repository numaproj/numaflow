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
            if (!isEqual(spec, json.data?.pipeline?.spec))
              setSpec(json.data?.pipeline?.spec);
          } else if (json?.errMsg) {
            // pipeline API call returns an error message
            setPipelineErr([
              {
                error: json.errMsg,
                options: { toastId: "pipeline-fetch-error", autoClose: 5000 },
              },
            ]);
          }
        } else {
          // Handle the case when the response is not OK
          setPipelineErr([
            {
              error: "Failed to fetch the pipeline details",
              options: { toastId: "pipeline-fetch", autoClose: 5000 },
            },
          ]);
        }
      } catch {
        // Handle any errors that occur during the fetch request
        setPipelineErr([
          {
            error: "Failed to fetch the pipeline details",
            options: { toastId: "pipeline-fetch", autoClose: 5000 },
          },
        ]);
      }
    };

    fetchPipeline();
  }, [requestKey]);

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
          } else if (json?.errMsg) {
            // Buffer API call returns an error message
            setBuffersErr([
              {
                error: json.errMsg,
                options: { toastId: "isb-fetch-error", autoClose: 5000 },
              },
            ]);
          }
        } else {
          // Handle the case when the response is not OK
          setBuffersErr([
            {
              error: "Failed to fetch the pipeline buffers",
              options: { toastId: "isb-fetch", autoClose: 5000 },
            },
          ]);
        }
      } catch {
        // Handle any errors that occur during the fetch request
        setBuffersErr([
          {
            error: "Failed to fetch the pipeline buffers",
            options: { toastId: "isb-fetch", autoClose: 5000 },
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
                podsErr.push({
                  error: json.errMsg,
                  options: {
                    toastId: `${vertex.name}-pods-fetch-error`,
                    autoClose: 5000,
                  },
                });
              }
            });
        })
      )
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              // Handle rejected promises and add error messages to podsErr
              podsErr.push({
                error: `${result.reason.response.status}: Failed to get pods count for ${result.reason.vertex} vertex`,
                options: {
                  toastId: `${result.reason.vertex}-pods-fetch`,
                  autoClose: 5000,
                },
              });
            }
          });
          if (podsErr.length > 0) {
            // Update podsErr state if there are any errors
            setPodsErr(podsErr);
          }
        })
        .then(() => {
          if (!isEqual(vertexPods, vertexToPodsMap)) {
            // Update vertexPods state if it is not equal to vertexToPodsMap
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
                      metricsErr.push({
                        error: `Failed to get metrics for ${vertexName} vertex`,
                        options: {
                          toastId: `${vertexName}-metrics`,
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
                  vertexPods.has(vertexName) &&
                  vertexPods.get(vertexName) !== 0
                ) {
                  vertexMetrics.podMetrics = json;
                }
                vertexToMetricsMap.set(vertexName, vertexMetrics);
              });
            } else if (json?.errMsg) {
              // Metrics API call returns an error message
              metricsErr.push({
                error: json.errMsg,
                options: {
                  toastId: "vertex-metrics-fetch-error",
                  autoClose: 5000,
                },
              });
            }
          }),
      ])
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              // Handle rejected promises and add error messages to metricsErr
              metricsErr.push({
                error: `${result.reason.response.status}: Failed to get metrics for some vertices`,
                options: { toastId: `vertex-metrics-fetch`, autoClose: 5000 },
              });
            }
          });
          if (metricsErr.length > 0) {
            // Update metricsErr state if there are any errors
            setMetricsErr(metricsErr);
          }
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
                watermarkErr.push({
                  error: json.errMsg,
                  options: {
                    toastId: "watermarks-fetch-error",
                    autoClose: 5000,
                  },
                });
              }
            }),
        ])
          .then((results) => {
            results.forEach((result) => {
              if (result && result?.status === "rejected") {
                // Handle rejected promises and add error messages to watermarkErr
                watermarkErr.push({
                  error: `${result.reason.status}: Failed to get watermarks for some vertices`,
                  options: { toastId: "watermarks-fetch", autoClose: 5000 },
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
    //creating side input nodes
    if (spec?.sideInputs) {
      spec.sideInputs.forEach((sideInput) => {
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
          color: edgeIsFull.get(edge?.to) ? "#DB334D" : "#8D9096",
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
    pipelineErr,
    buffersErr,
    podsErr,
    metricsErr,
    watermarkErr,
    loading,
  };
};
