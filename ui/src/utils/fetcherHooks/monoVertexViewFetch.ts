import { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { Edge, MarkerType, Node } from "@xyflow/react";
import { isEqual } from "lodash";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import {
  MonoVertex,
  MonoVertexSpec,
  MonoVertexMetrics,
} from "../../types/declarations/pipeline";
import {
  MONO_VERTEX_BYPASS_TARGETS,
  getMonoVertexBypassSourceStages,
  getMonoVertexContainerDimensions,
  getMonoVertexDirectInternalEdges,
  getMonoVertexInternalEdgeKey,
  getMonoVertexInternalStages,
  getMonoVertexMainStages,
  getMonoVertexStageNodeName,
  shouldFanOutMonoVertexSinkTargets,
} from "../monoVertexGraphLayout";

export {
  getMonoVertexContainerDimensions,
  getMonoVertexInternalStages,
} from "../monoVertexGraphLayout";

export const useMonoVertexViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  addError: (error: string) => void
) => {
  const [requestKey, setRequestKey] = useState("");
  const [pipeline, setPipeline] = useState<MonoVertex | undefined>(undefined);
  const [spec, setSpec] = useState<MonoVertexSpec | undefined>(undefined);
  const [replicas, setReplicas] = useState<number | undefined>(undefined);
  const [monoVertexPods, setMonoVertexPods] = useState<Map<string, number>>(
    new Map()
  );
  const [monoVertexMetrics, setMonoVertexMetrics] = useState<
    Map<string, MonoVertexMetrics>
  >(new Map());
  const [pipelineErr, setPipelineErr] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const { host } = useContext<AppContextProps>(AppContext);

  const BASE_API = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/mono-vertices/${pipelineId}`;

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
            setPipeline(json.data?.monoVertex);
            // Update spec state if it is not equal to the spec from the response
            if (!isEqual(spec, json.data?.monoVertex?.spec)) {
              setSpec(json.data.monoVertex.spec);
            }
            if (replicas !== json.data?.monoVertex?.status?.replicas) {
              setReplicas(json.data.monoVertex.status.replicas);
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

  // Refresh pipeline every 30 sec
  useEffect(() => {
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  // This useEffect is used to obtain all the pods for a given monoVertex.
  useEffect(() => {
    const vertexToPodsMap = new Map();
    if (spec?.source && spec?.sink) {
      // Fetch pods count for each vertex in parallel
      Promise.allSettled([
        fetch(`${BASE_API}/pods`)
          .then((response) => {
            if (response.ok) {
              return response.json();
            } else {
              return Promise.reject({ response, vertex: pipelineId });
            }
          })
          .then((json) => {
            if (json?.data) {
              const mvtxPods = json.data.filter(
                (mvtx: any) => !mvtx?.metadata?.name.includes("-daemon-")
              );
              // Update vertexToPodsMap with the number of pods for the current vertex
              vertexToPodsMap.set(pipelineId, mvtxPods?.length);
            } else if (json?.errMsg) {
              // Pods API call returns an error message
              addError(json.errMsg);
            }
          }),
      ])
        .then((results) => {
          results.forEach((result) => {
            if (result && result?.status === "rejected") {
              // Handle rejected promises and add error messages to podsErr
              addError(`Failed to get pods: ${result.reason.response.status}`);
            }
          });
        })
        .then(() => {
          if (!isEqual(monoVertexPods, vertexToPodsMap)) {
            // Update vertexPods state if it is not equal to vertexToPodsMap
            setMonoVertexPods(vertexToPodsMap);
          }
        })
        .catch((e: any) => {
          addError(`Error: ${e.message}`);
        });
    }
  }, [spec, requestKey, addError]);

  const getVertexMetrics = useCallback(() => {
    const vertexToMetricsMap = new Map();

    if (spec?.source && spec?.sink && monoVertexPods.size > 0) {
      // Fetch metrics for monoVertex
      Promise.allSettled([
        fetch(`${BASE_API}/metrics`)
          .then((response) => {
            if (response.ok) {
              return response.json();
            } else {
              return Promise.reject(response);
            }
          })
          .then((json) => {
            if (json?.data) {
              const mvtx = json.data;
              const monoVertexName = mvtx.monoVertex;
              const monoVertexMetrics: MonoVertexMetrics = {
                ratePerMin: "0.00",
                ratePerFiveMin: "0.00",
                ratePerFifteenMin: "0.00",
                podMetrics: [],
                error: false,
              };
              let ratePerMin = 0.0,
                ratePerFiveMin = 0.0,
                ratePerFifteenMin = 0.0;
              // Calculate processing rates as summation of pod values
              if ("processingRates" in mvtx) {
                if ("1m" in mvtx["processingRates"]) {
                  ratePerMin += mvtx["processingRates"]["1m"];
                }
                if ("5m" in mvtx["processingRates"]) {
                  ratePerFiveMin += mvtx["processingRates"]["5m"];
                }
                if ("15m" in mvtx["processingRates"]) {
                  ratePerFifteenMin += mvtx["processingRates"]["15m"];
                }
              } else {
                if (
                  monoVertexPods.has(monoVertexName) &&
                  monoVertexPods.get(monoVertexName) !== 0
                ) {
                  // Handle case when processingRates are not available for a vertex
                  monoVertexMetrics.error = true;
                  addError(
                    `Failed to get metrics for ${monoVertexName} monoVertex`
                  );
                }
              }
              monoVertexMetrics.ratePerMin = ratePerMin.toFixed(2);
              monoVertexMetrics.ratePerFiveMin = ratePerFiveMin.toFixed(2);
              monoVertexMetrics.ratePerFifteenMin =
                ratePerFifteenMin.toFixed(2);
              if (
                monoVertexPods.has(monoVertexName) &&
                monoVertexPods.get(monoVertexName) !== 0
              ) {
                monoVertexMetrics.podMetrics = json;
              }
              vertexToMetricsMap.set(monoVertexName, monoVertexMetrics);
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
        .then(() => setMonoVertexMetrics(vertexToMetricsMap))
        .catch((e: any) => {
          addError(`Error: ${e.message}`);
        });
    }
  }, [spec, monoVertexPods, addError]);

  // This useEffect is used to obtain metrics for a given monoVertex and refreshes every 1 minute
  useEffect(() => {
    getVertexMetrics();
    const interval = setInterval(() => {
      getVertexMetrics();
    }, 60000);
    return () => clearInterval(interval);
  }, [getVertexMetrics]);

  const vertices = useMemo(() => {
    const newVertices: Node[] = [];
    if (spec?.source && spec?.sink && monoVertexMetrics) {
      const name = pipelineId ?? "";
      const { width, height } = getMonoVertexContainerDimensions(spec);
      const newNode = {} as Node;
      newNode.id = name;
      newNode.data = { name: name };
      newNode.data.podnum = replicas ? replicas : 0;
      newNode.position = { x: 0, y: 0 };
      newNode.draggable = false;
      newNode.type = "custom";
      newNode.data.nodeInfo = spec;
      newNode.data.type = "monoVertex";
      newNode.data.containerWidth = width;
      newNode.data.containerHeight = height;
      newNode.data.vertexMetrics = monoVertexMetrics.has(name)
        ? monoVertexMetrics.get(name)
        : null;
      newNode.selectable = false;
      newVertices.push(newNode);

      const internalStages = getMonoVertexInternalStages(spec);
      const bypassSourceStages = getMonoVertexBypassSourceStages(spec);
      const directInternalEdges = getMonoVertexDirectInternalEdges(spec, name);
      const bypassTargetStages = MONO_VERTEX_BYPASS_TARGETS.filter(
        (targetStage) => {
          const rule = spec?.bypass?.[targetStage];
          const targetExists =
            targetStage === "sink" ||
            (targetStage === "onSuccess" && spec?.sink?.onSuccess) ||
            (targetStage === "fallback" && spec?.sink?.fallback);
          return !!rule?.tags && targetExists;
        }
      );
      const bypassTargetsByStage = bypassSourceStages.reduce(
        (targetsByStage, sourceStage) => {
          targetsByStage[sourceStage] = bypassTargetStages
            .filter((targetStage) => {
              const source = getMonoVertexStageNodeName(name, sourceStage);
              const target = getMonoVertexStageNodeName(name, targetStage);
              return !directInternalEdges.has(
                getMonoVertexInternalEdgeKey(source, target)
              );
            })
            .map((targetStage) => {
              const source = getMonoVertexStageNodeName(name, sourceStage);
              const target = getMonoVertexStageNodeName(name, targetStage);
              const rule = spec?.bypass?.[targetStage];
              return {
                id: `${source}-${target}-bypass`,
                target: targetStage,
                source: sourceStage,
                sourceNodeId: source,
                targetNodeId: target,
                bypassTargetFanOut: shouldFanOutMonoVertexSinkTargets(spec),
                operator: rule?.tags?.operator || "or",
                values: Array.isArray(rule?.tags?.values)
                  ? rule?.tags?.values
                  : [],
              };
            });
          return targetsByStage;
        },
        {} as Record<string, any[]>
      );
      internalStages.forEach((stage) => {
        const internalNode = {} as Node;
        internalNode.id = `${name}-${stage.key}`;
        internalNode.parentId = name;
        internalNode.extent = "parent";
        internalNode.data = {
          name: `${name}-${stage.key}`,
          nodeInfo: stage.spec,
          type: "monoVertexInternal",
          monoVertexStage: stage.key,
          bypassTargets: bypassTargetsByStage[stage.key] || [],
        };
        internalNode.position = { x: stage.x, y: stage.y };
        internalNode.draggable = false;
        internalNode.selectable = false;
        internalNode.type = "custom";
        newVertices.push(internalNode);
      });
    }
    return newVertices;
  }, [spec, monoVertexMetrics, pipelineId, replicas]);

  const edges = useMemo(() => {
    const newEdges: Edge[] = [];
    if (spec?.source && spec?.sink) {
      const name = pipelineId ?? "";
      const internalMarkerEnd = {
        type: MarkerType.Arrow,
        width: 10,
        height: 10,
        color: "#8D9096",
      };
      const bypassMarkerEnd = {
        type: MarkerType.Arrow,
        width: 8,
        height: 8,
        color: "var(--mono-vertex-bypass-color)",
      };
      const mainStages = getMonoVertexMainStages(spec);
      const directInternalEdges = new Set<string>();
      const pushInternalEdge = (source: string, target: string) => {
        directInternalEdges.add(getMonoVertexInternalEdgeKey(source, target));
        newEdges.push({
          id: `${source}-${target}`,
          source,
          target,
          sourceHandle: "out",
          targetHandle: "in",
          type: "custom",
          animated: false,
          zIndex: 3,
          markerEnd: internalMarkerEnd,
          data: {
            source,
            target,
            monoVertexInternalEdge: true,
          },
        } as Edge);
      };
      mainStages.forEach((stage, idx) => {
        if (idx === mainStages.length - 1) return;
        pushInternalEdge(`${name}-${stage}`, `${name}-${mainStages[idx + 1]}`);
      });
      if (spec?.sink?.onSuccess) {
        pushInternalEdge(`${name}-sink`, `${name}-onSuccess`);
      }
      if (spec?.sink?.fallback) {
        pushInternalEdge(`${name}-sink`, `${name}-fallback`);
      }
      const bypassSourceStages = getMonoVertexBypassSourceStages(spec);
      bypassSourceStages.forEach((sourceStage) => {
        MONO_VERTEX_BYPASS_TARGETS.forEach((targetStage) => {
          const rule = spec?.bypass?.[targetStage];
          const targetExists =
            targetStage === "sink" ||
            (targetStage === "onSuccess" && spec?.sink?.onSuccess) ||
            (targetStage === "fallback" && spec?.sink?.fallback);
          if (!rule?.tags || !targetExists) return;
          const bypassSource = `${name}-${sourceStage}`;
          const target = `${name}-${targetStage}`;
          if (
            directInternalEdges.has(
              getMonoVertexInternalEdgeKey(bypassSource, target)
            )
          )
            return;
          newEdges.push({
            id: `${bypassSource}-${target}-bypass`,
            source: bypassSource,
            target,
            sourceHandle: "bypass",
            targetHandle: "bypass",
            type: "custom",
            animated: true,
            zIndex: 4,
            markerEnd: bypassMarkerEnd,
            data: {
              source: bypassSource,
              target,
              monoVertexBypassEdge: true,
              bypassSourceStage: sourceStage,
              bypassTarget: targetStage,
              bypassTargetFanOut: shouldFanOutMonoVertexSinkTargets(spec),
              operator: rule?.tags?.operator || "or",
              values: Array.isArray(rule?.tags?.values) ? rule.tags.values : [],
            },
          } as Edge);
        });
      });
    }
    return newEdges;
  }, [spec, pipelineId]);

  //sets loading variable
  useEffect(() => {
    if (pipeline && vertices?.length > 0) {
      setLoading(false);
    }
  }, [pipeline, vertices, replicas]);

  return {
    pipeline,
    vertices,
    edges,
    pipelineErr,
    loading,
    refresh,
  };
};

