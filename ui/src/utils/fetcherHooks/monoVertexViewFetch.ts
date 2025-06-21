import { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { Node } from "@xyflow/react";
import { isEqual } from "lodash";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import {
  MonoVertex,
  MonoVertexSpec,
  MonoVertexMetrics,
} from "../../types/declarations/pipeline";

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
    // if (spec?.vertices && vertexPods && vertexMetrics) {
    if (spec?.source && spec?.sink && monoVertexMetrics) {
      const newNode = {} as Node;
      const name = pipelineId ?? "";
      newNode.id = name;
      newNode.data = { name: name };
      newNode.data.podnum = replicas ? replicas : 0;
      newNode.position = { x: 0, y: 0 };
      // change this in the future if you would like to make it draggable
      newNode.draggable = false;
      newNode.type = "custom";
      newNode.data.nodeInfo = spec;
      newNode.data.type = "monoVertex";
      newNode.data.vertexMetrics = null;
      newNode.data.vertexMetrics = monoVertexMetrics.has(name)
        ? monoVertexMetrics.get(name)
        : null;
      newVertices.push(newNode);
    }
    return newVertices;
  }, [spec, monoVertexMetrics]);

  //sets loading variable
  useEffect(() => {
    if (pipeline && vertices?.length > 0) {
      setLoading(false);
    }
  }, [pipeline, vertices, replicas]);

  return {
    pipeline,
    vertices,
    pipelineErr,
    loading,
    refresh,
  };
};
