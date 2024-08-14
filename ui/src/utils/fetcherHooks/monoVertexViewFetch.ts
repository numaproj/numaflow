import { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { Node } from "reactflow";
import { isEqual } from "lodash";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import {
  MonoVertex,
  MonoVertexSpec,
  // VertexMetrics,
} from "../../types/declarations/pipeline";

export const useMonoVertexViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  addError: (error: string) => void
) => {
  const [requestKey, setRequestKey] = useState("");
  const [pipeline, setPipeline] = useState<MonoVertex | undefined>(undefined);
  const [spec, setSpec] = useState<MonoVertexSpec | undefined>(undefined);
  // const [vertexMetrics, setVertexMetrics] = useState<
  //   Map<string, VertexMetrics>
  // >(new Map());
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
            if (!isEqual(spec, json.data)) setSpec(json.data?.monoVertex?.spec);
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

  // const getVertexMetrics = useCallback(() => {
  //   const vertexToMetricsMap = new Map();
  //
  //   if (spec?.vertices && vertexPods.size > 0) {
  //     // Fetch metrics for all vertices together
  //     Promise.allSettled([
  //       fetch(`${BASE_API}/vertices/metrics`)
  //         .then((response) => {
  //           if (response.ok) {
  //             return response.json();
  //           } else {
  //             return Promise.reject(response);
  //           }
  //         })
  //         .then((json) => {
  //           if (json?.data) {
  //             const vertices = json.data;
  //             Object.values(vertices).forEach((vertex: any) => {
  //               const vertexName = vertex[0].vertex;
  //               const vertexMetrics = {
  //                 ratePerMin: "0.00",
  //                 ratePerFiveMin: "0.00",
  //                 ratePerFifteenMin: "0.00",
  //                 podMetrics: [],
  //                 error: false,
  //               } as VertexMetrics;
  //               let ratePerMin = 0.0,
  //                 ratePerFiveMin = 0.0,
  //                 ratePerFifteenMin = 0.0;
  //               // Calculate processing rates as summation of pod values
  //               vertex.forEach((pod: any) => {
  //                 if ("processingRates" in pod) {
  //                   if ("1m" in pod["processingRates"]) {
  //                     ratePerMin += pod["processingRates"]["1m"];
  //                   }
  //                   if ("5m" in pod["processingRates"]) {
  //                     ratePerFiveMin += pod["processingRates"]["5m"];
  //                   }
  //                   if ("15m" in pod["processingRates"]) {
  //                     ratePerFifteenMin += pod["processingRates"]["15m"];
  //                   }
  //                 } else {
  //                   if (
  //                     vertexPods.has(vertexName) &&
  //                     vertexPods.get(vertexName) !== 0
  //                   ) {
  //                     // Handle case when processingRates are not available for a vertex
  //                     vertexMetrics.error = true;
  //                     addError(
  //                       `Failed to get metrics for ${vertexName} vertex`
  //                     );
  //                   }
  //                 }
  //               });
  //               vertexMetrics.ratePerMin = ratePerMin.toFixed(2);
  //               vertexMetrics.ratePerFiveMin = ratePerFiveMin.toFixed(2);
  //               vertexMetrics.ratePerFifteenMin = ratePerFifteenMin.toFixed(2);
  //               if (
  //                 vertexPods.has(vertexName) &&
  //                 vertexPods.get(vertexName) !== 0
  //               ) {
  //                 vertexMetrics.podMetrics = json;
  //               }
  //               vertexToMetricsMap.set(vertexName, vertexMetrics);
  //             });
  //           } else if (json?.errMsg) {
  //             // Metrics API call returns an error message
  //             addError(json.errMsg);
  //           }
  //         }),
  //     ])
  //       .then((results) => {
  //         results.forEach((result) => {
  //           if (result && result?.status === "rejected") {
  //             // Handle rejected promises and add error messages to metricsErr
  //             addError(
  //               `Failed to get metrics: ${result.reason.response.status}`
  //             );
  //           }
  //         });
  //       })
  //       .then(() => setVertexMetrics(vertexToMetricsMap))
  //       .catch((e: any) => {
  //         addError(`Error: ${e.message}`);
  //       });
  //   }
  // }, [spec, vertexPods, addError]);
  //
  // // This useEffect is used to obtain metrics for a given vertex in a pipeline and refreshes every 1 minute
  // useEffect(() => {
  //   getVertexMetrics();
  //   const interval = setInterval(() => {
  //     getVertexMetrics();
  //   }, 60000);
  //   return () => clearInterval(interval);
  // }, [getVertexMetrics]);

  const vertices = useMemo(() => {
    const newVertices: Node[] = [];
    // if (spec?.vertices && vertexPods && vertexMetrics) {
    if (spec?.source && spec?.sink) {
      const newNode = {} as Node;
      newNode.id = pipelineId ?? "";
      newNode.data = { name: pipelineId ?? "" };
      newNode.data.podnum = spec?.replicas ? spec.replicas : 0;
      newNode.position = { x: 0, y: 0 };
      // change this in the future if you would like to make it draggable
      newNode.draggable = false;
      newNode.type = "custom";
      newNode.data.nodeInfo = spec;
      newNode.data.type = "monoVertex";
      newNode.data.vertexMetrics = null;
      // newNode.data.vertexMetrics = vertexMetrics.has(vertex?.name)
      //   ? vertexMetrics.get(vertex?.name)
      //   : null;
      newVertices.push(newNode);
    }
    return newVertices;
  }, [spec]);
  // }, [spec, vertexMetrics]);

  //sets loading variable
  useEffect(() => {
    if (pipeline && vertices?.length > 0) {
      setLoading(false);
    }
  }, [pipeline, vertices]);

  return {
    pipeline,
    vertices,
    pipelineErr,
    loading,
    refresh,
  };
};
