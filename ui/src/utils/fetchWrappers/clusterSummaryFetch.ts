import { useEffect, useState } from "react";
import { useFetch, Options } from "./fetch";
import {
  ClusterNamespaceSummary,
  ClusterSummaryData,
  ClusterSummaryFetchProps,
  ClusterSummaryFetchResult,
} from "../../types/declarations/cluster";

// const MOCK_DATA = [
//   {
//     namespace: "my-ns",
//     pipelineSummary: {
//       active: {
//         Healthy: 1,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 2,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 2,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 2,
//     },
//   },
//   {
//     namespace: "my-other-ns",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns1",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns2",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns3",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns4",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns5",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns6",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns7",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
//   {
//     namespace: "my-other-ns8",
//     pipelineSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 1,
//         Critical: 1,
//       },
//       inactive: 1,
//     },
//     isbServiceSummary: {
//       active: {
//         Healthy: 5,
//         Warning: 2,
//         Critical: 2,
//       },
//       inactive: 1,
//     },
//   },
// ];

const rawDataToClusterSummary = (
  rawData: any[]
): ClusterSummaryData | undefined => {
  if (!rawData || !Array.isArray(rawData)) {
    return undefined;
  }
  const namespacesCount = rawData.length;
  let pipelinesCount = 0;
  let pipelinesActiveCount = 0;
  let pipelinesInactiveCount = 0;
  let pipelinesHealthyCount = 0;
  let pipelinesWarningCount = 0;
  let pipelinesCriticalCount = 0;
  let isbsCount = 0;
  let isbsActiveCount = 0;
  let isbsInactiveCount = 0;
  let isbsHealthyCount = 0;
  let isbsWarningCount = 0;
  let isbsCriticalCount = 0;
  const nameSpaceSummaries: ClusterNamespaceSummary[] = [];

  rawData.forEach((ns: any) => {
    // Pipeline counts
    const nsPipelinesHealthyCount = ns.pipelineSummary?.active?.Healthy || 0;
    const nsPipelinesWarningCount = ns.pipelineSummary?.active?.Warning || 0;
    const nsPipelinesCriticalCount = ns.pipelineSummary?.active?.Critical || 0;
    const nsPipelinesActiveCount =
      nsPipelinesHealthyCount +
      nsPipelinesWarningCount +
      nsPipelinesCriticalCount;
    const nsPipelinesInactiveCount = ns.pipelineSummary?.inactive || 0;
    const nsPipelinesCount = nsPipelinesActiveCount + nsPipelinesInactiveCount;
    // ISB counts
    const nsIsbsHealthyCount = ns.isbServiceSummary?.active?.Healthy || 0;
    const nsIsbsWarningCount = ns.isbServiceSummary?.active?.Warning || 0;
    const nsIsbsCriticalCount = ns.isbServiceSummary?.active?.Critical || 0;
    const nsIsbsActiveCount =
      nsIsbsHealthyCount + nsIsbsWarningCount + nsIsbsCriticalCount;
    const nsIsbsInactiveCount = ns.isbServiceSummary?.inactive || 0;
    const nsIsbsCount = nsIsbsActiveCount + nsIsbsInactiveCount;
    // Add ns summary to array
    nameSpaceSummaries.push({
      name: ns.namespace || DEFAULT_NS_NAME,
      pipelinesCount: nsPipelinesCount,
      pipelinesActiveCount: nsPipelinesActiveCount,
      pipelinesInactiveCount: nsPipelinesInactiveCount,
      pipelinesHealthyCount: nsPipelinesHealthyCount,
      pipelinesWarningCount: nsPipelinesWarningCount,
      pipelinesCriticalCount: nsPipelinesCriticalCount,
      isbsCount: nsIsbsCount,
      isbsActiveCount: nsIsbsActiveCount,
      isbsInactiveCount: nsIsbsInactiveCount,
      isbsHealthyCount: nsIsbsHealthyCount,
      isbsWarningCount: nsIsbsWarningCount,
      isbsCriticalCount: nsIsbsCriticalCount,
    });
    // Add counts to cluster summary counts
    pipelinesCount += nsPipelinesCount;
    pipelinesActiveCount += nsPipelinesActiveCount;
    pipelinesInactiveCount += nsPipelinesInactiveCount;
    pipelinesHealthyCount += nsPipelinesHealthyCount;
    pipelinesWarningCount += nsPipelinesWarningCount;
    pipelinesCriticalCount += nsPipelinesCriticalCount;
    isbsCount += nsIsbsCount;
    isbsActiveCount += nsIsbsActiveCount;
    isbsInactiveCount += nsIsbsInactiveCount;
    isbsHealthyCount += nsIsbsHealthyCount;
    isbsWarningCount += nsIsbsWarningCount;
    isbsCriticalCount += nsIsbsCriticalCount;
  });
  return {
    namespacesCount,
    pipelinesCount,
    pipelinesActiveCount,
    pipelinesInactiveCount,
    pipelinesHealthyCount,
    pipelinesWarningCount,
    pipelinesCriticalCount,
    isbsCount,
    isbsActiveCount,
    isbsInactiveCount,
    isbsHealthyCount,
    isbsWarningCount,
    isbsCriticalCount,
    nameSpaceSummaries,
  };
};

const DEFAULT_NS_NAME = "default";
const DATA_REFRESH_INTERVAL = 60000; // ms

export const useClusterSummaryFetch = ({
  loadOnRefresh = false,
}: ClusterSummaryFetchProps) => {
  const [results, setResults] = useState<ClusterSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [options, setOptions] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const {
    data: fetchData,
    loading: fetchLoading,
    error: fetchError,
  } = useFetch("/api/v1_1/cluster-summary", undefined, options);

  useEffect(() => {
    setInterval(() => {
      setOptions({
        skip: false,
        requestKey: "id" + Math.random().toString(16).slice(2),
      });
    }, DATA_REFRESH_INTERVAL);
  }, []);

  useEffect(() => {
    if (fetchLoading) {
      if (options?.requestKey === "" || loadOnRefresh) {
        // Only set loading true when first load or when loadOnRefresh is true
        setResults({
          data: undefined,
          loading: true,
          error: undefined,
        });
      }
      return;
    }
    if (fetchError) {
      setResults({
        data: undefined,
        loading: false,
        error: fetchError,
      });
      return;
    }
    if (fetchData && fetchData.errMsg) {
      setResults({
        data: undefined,
        loading: false,
        error: fetchData.errMsg,
      });
      return;
    }
    if (fetchData) {
      // const clusterSummary = rawDataToClusterSummary(MOCK_DATA); // TODO REMOVE MOCK
      const clusterSummary = rawDataToClusterSummary(fetchData.data);
      setResults({
        data: clusterSummary,
        loading: false,
        error: undefined,
      });
      return;
    }
  }, [fetchData, fetchLoading, fetchError, loadOnRefresh, options]);

  return results;
};
